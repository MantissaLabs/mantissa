use super::admission::{
    PublicPortClaim, collect_public_port_claims,
    ensure_public_ports_do_not_overlap_template_host_ports, public_claim_conflicts_host_port,
    service_reserves_public_ports, validate_network_contracts, workload_port_protocol_label,
};
use super::placement::{
    SlotTargetContext, allow_untargeted_fallback, build_missing_template_requests,
    build_placement_preference_inventory, build_start_requests, compute_effective_slot_targets,
    is_local_volume_unavailable_error, requests_require_pinned_targets,
};
use super::state::deploying_assignment_incomplete;
use super::*;
use crate::config;
use crate::ip_family::{IpFamily, infer_default_ip_family};
use crate::network::bpf::overlay_bpf_program_specs;
use crate::network::types::{
    NetworkEvent, NetworkSpecDraft, NetworkSpecUpdate, NetworkSpecValue, NetworkStatus,
};

/// IPv4 prefix used by deterministic auto-provisioned service networks.
const SERVICE_DEFAULT_NETWORK_SUBNET_PREFIX_V4: u8 = 20;
/// Number of non-overlapping `/20` candidates inside the default IPv4 `10.0.0.0/8` range.
const SERVICE_DEFAULT_NETWORK_SUBNET_CANDIDATES_V4: u32 = 1 << 12;
/// IPv6 prefix used by deterministic auto-provisioned service networks.
const SERVICE_DEFAULT_NETWORK_SUBNET_PREFIX_V6: u8 = 64;
/// Number of deterministic IPv6 ULA subnet candidates probed before falling back to the first.
const SERVICE_DEFAULT_NETWORK_SUBNET_CANDIDATES_V6: u32 = 1 << 16;

impl ServiceController {
    /// Schedules an asynchronous deployment for the provided service manifest and returns
    /// the deterministic service identifier so the caller can track progress separately.
    #[allow(dead_code)]
    pub async fn submit_deployment(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        task_templates: Vec<TaskTemplateSpecValue>,
    ) -> anyhow::Result<Uuid> {
        let submission = self
            .submit_deployment_with_strategy_outcome(
                manifest_id,
                manifest_name,
                service_name,
                task_templates,
                ServiceUpdateStrategy::default(),
            )
            .await?;
        Ok(submission.service_id)
    }

    /// Schedules an asynchronous deployment with explicit rollout strategy configuration.
    #[allow(dead_code)]
    pub async fn submit_deployment_with_strategy(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        task_templates: Vec<TaskTemplateSpecValue>,
        update_strategy: ServiceUpdateStrategy,
    ) -> anyhow::Result<Uuid> {
        let submission = self
            .submit_deployment_with_strategy_outcome(
                manifest_id,
                manifest_name,
                service_name,
                task_templates,
                update_strategy,
            )
            .await?;
        Ok(submission.service_id)
    }

    /// Submits a deployment and returns a structured outcome for idempotent callers.
    pub async fn submit_deployment_with_strategy_outcome(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        task_templates: Vec<TaskTemplateSpecValue>,
        update_strategy: ServiceUpdateStrategy,
    ) -> anyhow::Result<ServiceDeploymentSubmission> {
        self.submit_deployment_with_required_networks_outcome(
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            update_strategy,
            Vec::new(),
        )
        .await
    }

    /// Submits a deployment after provisioning network dependencies owned by the service request.
    pub async fn submit_deployment_with_required_networks_outcome(
        &self,
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        task_templates: Vec<TaskTemplateSpecValue>,
        update_strategy: ServiceUpdateStrategy,
        required_networks: Vec<ServiceRequiredNetworkSpec>,
    ) -> anyhow::Result<ServiceDeploymentSubmission> {
        let manifest_name = manifest_name.into();
        let service_name = service_name.into();
        let service_id = compute_service_id(&service_name);
        build_template_dependency_stages(&task_templates).map_err(|err| {
            anyhow!(
                "invalid task dependency graph for service '{}': {err}",
                service_name
            )
        })?;
        self.ensure_required_networks(&required_networks).await?;
        self.ensure_network_contracts(&service_name, task_templates.as_slice())?;
        let desired_public_claims =
            collect_public_port_claims(&service_name, task_templates.as_slice())?;
        ensure_public_ports_do_not_overlap_template_host_ports(
            &service_name,
            desired_public_claims.as_slice(),
            task_templates.as_slice(),
        )?;
        self.ensure_public_ports_do_not_overlap_active_host_ports(
            &service_name,
            desired_public_claims.as_slice(),
        )
        .await?;
        self.ensure_host_ports_do_not_overlap_existing_public_ports(
            service_id,
            &service_name,
            task_templates.as_slice(),
        )?;

        if let Some(existing) = self.registry.get(service_id)? {
            match existing.status() {
                ServiceStatus::Stopping => {
                    return Err(anyhow!(
                        "service '{}' is currently stopping; wait for completion before redeploying",
                        service_name
                    ));
                }
                ServiceStatus::Deploying => {
                    return Err(anyhow!(
                        "service '{}' already has a deployment in progress",
                        service_name
                    ));
                }
                _ => {}
            }

            if is_running_deployment_noop(
                &existing,
                &manifest_name,
                &service_name,
                &task_templates,
                &update_strategy,
            ) {
                tracing::info!(
                    target: "services",
                    "deployment for '{}' ignored because desired spec is already running",
                    service_name
                );
                return Ok(ServiceDeploymentSubmission {
                    service_id,
                    outcome: ServiceDeploymentOutcome::Unchanged,
                });
            }

            self.ensure_public_port_claims_available(
                service_id,
                &service_name,
                desired_public_claims.as_slice(),
            )?;

            if matches!(
                existing.status(),
                ServiceStatus::Failed | ServiceStatus::Stopped
            ) {
                let previous_status = existing.status();
                self.stop_tasks(&existing).await;

                let mut pending_spec = existing;
                pending_spec.manifest_id = manifest_id;
                pending_spec.manifest_name = manifest_name.clone();
                pending_spec.task_templates = task_templates.clone();
                pending_spec.update_strategy = update_strategy.clone();
                pending_spec.start_new_generation();
                pending_spec.replica_ids.clear();
                pending_spec.set_rollout(ServiceRolloutState::default());
                pending_spec.previous_generation = None;
                pending_spec.set_status(ServiceStatus::Deploying);

                tracing::info!(
                    target: "services",
                    "starting deployment recovery for service '{}' from {:?} with manifest {}",
                    service_name,
                    previous_status,
                    manifest_id
                );

                self.apply_upsert(pending_spec.clone()).await?;
                self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;
                self.maybe_spawn_generation_execution_for_service(service_id)
                    .await;

                return Ok(ServiceDeploymentSubmission {
                    service_id,
                    outcome: ServiceDeploymentOutcome::Accepted,
                });
            }

            let current_spec = existing.clone();
            let mut pending_spec = existing;
            pending_spec.manifest_id = manifest_id;
            pending_spec.manifest_name = manifest_name.clone();
            pending_spec.task_templates = task_templates.clone();
            pending_spec.update_strategy = update_strategy.clone();
            pending_spec.start_new_generation();
            // A new deployment generation must start from an empty assignment set so peers can
            // observe a clean Deploying bootstrap before task ids are repopulated.
            pending_spec.replica_ids.clear();
            pending_spec.previous_generation =
                Some(ServicePreviousGeneration::from_service(&current_spec));
            pending_spec.set_status(ServiceStatus::Deploying);

            tracing::info!(
                target: "services",
                "starting redeployment for '{}' with manifest {}",
                service_name,
                manifest_id
            );

            self.apply_upsert(pending_spec.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;
            self.maybe_spawn_generation_execution_for_service(service_id)
                .await;

            return Ok(ServiceDeploymentSubmission {
                service_id,
                outcome: ServiceDeploymentOutcome::Accepted,
            });
        }

        self.ensure_public_port_claims_available(
            service_id,
            &service_name,
            desired_public_claims.as_slice(),
        )?;

        let mut pending_spec = ServiceSpecValue::new(
            manifest_id,
            manifest_name.clone(),
            service_name.clone(),
            task_templates.clone(),
            Vec::new(),
        );
        pending_spec.update_strategy = update_strategy.clone();
        pending_spec.previous_generation = None;
        pending_spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(pending_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(pending_spec)).await?;
        self.maybe_spawn_generation_execution_for_service(service_id)
            .await;

        Ok(ServiceDeploymentSubmission {
            service_id,
            outcome: ServiceDeploymentOutcome::Accepted,
        })
    }

    /// Ensures every network declared by the deployment request exists before service admission.
    async fn ensure_required_networks(
        &self,
        required_networks: &[ServiceRequiredNetworkSpec],
    ) -> anyhow::Result<()> {
        let required = normalize_required_networks(required_networks)?;
        if required.is_empty() {
            return Ok(());
        }

        let existing = self.network_registry.list_specs()?;
        let existing_by_name: HashMap<String, NetworkSpecValue> = existing
            .iter()
            .cloned()
            .map(|spec| (spec.name.clone(), spec))
            .collect();
        let mut known_subnets: BTreeSet<String> = existing
            .iter()
            .filter(|spec| !spec.is_deleted())
            .map(|spec| spec.subnet_cidr.clone())
            .collect();

        for requested in required {
            if let Some(existing) = existing_by_name.get(&requested.name)
                && !existing.is_deleted()
            {
                self.validate_existing_required_network(existing, &requested)?;
                continue;
            }

            let mut spec = self.build_required_network_spec(&requested, &known_subnets);
            if let Some(mut deleted) = existing_by_name.get(&requested.name).cloned()
                && deleted.is_deleted()
            {
                deleted.reset_for_recreate(NetworkSpecUpdate {
                    description: spec.description.clone(),
                    driver: spec.driver,
                    subnet_cidr: spec.subnet_cidr.clone(),
                    vni: spec.vni,
                    mtu: spec.mtu,
                    sealed: spec.sealed,
                    bpf_programs: spec.bpf_programs.clone(),
                });
                spec = deleted;
            }

            spec.set_status(NetworkStatus::Pending);
            self.network_registry.upsert_spec(spec.clone()).await?;
            self.gossip_tx
                .send(Message::Network {
                    id: Uuid::new_v4(),
                    event: NetworkEvent::Upsert(spec.clone()),
                })
                .await
                .map_err(|err| anyhow!("failed to broadcast network upsert: {err}"))?;
            self.network_controller.schedule_spec_change(spec.id).await;
            known_subnets.insert(spec.subnet_cidr.clone());
            tracing::info!(
                target: "services",
                "network '{}' auto-provisioned for service deployment with id {}",
                spec.name,
                spec.id
            );
        }

        Ok(())
    }

    /// Validates that a named network already in the registry satisfies the deployment request.
    fn validate_existing_required_network(
        &self,
        existing: &NetworkSpecValue,
        requested: &ServiceRequiredNetworkSpec,
    ) -> anyhow::Result<()> {
        if existing.status == NetworkStatus::Deleting {
            return Err(anyhow!(
                "service deployment requests network '{}' but the existing network is deleting",
                requested.name
            ));
        }
        if existing.driver != requested.driver {
            return Err(anyhow!(
                "service deployment requests network '{}' with driver {:?} but existing network uses {:?}",
                requested.name,
                requested.driver,
                existing.driver
            ));
        }
        Ok(())
    }

    /// Builds the replicated network spec used when a service deployment auto-provisions a network.
    fn build_required_network_spec(
        &self,
        requested: &ServiceRequiredNetworkSpec,
        known_subnets: &BTreeSet<String>,
    ) -> NetworkSpecValue {
        let family = match requested.ip_family {
            ServiceRequiredNetworkIpFamily::Ipv4 => ServiceRequiredNetworkIpFamily::Ipv4,
            ServiceRequiredNetworkIpFamily::Ipv6 => ServiceRequiredNetworkIpFamily::Ipv6,
            ServiceRequiredNetworkIpFamily::Default => default_required_network_family(),
        };
        let bpf_programs = match requested.driver {
            NetworkDriver::Vxlan => overlay_bpf_program_specs(),
            NetworkDriver::Bridge => Vec::new(),
        };

        NetworkSpecValue::new(NetworkSpecDraft {
            name: requested.name.clone(),
            description: String::new(),
            driver: requested.driver,
            subnet_cidr: default_required_network_subnet(
                &requested.name,
                known_subnets.iter().map(String::as_str),
                family,
            ),
            vni: 0,
            mtu: 0,
            sealed: false,
            bpf_programs,
        })
    }

    /// Builds a human-readable readiness blocker when targeted task nodes lack required networks.
    fn deployment_network_readiness_detail(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> anyhow::Result<Option<String>> {
        let mut blockers = BTreeSet::new();
        for request in requests {
            for network_id in &request.networks {
                match request.target_node {
                    Some(node_id) => {
                        if !self.network_ready_on_node(*network_id, node_id)? {
                            blockers.insert(format!(
                                "network '{}' not ready on node '{}'",
                                self.network_label(*network_id)?,
                                self.node_label(node_id)
                            ));
                        }
                    }
                    None => {
                        if !self.network_ready_on_any_peer(*network_id)? {
                            blockers.insert(format!(
                                "network '{}' has no ready schedulable peer",
                                self.network_label(*network_id)?
                            ));
                        }
                    }
                }
            }
        }

        if blockers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(format!(
                "waiting for network readiness: {}",
                format_service_network_readiness_blockers(&blockers)
            )))
        }
    }

    /// Returns true once the given peer has reconciled the requested network locally.
    fn network_ready_on_node(&self, network_id: Uuid, node_id: Uuid) -> anyhow::Result<bool> {
        let Some(spec) = self.network_registry.get_spec(network_id)? else {
            return Ok(false);
        };
        if spec.is_deleted() {
            return Ok(false);
        }
        Ok(self
            .network_registry
            .get_peer_state(network_id, node_id)?
            .is_some_and(|state| state.state.is_ready()))
    }

    /// Returns true once any schedulable peer can host workloads for the requested network.
    fn network_ready_on_any_peer(&self, network_id: Uuid) -> anyhow::Result<bool> {
        let Some(spec) = self.network_registry.get_spec(network_id)? else {
            return Ok(false);
        };
        if spec.is_deleted() {
            return Ok(false);
        }
        for state in self.network_registry.list_peer_states(Some(network_id))? {
            if state.state.is_ready() && self.cluster_registry.peer_schedulable(state.peer_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Renders one network id as a stable operator-facing label for service status details.
    fn network_label(&self, network_id: Uuid) -> anyhow::Result<String> {
        Ok(self
            .network_registry
            .get_spec(network_id)?
            .map(|spec| spec.name)
            .unwrap_or_else(|| short_uuid(network_id)))
    }

    /// Renders one node id as a compact hostname-or-id label for service status details.
    fn node_label(&self, node_id: Uuid) -> String {
        self.cluster_registry
            .peer_hostname(node_id)
            .map(|hostname| hostname.trim().to_string())
            .filter(|hostname| !hostname.is_empty())
            .unwrap_or_else(|| short_uuid(node_id))
    }

    /// Validate service declarations whose behavior depends on the referenced network drivers.
    fn ensure_network_contracts(
        &self,
        service_name: &str,
        task_templates: &[TaskTemplateSpecValue],
    ) -> anyhow::Result<()> {
        validate_network_contracts(service_name, task_templates, &self.network_registry)
    }

    /// Validates that the incoming public endpoint claims do not overlap an existing service.
    fn ensure_public_port_claims_available(
        &self,
        service_id: Uuid,
        service_name: &str,
        desired_claims: &[PublicPortClaim],
    ) -> anyhow::Result<()> {
        if desired_claims.is_empty() {
            return Ok(());
        }

        let existing_specs = self.registry.list()?;
        for existing in existing_specs {
            if existing.id == service_id || !service_reserves_public_ports(existing.status()) {
                continue;
            }

            let existing_claims = collect_public_port_claims(
                &existing.service_name,
                existing.task_templates.as_slice(),
            )
            .map_err(|err| {
                anyhow!(
                    "existing service '{}' has invalid public endpoint metadata: {err}",
                    existing.service_name
                )
            })?;

            for desired in desired_claims {
                if let Some(conflict) = existing_claims
                    .iter()
                    .find(|existing_claim| existing_claim.selector == desired.selector)
                {
                    return Err(anyhow!(
                        "service '{service_name}' template '{}' cannot claim public port {} because service '{}' template '{}' already reserves it",
                        desired.template_name,
                        desired.selector,
                        existing.service_name,
                        conflict.template_name
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validates that desired public endpoints do not overlap active workload host ports.
    async fn ensure_public_ports_do_not_overlap_active_host_ports(
        &self,
        service_name: &str,
        desired_claims: &[PublicPortClaim],
    ) -> anyhow::Result<()> {
        if desired_claims.is_empty() {
            return Ok(());
        }

        let workloads = self
            .workload_manager
            .list_workloads(&TaskStateFilter::active_only())
            .await?;
        for workload in workloads {
            for port in &workload.ports {
                if let Some(public_claim) = desired_claims
                    .iter()
                    .find(|claim| public_claim_conflicts_host_port(claim, port))
                {
                    return Err(anyhow!(
                        "service '{service_name}' template '{}' cannot claim public port {} because active workload '{}' ({}) already reserves host port {}/{}",
                        public_claim.template_name,
                        public_claim.selector,
                        workload.name,
                        workload.id,
                        port.host_port,
                        workload_port_protocol_label(port.protocol)
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validates that desired static host ports do not overlap existing public endpoints.
    fn ensure_host_ports_do_not_overlap_existing_public_ports(
        &self,
        service_id: Uuid,
        service_name: &str,
        task_templates: &[TaskTemplateSpecValue],
    ) -> anyhow::Result<()> {
        let has_host_ports = task_templates
            .iter()
            .any(|template| !template.execution.ports.is_empty());
        if !has_host_ports {
            return Ok(());
        }

        let existing_specs = self.registry.list()?;
        for existing in existing_specs {
            if existing.id == service_id || !service_reserves_public_ports(existing.status()) {
                continue;
            }

            let existing_claims = collect_public_port_claims(
                &existing.service_name,
                existing.task_templates.as_slice(),
            )
            .map_err(|err| {
                anyhow!(
                    "existing service '{}' has invalid public endpoint metadata: {err}",
                    existing.service_name
                )
            })?;

            for template in task_templates {
                for port in &template.execution.ports {
                    if let Some(public_claim) = existing_claims
                        .iter()
                        .find(|claim| public_claim_conflicts_host_port(claim, port))
                    {
                        return Err(anyhow!(
                            "service '{service_name}' template '{}' cannot reserve host port {}/{} because service '{}' template '{}' already claims public port {}",
                            template.name,
                            port.host_port,
                            workload_port_protocol_label(port.protocol),
                            existing.service_name,
                            public_claim.template_name,
                            public_claim.selector
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Loads the current service spec and launches local generation execution when this node owns it.
    pub(super) async fn maybe_spawn_generation_execution_for_service(&self, service_id: Uuid) {
        let spec = match self.registry.get(service_id) {
            Ok(Some(spec)) => spec,
            Ok(None) => return,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load service {service_id} while checking generation ownership: {err}"
                );
                return;
            }
        };
        let eligible_nodes = self.collect_eligible_nodes();
        self.maybe_spawn_generation_execution(spec, &eligible_nodes)
            .await;
    }

    /// Starts the local adopter when replicated state says this node owns the deploying generation.
    pub(super) async fn maybe_spawn_generation_execution(
        &self,
        spec: ServiceSpecValue,
        eligible_nodes: &[Uuid],
    ) {
        if spec.status() != ServiceStatus::Deploying || eligible_nodes.is_empty() {
            return;
        }

        let Some(owner_id) = select_generation_owner(spec.id, spec.service_epoch, eligible_nodes)
        else {
            return;
        };
        if owner_id != self.local_node_id {
            return;
        }

        let key = ServiceGenerationExecutionKey::from_spec(&spec);
        let mut inflight = self.inflight_generations.lock().await;
        if !inflight.insert(key) {
            return;
        }
        drop(inflight);

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = controller.adopt_deploying_generation(spec.clone()).await {
                tracing::warn!(
                    target: "services",
                    service = %spec.service_name,
                    manifest = %spec.manifest_id,
                    epoch = spec.service_epoch,
                    "service generation execution failed: {err:#}"
                );
                controller
                    .record_generation_execution_error(&spec, service_error_detail(&err))
                    .await;
            }
            controller.finish_generation_execution(key).await;
        });
    }

    /// Persists the latest deployment execution error while the same generation remains pending.
    async fn record_generation_execution_error(&self, spec: &ServiceSpecValue, detail: String) {
        let Ok(Some(mut current)) = self.registry.get(spec.id) else {
            return;
        };
        if current.manifest_id != spec.manifest_id
            || current.service_epoch != spec.service_epoch
            || current.status() != ServiceStatus::Deploying
        {
            return;
        }

        let Some(detail) = normalize_service_status_detail(detail) else {
            return;
        };
        if current.status_detail.as_deref() == Some(detail.as_str()) {
            return;
        }

        current.set_status_detail(Some(detail));
        if let Err(err) = self.apply_upsert(current.clone()).await {
            tracing::warn!(
                target: "services",
                service = %spec.service_name,
                manifest = %spec.manifest_id,
                epoch = spec.service_epoch,
                "failed to persist generation execution error detail: {err:#}"
            );
            return;
        }
        if let Err(err) = self.broadcast(ServiceEvent::Upsert(current)).await {
            tracing::warn!(
                target: "services",
                service = %spec.service_name,
                manifest = %spec.manifest_id,
                epoch = spec.service_epoch,
                "failed to broadcast generation execution error detail: {err:#}"
            );
        }
    }

    /// Removes one completed generation execution from the local in-flight dedupe set.
    async fn finish_generation_execution(&self, key: ServiceGenerationExecutionKey) {
        let mut inflight = self.inflight_generations.lock().await;
        inflight.remove(&key);
    }

    /// Adopts the current deploying service generation directly from replicated service state.
    async fn adopt_deploying_generation(&self, spec: ServiceSpecValue) -> anyhow::Result<()> {
        let current = match self.registry.get(spec.id)? {
            Some(current)
                if current.manifest_id == spec.manifest_id
                    && current.service_epoch == spec.service_epoch
                    && current.status() == ServiceStatus::Deploying =>
            {
                current
            }
            Some(_) | None => return Ok(()),
        };

        if let Some(previous) = current.previous_generation.as_ref() {
            let job = ServiceRedeploymentJob {
                manifest_id: current.manifest_id,
                manifest_name: current.manifest_name.clone(),
                service_name: current.service_name.clone(),
                task_templates: current.task_templates.clone(),
                current_spec: previous.to_service_spec(current.id, current.service_name.clone()),
                update_strategy: current.update_strategy.clone(),
            };
            return self.clone().execute_redeployment(job).await;
        }

        if deploying_assignment_incomplete(&current) {
            let job = ServiceDeploymentJob {
                manifest_id: current.manifest_id,
                manifest_name: current.manifest_name.clone(),
                service_name: current.service_name.clone(),
                task_templates: current.task_templates.clone(),
                update_strategy: current.update_strategy.clone(),
                assigned_task_ids: current.replica_ids.clone(),
            };
            return self.clone().execute_deployment(job).await;
        }

        self.clone().await_service_readiness(current).await;
        Ok(())
    }
    /// Executes the deployment workflow in the background by starting tasks via the task manager
    /// and persisting the resulting service specification into the replicated registry.
    async fn execute_deployment(self, job: ServiceDeploymentJob) -> anyhow::Result<()> {
        let stages = build_template_dependency_stages(&job.task_templates).map_err(|err| {
            anyhow!(
                "invalid task dependency graph for service '{}': {err}",
                job.service_name
            )
        })?;
        if stages.len() <= 1 {
            return self.execute_flat_deployment(job).await;
        }

        self.execute_dependency_ordered_deployment(job, stages)
            .await
    }

    /// Launches a service whose task templates do not declare cross-template dependencies.
    async fn execute_flat_deployment(self, job: ServiceDeploymentJob) -> anyhow::Result<()> {
        let ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            update_strategy,
            assigned_task_ids: _,
        } = job;

        let service_id = compute_service_id(&service_name);
        let eligible_nodes = self.collect_eligible_nodes();
        let placement_nodes = self.placement_nodes_for(&eligible_nodes);
        let preference_inventory =
            build_placement_preference_inventory(&self.workload_manager).await?;
        let requests = build_start_requests(SlotTargetContext {
            service_name: &service_name,
            service_id,
            task_templates: &task_templates,
            eligible_nodes: &eligible_nodes,
            placement_nodes: &placement_nodes,
            preference_inventory: &preference_inventory,
            network_registry: &self.network_registry,
            volume_registry: &self.volume_registry,
        })?;

        if requests.is_empty() {
            let spec = ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                task_templates,
                Vec::new(),
            );
            let mut spec = spec;
            spec.update_strategy = update_strategy;
            self.apply_upsert(spec.clone()).await?;
            self.broadcast(ServiceEvent::Upsert(spec)).await?;
            tracing::info!(
                target: "services",
                "registered service '{}' with no runnable tasks",
                service_name
            );
            return Ok(());
        }

        if let Some(detail) = self.deployment_network_readiness_detail(&requests)? {
            self.update_service_status_detail_if_current(service_id, manifest_id, Some(detail))
                .await;
            tracing::info!(
                target: "services",
                "deferring deployment for service '{}' until target network readiness converges",
                service_name
            );
            return Ok(());
        }

        tracing::info!(
            target: "services",
            "starting deployment for service '{}' with {} task replicas",
            service_name,
            requests.len()
        );
        let desired_task_ids: Vec<Uuid> =
            requests.iter().filter_map(|request| request.id).collect();

        let task_specs = match self
            .start_tasks_with_fallback(requests, &format!("service '{}' deployment", service_name))
            .await
        {
            Ok(specs) => specs,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "initial task launch for service '{}' failed: {err:#}",
                    service_name
                );

                if workload_start_error_requires_service_requeue(&err) {
                    self.persist_retryable_deployment_launch_error(service_id, &service_name, &err)
                        .await;
                    tracing::info!(
                        target: "services",
                        "deferring deployment retry for '{}' until scheduling prerequisites converge",
                        service_name
                    );
                    return Ok(());
                }

                let detail = service_error_detail(&err);
                match self.registry.get(service_id) {
                    Ok(Some(mut persisted_spec)) if is_local_volume_unavailable_error(&err) => {
                        persisted_spec.replica_ids = desired_task_ids.clone();
                        persisted_spec.set_rollout(ServiceRolloutState::default());
                        persisted_spec.set_status(ServiceStatus::VolumeUnavailable);
                        if let Err(upsert_err) = self.apply_upsert(persisted_spec.clone()).await {
                            tracing::warn!(
                                target: "services",
                                "failed to persist volume-unavailable state for '{}': {upsert_err}",
                                service_name
                            );
                        } else if let Err(broadcast_err) =
                            self.broadcast(ServiceEvent::Upsert(persisted_spec)).await
                        {
                            tracing::warn!(
                                target: "services",
                                "failed to broadcast volume-unavailable state for '{}': {broadcast_err}",
                                service_name
                            );
                        }
                    }
                    Ok(Some(persisted_spec)) => {
                        self.persist_deploying_launch_error(persisted_spec.clone(), detail.clone())
                            .await;
                        if workload_start_error_consumes_service_failure_budget(&err) {
                            let controller = self.clone();
                            tokio::task::spawn_local(async move {
                                controller.await_service_readiness(persisted_spec).await;
                            });
                        }
                    }
                    Ok(None) if is_local_volume_unavailable_error(&err) => {
                        let mut blocked_spec = ServiceSpecValue::new(
                            manifest_id,
                            manifest_name.clone(),
                            service_name.clone(),
                            task_templates.clone(),
                            desired_task_ids,
                        );
                        blocked_spec.update_strategy = update_strategy.clone();
                        blocked_spec.set_rollout(ServiceRolloutState::default());
                        blocked_spec.set_status(ServiceStatus::VolumeUnavailable);
                        if let Err(upsert_err) = self.apply_upsert(blocked_spec.clone()).await {
                            tracing::warn!(
                                target: "services",
                                "failed to persist fallback volume-unavailable state for '{}': {upsert_err}",
                                service_name
                            );
                        } else if let Err(broadcast_err) =
                            self.broadcast(ServiceEvent::Upsert(blocked_spec)).await
                        {
                            tracing::warn!(
                                target: "services",
                                "failed to broadcast fallback volume-unavailable state for '{}': {broadcast_err}",
                                service_name
                            );
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(
                            target: "services",
                            "unable to schedule deployment retry for '{}' because the service spec is missing; marking service failed",
                            service_name
                        );
                        let mut failed_spec = ServiceSpecValue::new(
                            manifest_id,
                            manifest_name.clone(),
                            service_name.clone(),
                            task_templates.clone(),
                            Vec::new(),
                        );
                        failed_spec.update_strategy = update_strategy.clone();
                        failed_spec.set_rollout(ServiceRolloutState {
                            last_error: Some(detail.clone()),
                            ..ServiceRolloutState::default()
                        });
                        failed_spec.set_status(ServiceStatus::Failed);
                        failed_spec.set_status_detail(Some(detail));
                        if let Err(upsert_err) = self.apply_upsert(failed_spec.clone()).await {
                            tracing::warn!(
                                target: "services",
                                "failed to persist fallback failed state for '{}': {upsert_err}",
                                service_name
                            );
                        } else if let Err(broadcast_err) =
                            self.broadcast(ServiceEvent::Upsert(failed_spec)).await
                        {
                            tracing::warn!(
                                target: "services",
                                "failed to broadcast fallback failed state for '{}': {broadcast_err}",
                                service_name
                            );
                        }
                    }
                    Err(fetch_err) => {
                        tracing::warn!(
                            target: "services",
                            "unable to load service '{}' spec for retry: {fetch_err}",
                            service_name
                        );
                    }
                }

                return Ok(());
            }
        };
        let replica_ids: Vec<Uuid> = task_specs.iter().map(|spec| spec.id).collect();

        let mut spec = match self.registry.get(service_id)? {
            Some(spec) if spec.manifest_id == manifest_id => spec,
            _ => ServiceSpecValue::new(
                manifest_id,
                manifest_name.clone(),
                service_name.clone(),
                task_templates.clone(),
                Vec::new(),
            ),
        };
        spec.manifest_id = manifest_id;
        spec.manifest_name = manifest_name;
        spec.service_name = service_name.clone();
        spec.task_templates = task_templates;
        spec.replica_ids = replica_ids;
        spec.update_strategy = update_strategy;
        spec.previous_generation = None;
        spec.set_rollout(ServiceRolloutState::default());
        spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(spec.clone())).await?;

        let readiness_spec = spec.clone();
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.await_service_readiness(readiness_spec).await;
        });

        tracing::info!(
            target: "services",
            "service '{}' deployment submitted; tasks launching asynchronously",
            service_name
        );

        Ok(())
    }

    /// Launches service task templates in deterministic dependency order, waiting for each upstream
    /// template to become discoverable before starting the task templates that depend on it.
    async fn execute_dependency_ordered_deployment(
        self,
        job: ServiceDeploymentJob,
        stages: Vec<TemplateDependencyStage>,
    ) -> anyhow::Result<()> {
        let ServiceDeploymentJob {
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            update_strategy,
            assigned_task_ids,
        } = job;

        let service_id = compute_service_id(&service_name);
        let eligible_nodes = self.collect_eligible_nodes();
        let deployment = ServiceDeploymentContext {
            manifest_id,
            manifest_name: &manifest_name,
            service_name: &service_name,
            task_templates: &task_templates,
            update_strategy: &update_strategy,
        };
        let ordered_indices: Vec<usize> = stages
            .iter()
            .flat_map(|stage| stage.template_indices.iter().copied())
            .collect();

        tracing::info!(
            target: "services",
            "starting dependency-ordered deployment for service '{}' across {} template stage(s)",
            service_name,
            stages.len()
        );

        let template_replica_counts: HashMap<String, u16> = task_templates
            .iter()
            .map(|template| (template.name.clone(), template.replicas))
            .collect();
        let mut assignments: BTreeMap<(String, u16), Uuid> = BTreeMap::new();
        for assignment in self
            .collect_assignments(&service_name, &assigned_task_ids)
            .await
        {
            assignments.insert(
                (assignment.template.clone(), assignment.replica),
                assignment.task_id,
            );
        }

        let mut launched_task_ids: HashMap<String, Vec<Uuid>> = HashMap::new();
        for template in &task_templates {
            let mut template_task_ids = Vec::new();
            for replica in 1..=template.replicas {
                if let Some(task_id) = assignments.get(&(template.name.clone(), replica)) {
                    template_task_ids.push(*task_id);
                }
            }
            if !template_task_ids.is_empty() {
                launched_task_ids.insert(template.name.clone(), template_task_ids);
            }
        }

        let placement_nodes = self.placement_nodes_for(&eligible_nodes);
        let preference_inventory =
            build_placement_preference_inventory(&self.workload_manager).await?;
        let slot_targets = compute_effective_slot_targets(&SlotTargetContext {
            service_name: &service_name,
            service_id,
            task_templates: &task_templates,
            eligible_nodes: &eligible_nodes,
            placement_nodes: &placement_nodes,
            preference_inventory: &preference_inventory,
            network_registry: &self.network_registry,
            volume_registry: &self.volume_registry,
        })?;

        for template_index in ordered_indices {
            let template = task_templates[template_index].clone();
            if !template.depends_on.is_empty()
                && let Err(err) = self
                    .wait_for_template_dependencies_ready(
                        &deployment,
                        &template,
                        &template_replica_counts,
                        &launched_task_ids,
                    )
                    .await
            {
                tracing::warn!(
                    target: "services",
                    "dependency gate for service '{}' failed before launching template '{}': {err:#}",
                    service_name,
                    template.name
                );
                self.mark_deployment_failed(&deployment, Some(err.to_string()))
                    .await?;
                return Ok(());
            }

            let requests = build_missing_template_requests(
                &service_name,
                service_id,
                &template,
                &assignments,
                &slot_targets,
            );
            if requests.is_empty() {
                continue;
            }

            if let Some(detail) = self.deployment_network_readiness_detail(&requests)? {
                self.update_service_status_detail_if_current(service_id, manifest_id, Some(detail))
                    .await;
                tracing::info!(
                    target: "services",
                    "deferring deployment for service '{}' template '{}' until target network readiness converges",
                    service_name,
                    template.name
                );
                return Ok(());
            }

            let desired_task_ids: Vec<Uuid> =
                requests.iter().filter_map(|request| request.id).collect();
            let context = format!(
                "service '{}' deployment for template '{}'",
                service_name, template.name
            );
            let task_specs = match self.start_tasks_with_fallback(requests, &context).await {
                Ok(specs) => specs,
                Err(err) if launched_task_ids.is_empty() => {
                    self.handle_initial_deployment_launch_failure(
                        &deployment,
                        &desired_task_ids,
                        &err,
                    )
                    .await;
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "dependency-ordered launch for service '{}' failed on template '{}': {err:#}",
                        service_name,
                        template.name
                    );
                    self.mark_deployment_failed(&deployment, Some(err.to_string()))
                        .await?;
                    return Ok(());
                }
            };

            let stage_ids: Vec<Uuid> = task_specs.iter().map(|spec| spec.id).collect();
            launched_task_ids
                .entry(template.name.clone())
                .or_default()
                .extend(stage_ids);
            record_task_assignments(&service_name, &task_specs, &mut assignments);

            let ordered_task_ids = ordered_known_task_ids(&task_templates, &assignments);
            let _ = self
                .persist_deploying_task_ids(&deployment, ordered_task_ids)
                .await?;
        }

        let readiness_spec = self
            .persist_deploying_task_ids(
                &deployment,
                ordered_known_task_ids(&task_templates, &assignments),
            )
            .await?;
        self.update_service_status_detail_if_current(service_id, manifest_id, None)
            .await;

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.await_service_readiness(readiness_spec).await;
        });

        tracing::info!(
            target: "services",
            "service '{}' dependency-ordered deployment submitted; tasks launching asynchronously",
            service_name
        );

        Ok(())
    }

    /// Persists the latest startup scheduling failure while keeping the deployment recoverable.
    async fn persist_deploying_launch_error(&self, mut spec: ServiceSpecValue, detail: String) {
        if spec.status() != ServiceStatus::Deploying {
            return;
        }

        let Some(detail) = normalize_service_status_detail(detail) else {
            return;
        };
        if spec.status_detail.as_deref() == Some(detail.as_str()) {
            return;
        }

        spec.set_status_detail(Some(detail));
        if let Err(err) = self.apply_upsert(spec.clone()).await {
            tracing::warn!(
                target: "services",
                "failed to persist deployment launch detail for '{}': {err}",
                spec.service_name
            );
            return;
        }
        if let Err(err) = self.broadcast(ServiceEvent::Upsert(spec.clone())).await {
            tracing::warn!(
                target: "services",
                "failed to broadcast deployment launch detail for '{}': {err}",
                spec.service_name
            );
        }
    }

    /// Persists a retryable launch blocker so deployment progress explains why assignment paused.
    async fn persist_retryable_deployment_launch_error(
        &self,
        service_id: Uuid,
        service_name: &str,
        err: &anyhow::Error,
    ) {
        let detail =
            workload_start_retryable_detail(err).unwrap_or_else(|| service_error_detail(err));
        match self.registry.get(service_id) {
            Ok(Some(spec)) => {
                self.persist_deploying_launch_error(spec, detail).await;
            }
            Ok(None) => {
                tracing::warn!(
                    target: "services",
                    "unable to persist retryable deployment detail for '{}' because the service spec is missing",
                    service_name
                );
            }
            Err(fetch_err) => {
                tracing::warn!(
                    target: "services",
                    "unable to load service '{}' while persisting retryable deployment detail: {fetch_err}",
                    service_name
                );
            }
        }
    }

    /// Waits until one template's dependency task ids are running and ready to receive traffic.
    ///
    /// Both initial staged deployment and dependency-aware rolling updates use this to keep one
    /// downstream template from launching before every required upstream replica is actually
    /// discoverable and dataplane-ready.
    async fn update_service_status_detail_if_current(
        &self,
        service_id: Uuid,
        manifest_id: Uuid,
        detail: Option<String>,
    ) {
        let detail = detail.and_then(|detail| {
            let trimmed = detail.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        });

        let current = match self.registry.get(service_id) {
            Ok(Some(spec)) if spec.manifest_id == manifest_id => spec,
            Ok(Some(_)) | Ok(None) => return,
            Err(err) => {
                tracing::warn!(
                    target: "services",
                    "failed to load service {service_id} while updating status detail: {err}"
                );
                return;
            }
        };

        if current.status() != ServiceStatus::Deploying || current.status_detail == detail {
            return;
        }

        let mut updated = current;
        updated.set_status_detail(detail);
        if let Err(err) = self.apply_upsert(updated.clone()).await {
            tracing::warn!(
                target: "services",
                "failed to persist status detail for service '{}': {err}",
                updated.service_name
            );
            return;
        }
        if let Err(err) = self.broadcast(ServiceEvent::Upsert(updated.clone())).await {
            tracing::warn!(
                target: "services",
                "failed to broadcast status detail for service '{}': {err}",
                updated.service_name
            );
        }
    }

    /// Computes the next dependency-gate wait reason, if any, for one downstream template.
    async fn dependency_gate_wait_detail(
        &self,
        service_name: &str,
        template_name: &str,
        depends_on: &[String],
        template_replica_counts: &HashMap<String, u16>,
        dependency_task_ids: &HashMap<String, Vec<Uuid>>,
    ) -> anyhow::Result<Option<String>> {
        for dependency in depends_on {
            let expected_replicas = template_replica_counts
                .get(dependency)
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "template '{}' in service '{}' depends on unknown template '{}'",
                        template_name,
                        service_name,
                        dependency
                    )
                })? as usize;
            let Some(dependency_task_ids) = dependency_task_ids.get(dependency) else {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Assigned,
                    0,
                    expected_replicas,
                )));
            };
            if dependency_task_ids.len() != expected_replicas {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Assigned,
                    dependency_task_ids.len(),
                    expected_replicas,
                )));
            }

            let mut running_replicas = 0usize;
            let mut published_replicas = 0usize;
            for task_id in dependency_task_ids {
                let spec = self.workload_manager.inspect_workload(*task_id).await?;
                match spec.state {
                    WorkloadPhase::Running => {
                        running_replicas = running_replicas.saturating_add(1);
                        if self
                            .workload_manager
                            .ensure_task_service_traffic_ready(*task_id)
                            .await?
                        {
                            published_replicas = published_replicas.saturating_add(1);
                        }
                    }
                    WorkloadPhase::Failed | WorkloadPhase::Stopped | WorkloadPhase::Exited(_) => {
                        return Err(anyhow!(
                            "dependency task {} for template '{}' in service '{}' entered terminal state {:?}",
                            task_id,
                            dependency,
                            service_name,
                            spec.state
                        ));
                    }
                    WorkloadPhase::Pending
                    | WorkloadPhase::Pulling
                    | WorkloadPhase::Creating
                    | WorkloadPhase::VolumeUnavailable
                    | WorkloadPhase::Paused
                    | WorkloadPhase::Stopping
                    | WorkloadPhase::Unknown => {}
                }
            }

            if running_replicas != expected_replicas {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Running,
                    running_replicas,
                    expected_replicas,
                )));
            }
            if published_replicas != expected_replicas {
                return Ok(Some(format_dependency_gate_wait_detail(
                    service_name,
                    template_name,
                    dependency,
                    DependencyGateBlock::Published,
                    published_replicas,
                    expected_replicas,
                )));
            }
        }

        Ok(None)
    }

    /// Waits for dependency task templates to be assigned, running, traffic-published, and stable.
    pub(super) async fn wait_for_dependency_task_ids_ready(
        &self,
        gate: DependencyGateContext<'_>,
        dependency_task_ids: &HashMap<String, Vec<Uuid>>,
    ) -> anyhow::Result<()> {
        let startup_timeout =
            Duration::from_secs(gate.update_strategy.rolling.startup_timeout_secs.max(1) as u64);
        let monitor_window =
            Duration::from_secs(gate.update_strategy.rolling.monitor_secs.max(1) as u64);
        let deadline = Instant::now() + startup_timeout;
        let mut stable_since: Option<Instant> = None;
        let mut last_detail: Option<String> = None;

        loop {
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for dependencies {:?} of template '{}' in service '{}' to become ready",
                    gate.depends_on,
                    gate.template_name,
                    gate.service_name
                ));
            }

            if let Some(detail) = self
                .dependency_gate_wait_detail(
                    gate.service_name,
                    gate.template_name,
                    gate.depends_on,
                    gate.template_replica_counts,
                    dependency_task_ids,
                )
                .await?
            {
                stable_since = None;
                if last_detail.as_deref() != Some(detail.as_str()) {
                    self.update_service_status_detail_if_current(
                        gate.service_id,
                        gate.manifest_id,
                        Some(detail.clone()),
                    )
                    .await;
                    last_detail = Some(detail);
                }
            } else {
                let stable_at = stable_since.get_or_insert_with(Instant::now);
                if stable_at.elapsed() >= monitor_window {
                    if last_detail.is_some() {
                        self.update_service_status_detail_if_current(
                            gate.service_id,
                            gate.manifest_id,
                            None,
                        )
                        .await;
                    }
                    return Ok(());
                }

                let detail = format_dependency_gate_stability_detail(
                    gate.service_name,
                    gate.template_name,
                    gate.depends_on,
                );
                if last_detail.as_deref() != Some(detail.as_str()) {
                    self.update_service_status_detail_if_current(
                        gate.service_id,
                        gate.manifest_id,
                        Some(detail.clone()),
                    )
                    .await;
                    last_detail = Some(detail);
                }
            }

            sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
        }
    }

    /// Waits until every dependency template for one template is running and, when attached to
    /// networks, published for service traffic.
    async fn wait_for_template_dependencies_ready(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        template: &TaskTemplateSpecValue,
        template_replica_counts: &HashMap<String, u16>,
        launched_task_ids: &HashMap<String, Vec<Uuid>>,
    ) -> anyhow::Result<()> {
        self.wait_for_dependency_task_ids_ready(
            DependencyGateContext {
                service_id: compute_service_id(deployment.service_name),
                manifest_id: deployment.manifest_id,
                service_name: deployment.service_name,
                template_name: &template.name,
                depends_on: &template.depends_on,
                template_replica_counts,
                update_strategy: deployment.update_strategy,
            },
            launched_task_ids,
        )
        .await
    }

    /// Persists the current `Deploying` service snapshot with the provided replica id set.
    async fn persist_deploying_task_ids(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        replica_ids: Vec<Uuid>,
    ) -> anyhow::Result<ServiceSpecValue> {
        let service_id = compute_service_id(deployment.service_name);
        let mut spec = match self.registry.get(service_id)? {
            Some(spec) if spec.manifest_id == deployment.manifest_id => spec,
            _ => ServiceSpecValue::new(
                deployment.manifest_id,
                deployment.manifest_name.to_string(),
                deployment.service_name.to_string(),
                deployment.task_templates.to_vec(),
                Vec::new(),
            ),
        };
        spec.manifest_id = deployment.manifest_id;
        spec.manifest_name = deployment.manifest_name.to_string();
        spec.service_name = deployment.service_name.to_string();
        spec.task_templates = deployment.task_templates.to_vec();
        spec.replica_ids = replica_ids;
        spec.update_strategy = deployment.update_strategy.clone();
        spec.previous_generation = None;
        spec.set_rollout(ServiceRolloutState::default());
        spec.set_status(ServiceStatus::Deploying);
        self.apply_upsert(spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(spec.clone())).await?;
        Ok(spec)
    }

    /// Handles the initial launch failure path before any dependency-ordered task templates have been
    /// started, preserving the existing volume-unavailable recovery behavior.
    async fn handle_initial_deployment_launch_failure(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        desired_task_ids: &[Uuid],
        err: &anyhow::Error,
    ) {
        tracing::warn!(
            target: "services",
            "initial task launch for service '{}' failed: {err:#}",
            deployment.service_name
        );

        if workload_start_error_requires_service_requeue(err) {
            self.persist_retryable_deployment_launch_error(
                compute_service_id(deployment.service_name),
                deployment.service_name,
                err,
            )
            .await;
            tracing::info!(
                target: "services",
                "deferring deployment retry for '{}' until scheduling prerequisites converge",
                deployment.service_name
            );
            return;
        }

        let service_id = compute_service_id(deployment.service_name);
        let detail = service_error_detail(err);
        match self.registry.get(service_id) {
            Ok(Some(mut persisted_spec)) if is_local_volume_unavailable_error(err) => {
                persisted_spec.replica_ids = desired_task_ids.to_vec();
                persisted_spec.previous_generation = None;
                persisted_spec.set_rollout(ServiceRolloutState::default());
                persisted_spec.set_status(ServiceStatus::VolumeUnavailable);
                if let Err(upsert_err) = self.apply_upsert(persisted_spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist volume-unavailable state for '{}': {upsert_err}",
                        deployment.service_name
                    );
                } else if let Err(broadcast_err) =
                    self.broadcast(ServiceEvent::Upsert(persisted_spec)).await
                {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast volume-unavailable state for '{}': {broadcast_err}",
                        deployment.service_name
                    );
                }
            }
            Ok(Some(persisted_spec)) => {
                self.persist_deploying_launch_error(persisted_spec.clone(), detail.clone())
                    .await;
                if workload_start_error_consumes_service_failure_budget(err) {
                    let controller = self.clone();
                    tokio::task::spawn_local(async move {
                        controller.await_service_readiness(persisted_spec).await;
                    });
                }
            }
            Ok(None) if is_local_volume_unavailable_error(err) => {
                let mut blocked_spec = ServiceSpecValue::new(
                    deployment.manifest_id,
                    deployment.manifest_name.to_string(),
                    deployment.service_name.to_string(),
                    deployment.task_templates.to_vec(),
                    desired_task_ids.to_vec(),
                );
                blocked_spec.update_strategy = deployment.update_strategy.clone();
                blocked_spec.previous_generation = None;
                blocked_spec.set_rollout(ServiceRolloutState::default());
                blocked_spec.set_status(ServiceStatus::VolumeUnavailable);
                if let Err(upsert_err) = self.apply_upsert(blocked_spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist fallback volume-unavailable state for '{}': {upsert_err}",
                        deployment.service_name
                    );
                } else if let Err(broadcast_err) =
                    self.broadcast(ServiceEvent::Upsert(blocked_spec)).await
                {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast fallback volume-unavailable state for '{}': {broadcast_err}",
                        deployment.service_name
                    );
                }
            }
            Ok(None) => {
                tracing::warn!(
                    target: "services",
                    "unable to schedule deployment retry for '{}' because the service spec is missing; marking service failed",
                    deployment.service_name
                );
                let mut failed_spec = ServiceSpecValue::new(
                    deployment.manifest_id,
                    deployment.manifest_name.to_string(),
                    deployment.service_name.to_string(),
                    deployment.task_templates.to_vec(),
                    Vec::new(),
                );
                failed_spec.update_strategy = deployment.update_strategy.clone();
                failed_spec.previous_generation = None;
                failed_spec.set_rollout(ServiceRolloutState {
                    last_error: Some(detail.clone()),
                    ..ServiceRolloutState::default()
                });
                failed_spec.set_status(ServiceStatus::Failed);
                failed_spec.set_status_detail(Some(detail));
                if let Err(upsert_err) = self.apply_upsert(failed_spec.clone()).await {
                    tracing::warn!(
                        target: "services",
                        "failed to persist fallback failed state for '{}': {upsert_err}",
                        deployment.service_name
                    );
                } else if let Err(broadcast_err) =
                    self.broadcast(ServiceEvent::Upsert(failed_spec)).await
                {
                    tracing::warn!(
                        target: "services",
                        "failed to broadcast fallback failed state for '{}': {broadcast_err}",
                        deployment.service_name
                    );
                }
            }
            Err(fetch_err) => {
                tracing::warn!(
                    target: "services",
                    "unable to load service '{}' spec for retry: {fetch_err}",
                    deployment.service_name
                );
            }
        }
    }

    /// Marks the active deployment manifest as failed and stops any partially launched tasks so a
    /// dependency-ordered deployment cannot leave a half-started service behind.
    async fn mark_deployment_failed(
        &self,
        deployment: &ServiceDeploymentContext<'_>,
        reason: Option<String>,
    ) -> anyhow::Result<()> {
        let service_id = compute_service_id(deployment.service_name);
        let mut failed_spec = match self.registry.get(service_id)? {
            Some(current) if current.manifest_id == deployment.manifest_id => current,
            Some(_) => return Ok(()),
            None => ServiceSpecValue::new(
                deployment.manifest_id,
                deployment.manifest_name.to_string(),
                deployment.service_name.to_string(),
                deployment.task_templates.to_vec(),
                Vec::new(),
            ),
        };
        failed_spec.manifest_name = deployment.manifest_name.to_string();
        failed_spec.service_name = deployment.service_name.to_string();
        failed_spec.task_templates = deployment.task_templates.to_vec();
        failed_spec.update_strategy = deployment.update_strategy.clone();
        failed_spec.previous_generation = None;
        failed_spec.set_rollout(ServiceRolloutState {
            last_error: reason,
            ..ServiceRolloutState::default()
        });
        failed_spec.replica_ids.clear();
        failed_spec.set_status(ServiceStatus::Failed);
        self.apply_upsert(failed_spec.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(failed_spec.clone()))
            .await?;
        self.stop_tasks(&failed_spec).await;
        Ok(())
    }

    /// Builds the current assignment view for a service by inspecting every tracked task id.
    pub(super) async fn collect_assignments(
        &self,
        service_name: &str,
        task_ids: &[Uuid],
    ) -> Vec<ServiceReplicaAssignment> {
        let mut assignments = Vec::new();
        for task_id in task_ids {
            match self.workload_manager.inspect_workload(*task_id).await {
                Ok(spec) => {
                    if let Some((template, replica)) =
                        parse_template_and_replica(service_name, &spec.name)
                    {
                        assignments.push(ServiceReplicaAssignment {
                            task_id: spec.id,
                            template,
                            replica,
                        });
                    } else {
                        tracing::debug!(
                            target: "services",
                            "unable to map task '{}' back to service '{}' template",
                            spec.name,
                            service_name
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        target: "services",
                        "failed to inspect task {task_id} for service '{service_name}': {err}"
                    );
                }
            }
        }
        assignments
    }

    /// Starts a batch of workloads, retrying without node targets to keep deployments progressing.
    pub(super) async fn start_tasks_with_fallback(
        &self,
        mut requests: Vec<WorkloadStartRequest>,
        context: &str,
    ) -> anyhow::Result<Vec<WorkloadSpec>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let has_targets = requests.iter().any(|request| request.target_node.is_some());
        let allow_untargeted_fallback = allow_untargeted_fallback(&requests);
        let requires_pinned_targets = if has_targets {
            requests_require_pinned_targets(
                &self.volume_registry,
                &self.network_registry,
                &requests,
            )?
        } else {
            false
        };
        match self
            .workload_manager
            .start_workloads_batch(requests.clone())
            .await
        {
            Ok(specs) => Ok(specs),
            Err(err) if has_targets && requires_pinned_targets => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; local resources require preserving target nodes: {err:#}"
                );
                Err(err)
            }
            Err(err) if has_targets && workload_start_error_requires_service_requeue(&err) => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; preserving targets while scheduling prerequisites converge: {err:#}"
                );
                Err(err)
            }
            Err(err) if has_targets && !allow_untargeted_fallback => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; preserving multi-node targets for retry: {err:#}"
                );
                Err(err)
            }
            Err(err) if has_targets => {
                tracing::warn!(
                    target: "services",
                    "pinned placement failed for {context}; retrying without targets: {err:#}"
                );
                for request in &mut requests {
                    request.target_node = None;
                }
                self.workload_manager
                    .start_workloads_batch_with_scheduling_retry_limit(
                        requests,
                        Some(SERVICE_FALLBACK_SCHEDULING_RETRY_MAX_ATTEMPTS),
                    )
                    .await
                    .map_err(|err| err.context("fallback placement failed"))
            }
            Err(err) => Err(err),
        }
    }

    /// Publishes task traffic after attachment rows exist so cutover only exposes ready endpoints.
    pub(super) async fn publish_task_traffic_for_cutover(
        &self,
        service_name: &str,
        task_id: Uuid,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.wait_for_task_cutover_ready(service_name, task_id, timeout)
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to publish task {} for service '{}' during traffic cutover: {err}",
                    task_id,
                    service_name
                )
            })
    }

    /// Waits until one replacement task is both running and traffic-ready before cutover.
    ///
    /// Start-first service handoff must not swap slot ownership to a replacement until the new
    /// runtime has actually reached `Running` and every local attachment is ready to publish
    /// service traffic. Otherwise the service can momentarily point at a replica that still has
    /// attachment rows but cannot carry overlay traffic yet.
    async fn wait_for_task_cutover_ready(
        &self,
        service_name: &str,
        task_id: Uuid,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for replacement task {} in service '{}' to become traffic-ready",
                    task_id,
                    service_name
                ));
            }

            let state = self
                .workload_manager
                .workload_phase_snapshot(&[task_id])
                .await?
                .first()
                .and_then(|(_, state)| state.as_ref())
                .cloned();

            match state {
                Some(WorkloadPhase::Running) => {
                    if self
                        .workload_manager
                        .ensure_task_service_traffic_ready(task_id)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Some(WorkloadPhase::Pending)
                | Some(WorkloadPhase::Pulling)
                | Some(WorkloadPhase::Creating)
                | Some(WorkloadPhase::Unknown)
                | None => {}
                Some(other) => {
                    return Err(anyhow!(
                        "replacement task {} for service '{}' entered non-routable state {:?} before cutover",
                        task_id,
                        service_name,
                        other
                    ));
                }
            }

            sleep(Duration::from_millis(SERVICE_ROLLOUT_POLL_INTERVAL_MS)).await;
        }
    }

    /// Replaces one service slot's desired task id after a fresh replacement is ready.
    ///
    /// Service slot identity is positional inside `replica_ids`, so start-first handoff must
    /// update exactly one slot once the replacement task is ready instead of reusing the
    /// previous task id across multiple placements.
    pub(super) async fn swap_service_slot_task_id_for_cutover(
        &self,
        service_id: Uuid,
        manifest_id: Uuid,
        template_name: &str,
        replica: u16,
        previous_task_id: Uuid,
        replacement_task_id: Uuid,
    ) -> anyhow::Result<()> {
        let Some(mut current) = self.registry.get(service_id)? else {
            return Err(anyhow!(
                "service {} disappeared before slot '{}' replica {} could cut over to {}",
                service_id,
                template_name,
                replica,
                replacement_task_id
            ));
        };
        if current.manifest_id != manifest_id {
            return Err(anyhow!(
                "service '{}' advanced to manifest {} before slot '{}' replica {} could cut over",
                current.service_name,
                current.manifest_id,
                template_name,
                replica
            ));
        }

        let Some(slot_index) = service_slot_index(&current, template_name, replica) else {
            return Err(anyhow!(
                "service '{}' no longer declares slot '{}' replica {} during cutover",
                current.service_name,
                template_name,
                replica
            ));
        };

        let Some(current_task_id) = current.replica_ids.get(slot_index).copied() else {
            return Err(anyhow!(
                "service '{}' slot '{}' replica {} lost its desired task id during cutover",
                current.service_name,
                template_name,
                replica
            ));
        };

        if current_task_id == replacement_task_id {
            return Ok(());
        }
        if current_task_id != previous_task_id {
            return Err(anyhow!(
                "service '{}' slot '{}' replica {} points at {} instead of expected {} during cutover",
                current.service_name,
                template_name,
                replica,
                current_task_id,
                previous_task_id
            ));
        }

        current.replica_ids[slot_index] = replacement_task_id;
        current.phase_version = current.phase_version.saturating_add(1);
        current.touch();
        self.apply_upsert(current.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(current)).await?;
        Ok(())
    }
}

/// Resolves the positional replica-slot index stored in `ServiceSpecValue::replica_ids`.
///
/// Service slots are flattened in template order and then replica order. Slot handoff updates
/// need the exact index for one `(template, replica)` pair so the controller can replace only
/// the desired slot without disturbing the rest of the service assignment vector.
fn service_slot_index(spec: &ServiceSpecValue, template_name: &str, replica: u16) -> Option<usize> {
    let mut cursor = 0usize;
    for template in &spec.task_templates {
        for current_replica in 1..=template.replicas {
            if template.name == template_name && current_replica == replica {
                return Some(cursor);
            }
            cursor = cursor.saturating_add(1);
        }
    }
    None
}

/// Builds a compact service status detail from an error and its causal chain.
fn service_error_detail(err: &anyhow::Error) -> String {
    let parts: Vec<String> = err
        .chain()
        .map(ToString::to_string)
        .filter(|part| !part.trim().is_empty())
        .collect();

    if parts.is_empty() {
        return err.to_string();
    }

    parts.join(": ")
}

/// Normalizes one service-facing status detail before persisting it.
fn normalize_service_status_detail(detail: String) -> Option<String> {
    let detail = detail.trim();
    (!detail.is_empty()).then(|| detail.to_string())
}

/// Returns true when a submission matches the active running service spec exactly.
///
/// This preserves idempotent `services run` behavior by rejecting unchanged
/// submissions before any generation/status mutation is broadcast.
fn is_running_deployment_noop(
    existing: &ServiceSpecValue,
    manifest_name: &str,
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
    update_strategy: &ServiceUpdateStrategy,
) -> bool {
    existing.status() == ServiceStatus::Running
        && existing.manifest_name == manifest_name
        && existing.service_name == service_name
        && existing.task_templates == task_templates
        && existing.update_strategy == *update_strategy
}

/// Deduplicates service-required networks while rejecting conflicting driver or family requests.
fn normalize_required_networks(
    required_networks: &[ServiceRequiredNetworkSpec],
) -> anyhow::Result<Vec<ServiceRequiredNetworkSpec>> {
    let mut normalized: BTreeMap<String, ServiceRequiredNetworkSpec> = BTreeMap::new();
    for network in required_networks {
        let name = network.name.trim();
        if name.is_empty() {
            continue;
        }

        if let Some(existing) = normalized.get_mut(name) {
            if existing.driver != network.driver {
                return Err(anyhow!(
                    "service deployment requests network '{}' with conflicting drivers",
                    name
                ));
            }
            match (existing.ip_family, network.ip_family) {
                (ServiceRequiredNetworkIpFamily::Ipv4, ServiceRequiredNetworkIpFamily::Ipv6)
                | (ServiceRequiredNetworkIpFamily::Ipv6, ServiceRequiredNetworkIpFamily::Ipv4) => {
                    return Err(anyhow!(
                        "service deployment requests network '{}' with conflicting IP families",
                        name
                    ));
                }
                (ServiceRequiredNetworkIpFamily::Default, explicit)
                    if explicit != ServiceRequiredNetworkIpFamily::Default =>
                {
                    existing.ip_family = explicit;
                }
                _ => {}
            }
            continue;
        }

        normalized.insert(
            name.to_string(),
            ServiceRequiredNetworkSpec {
                name: name.to_string(),
                driver: network.driver,
                ip_family: network.ip_family,
            },
        );
    }

    Ok(normalized.into_values().collect())
}

/// Resolves the daemon's default network IP family for service-side auto-provisioning.
fn default_required_network_family() -> ServiceRequiredNetworkIpFamily {
    let (has_ipv4, has_ipv6) = crate::node::address::detect_local_ip_families();
    match infer_default_ip_family(
        config::nodeport_ip(),
        config::advertise_addr().as_deref(),
        config::default_ip_family_policy(),
        has_ipv4,
        has_ipv6,
    ) {
        IpFamily::Ipv4 => ServiceRequiredNetworkIpFamily::Ipv4,
        IpFamily::Ipv6 => ServiceRequiredNetworkIpFamily::Ipv6,
    }
}

/// Computes a deterministic default subnet for an auto-provisioned service network.
fn default_required_network_subnet<I, S>(
    name: &str,
    existing_subnets: I,
    family: ServiceRequiredNetworkIpFamily,
) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let used: BTreeSet<String> = existing_subnets
        .into_iter()
        .map(|subnet| subnet.as_ref().trim().to_string())
        .collect();
    let hash = default_required_network_subnet_hash(name);
    let candidates = default_required_network_subnet_candidate_count(family);

    for offset in 0..candidates {
        let candidate = default_required_network_subnet_candidate(hash, offset, family);
        if !used.contains(&candidate) {
            return candidate;
        }
    }

    default_required_network_subnet_candidate(hash, 0, family)
}

/// Hashes a network name into a stable default-subnet selection seed.
fn default_required_network_subnet_hash(name: &str) -> u32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&digest.as_bytes()[..4]);
    u32::from_le_bytes(bytes)
}

/// Returns the number of deterministic subnet candidates in the requested family.
fn default_required_network_subnet_candidate_count(family: ServiceRequiredNetworkIpFamily) -> u32 {
    match family {
        ServiceRequiredNetworkIpFamily::Default | ServiceRequiredNetworkIpFamily::Ipv4 => {
            SERVICE_DEFAULT_NETWORK_SUBNET_CANDIDATES_V4
        }
        ServiceRequiredNetworkIpFamily::Ipv6 => SERVICE_DEFAULT_NETWORK_SUBNET_CANDIDATES_V6,
    }
}

/// Converts a deterministic subnet candidate offset into a concrete CIDR string.
fn default_required_network_subnet_candidate(
    hash: u32,
    offset: u32,
    family: ServiceRequiredNetworkIpFamily,
) -> String {
    match family {
        ServiceRequiredNetworkIpFamily::Default | ServiceRequiredNetworkIpFamily::Ipv4 => {
            default_required_network_subnet_candidate_v4(hash, offset)
        }
        ServiceRequiredNetworkIpFamily::Ipv6 => {
            default_required_network_subnet_candidate_v6(hash, offset)
        }
    }
}

/// Converts one candidate offset into a unique `10.0.0.0/8` `/20` subnet.
fn default_required_network_subnet_candidate_v4(hash: u32, offset: u32) -> String {
    let seed = hash & (SERVICE_DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let bucket = seed.wrapping_add(offset) & (SERVICE_DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let second_octet = (bucket >> 4) as u8;
    let third_octet = ((bucket & 0x0f) << 4) as u8;
    format!("10.{second_octet}.{third_octet}.0/{SERVICE_DEFAULT_NETWORK_SUBNET_PREFIX_V4}")
}

/// Converts one candidate offset into a unique `fd42::/16` `/64` subnet.
fn default_required_network_subnet_candidate_v6(hash: u32, offset: u32) -> String {
    let group = (hash >> 16) as u16;
    let seed = hash as u16;
    let bucket = seed.wrapping_add(offset as u16);
    format!("fd42:{group:04x}:{bucket:04x}::/{SERVICE_DEFAULT_NETWORK_SUBNET_PREFIX_V6}")
}

/// Formats a bounded list of network readiness blockers for service status details.
fn format_service_network_readiness_blockers(blockers: &BTreeSet<String>) -> String {
    let mut parts = Vec::new();
    for blocker in blockers.iter().take(3) {
        parts.push(blocker.clone());
    }
    if blockers.len() > parts.len() {
        let remaining = blockers.len() - parts.len();
        parts.push(format!("{remaining} more blocker(s)"));
    }
    parts.join("; ")
}

struct ServiceDeploymentJob {
    manifest_id: Uuid,
    manifest_name: String,
    service_name: String,
    task_templates: Vec<TaskTemplateSpecValue>,
    update_strategy: ServiceUpdateStrategy,
    assigned_task_ids: Vec<Uuid>,
}

/// Bundles immutable deployment manifest context shared across dependency-order helpers.
///
/// Passing one borrowed context keeps the staged deployment helpers aligned on the same manifest
/// generation without repeatedly threading the same identifiers and template vectors through
/// every failure and persistence path.
struct ServiceDeploymentContext<'a> {
    manifest_id: Uuid,
    manifest_name: &'a str,
    service_name: &'a str,
    task_templates: &'a [TaskTemplateSpecValue],
    update_strategy: &'a ServiceUpdateStrategy,
}

pub(super) struct ServiceRedeploymentJob {
    pub(super) manifest_id: Uuid,
    pub(super) manifest_name: String,
    pub(super) service_name: String,
    pub(super) task_templates: Vec<TaskTemplateSpecValue>,
    pub(super) current_spec: ServiceSpecValue,
    pub(super) update_strategy: ServiceUpdateStrategy,
}

/// Bundles immutable metadata for one dependency gate while a downstream template is blocked.
#[derive(Clone, Copy)]
pub(super) struct DependencyGateContext<'a> {
    pub(super) service_id: Uuid,
    pub(super) manifest_id: Uuid,
    pub(super) service_name: &'a str,
    pub(super) template_name: &'a str,
    pub(super) depends_on: &'a [String],
    pub(super) template_replica_counts: &'a HashMap<String, u16>,
    pub(super) update_strategy: &'a ServiceUpdateStrategy,
}

/// Distinguishes the dependency-gate phase that is currently blocking one downstream template.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DependencyGateBlock {
    Assigned,
    Running,
    Published,
}

#[cfg(test)]
mod deployment_tests {
    use super::*;

    #[test]
    /// Required network normalization rejects driver conflicts for the same network name.
    fn normalize_required_networks_rejects_conflicting_drivers() {
        let err = normalize_required_networks(&[
            ServiceRequiredNetworkSpec {
                name: "shared".to_string(),
                driver: NetworkDriver::Vxlan,
                ip_family: ServiceRequiredNetworkIpFamily::Default,
            },
            ServiceRequiredNetworkSpec {
                name: "shared".to_string(),
                driver: NetworkDriver::Bridge,
                ip_family: ServiceRequiredNetworkIpFamily::Default,
            },
        ])
        .expect_err("conflicting drivers should fail");

        assert!(
            err.to_string().contains("conflicting drivers"),
            "unexpected error: {err}"
        );
    }

    #[test]
    /// Default-subnet selection probes away from an already used IPv4 candidate.
    fn default_required_network_subnet_skips_used_ipv4_candidate() {
        let initial = default_required_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            ServiceRequiredNetworkIpFamily::Ipv4,
        );
        let resolved = default_required_network_subnet(
            "alpha",
            [initial.as_str()],
            ServiceRequiredNetworkIpFamily::Ipv4,
        );

        assert_ne!(initial, resolved);
        assert!(resolved.ends_with("/20"));
    }

    #[test]
    /// Default-subnet selection probes away from an already used IPv6 candidate.
    fn default_required_network_subnet_skips_used_ipv6_candidate() {
        let initial = default_required_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            ServiceRequiredNetworkIpFamily::Ipv6,
        );
        let resolved = default_required_network_subnet(
            "alpha",
            [initial.as_str()],
            ServiceRequiredNetworkIpFamily::Ipv6,
        );

        assert_ne!(initial, resolved);
        assert!(resolved.starts_with("fd42:"));
        assert!(resolved.ends_with("/64"));
    }
}

/// Formats one human-readable dependency wait reason for persisted service status details.
fn format_dependency_gate_wait_detail(
    service_name: &str,
    template_name: &str,
    dependency_name: &str,
    block: DependencyGateBlock,
    ready_replicas: usize,
    expected_replicas: usize,
) -> String {
    match block {
        DependencyGateBlock::Assigned => format!(
            "service '{service_name}' waiting for dependency template '{dependency_name}' before launching template '{template_name}' ({ready_replicas}/{expected_replicas} replicas assigned)"
        ),
        DependencyGateBlock::Running => format!(
            "service '{service_name}' waiting for dependency template '{dependency_name}' before launching template '{template_name}' ({ready_replicas}/{expected_replicas} replicas running)"
        ),
        DependencyGateBlock::Published => format!(
            "service '{service_name}' waiting for dependency template '{dependency_name}' before launching template '{template_name}' ({ready_replicas}/{expected_replicas} replicas traffic-published)"
        ),
    }
}

/// Formats the stability-window message shown after dependencies become ready but before cutover.
fn format_dependency_gate_stability_detail(
    service_name: &str,
    template_name: &str,
    depends_on: &[String],
) -> String {
    let dependency_summary = depends_on.join(", ");
    format!(
        "service '{service_name}' monitoring dependency readiness before launching template '{template_name}' ({dependency_summary})"
    )
}

/// Records launched task ids back into the `(template, replica)` assignment index used to build
/// Records launched task ids back into the `(template, replica)` assignment index used to build
/// ordered service task id lists during dependency-ordered deployment.
fn record_task_assignments(
    service_name: &str,
    task_specs: &[WorkloadSpec],
    assignments: &mut BTreeMap<(String, u16), Uuid>,
) {
    for spec in task_specs {
        let Some((template, replica)) = parse_template_and_replica(service_name, &spec.name) else {
            tracing::warn!(
                target: "services",
                "unable to map dependency-ordered task '{}' back to service '{}' template metadata",
                spec.name,
                service_name
            );
            continue;
        };
        assignments.insert((template, replica), spec.id);
    }
}

/// Returns the currently known task ids in manifest template/replica order without warning about
/// later task templates that have not launched yet.
fn ordered_known_task_ids(
    task_templates: &[TaskTemplateSpecValue],
    assignments: &BTreeMap<(String, u16), Uuid>,
) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for template in task_templates {
        for replica in 1..=template.replicas {
            if let Some(task_id) = assignments.get(&(template.name.clone(), replica)) {
                ids.push(*task_id);
            }
        }
    }
    ids
}

/// Computes the ordered task identifiers for the manifest by iterating template/replica pairs.
pub(super) fn order_task_ids(
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
    assignments: &BTreeMap<(String, u16), Uuid>,
) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for template in task_templates {
        for replica in 1..=template.replicas {
            let key = (template.name.clone(), replica);
            match assignments.get(&key) {
                Some(task_id) => ids.push(*task_id),
                None => {
                    tracing::warn!(
                        target: "services",
                        "missing replica assignment for template '{}' replica {} while updating '{}'",
                        template.name,
                        replica,
                        service_name
                    );
                }
            }
        }
    }
    ids
}
