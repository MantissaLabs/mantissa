use crate::cluster::ClusterViewId;
use crate::cluster::operations::{
    SplitNodeAssignment, SplitNodeCandidate, SplitTargetSpec, build_split_assignments_for_nodes,
};
use crate::topology::Topology;
use crate::topology::peers::PeerValue;
use std::collections::HashMap;
use uuid::Uuid;

impl Topology {
    /// Applies host resource metadata from an `Info` payload onto one split node candidate.
    fn apply_split_node_info(
        candidate: &mut SplitNodeCandidate,
        info: protocol::info_capnp::info::Reader<'_>,
    ) {
        if let Ok(cpu) = info.get_cpu() {
            if let Ok(vendor) = cpu.get_vendor() {
                let text = vendor.to_string().unwrap_or_default();
                if !text.is_empty() {
                    candidate.cpu_vendor = Some(text);
                }
            }
            if let Ok(brand) = cpu.get_brand() {
                let text = brand.to_string().unwrap_or_default();
                if !text.is_empty() {
                    candidate.cpu_brand = Some(text);
                }
            }
            let logical = cpu.get_logical_cpus();
            if logical > 0 {
                candidate.cpu_logical = Some(logical as u64);
            }
            let cores = cpu.get_num_cores();
            if cores > 0 {
                candidate.cpu_cores = Some(cores as u64);
            }
        }

        if let Ok(memory) = info.get_memory() {
            let total = memory.get_total();
            if total > 0 {
                candidate.memory_total_kb = Some(total);
            }
        }

        if let Ok(gpu) = info.get_gpu() {
            if let Ok(vendor) = gpu.get_vendor() {
                let text = vendor.to_string().unwrap_or_default();
                if !text.is_empty() {
                    candidate.gpu_vendor = Some(text);
                }
            }
            if let Ok(devices) = gpu.get_devices() {
                candidate.gpu_count = Some(devices.len() as u64);
                let mut models = Vec::with_capacity(devices.len() as usize);
                for device in devices.iter() {
                    if let Ok(name) = device.get_name() {
                        let text = name.to_string().unwrap_or_default();
                        if !text.is_empty() {
                            models.push(text);
                        }
                    }
                }
                candidate.gpu_models = models;
            }
        }
    }

    /// Collects a deterministic snapshot of nodes eligible for split partition assignment.
    pub(in crate::topology) async fn collect_split_node_candidates(
        &self,
        source_view: ClusterViewId,
    ) -> Result<Vec<SplitNodeCandidate>, capnp::Error> {
        let (actives, _) = self
            .stores
            .peers
            .load_all_regs()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let mut candidates: HashMap<Uuid, SplitNodeCandidate> = HashMap::new();
        for (key, reg) in actives {
            let Some(value) = PeerValue::select_reg(&reg).filter(|value| value.is_active()) else {
                continue;
            };

            let node_id = key.to_uuid();
            let wireguard_enabled = value
                .wireguard
                .as_ref()
                .map(|wg| wg.enabled)
                .unwrap_or(false);
            candidates.insert(
                node_id,
                SplitNodeCandidate {
                    node_id,
                    hostname: value.hostname.clone(),
                    address: value.address.clone(),
                    wireguard_enabled,
                    labels: value.labels.labels.clone(),
                    cpu_vendor: None,
                    cpu_brand: None,
                    cpu_logical: None,
                    cpu_cores: None,
                    memory_total_kb: None,
                    gpu_vendor: None,
                    gpu_count: None,
                    gpu_models: Vec::new(),
                },
            );
        }

        let self_entry =
            candidates
                .entry(self.local.node.id)
                .or_insert_with(|| SplitNodeCandidate {
                    node_id: self.local.node.id,
                    hostname: self
                        .local
                        .node
                        .system_info
                        .info
                        .hostname
                        .clone()
                        .unwrap_or_default(),
                    address: self
                        .compute_advertise_addr()
                        .unwrap_or_else(|_| String::new()),
                    wireguard_enabled: false,
                    labels: self.current_label_state().labels,
                    cpu_vendor: None,
                    cpu_brand: None,
                    cpu_logical: None,
                    cpu_cores: None,
                    memory_total_kb: None,
                    gpu_vendor: None,
                    gpu_count: None,
                    gpu_models: Vec::new(),
                });
        if let Some(cpu) = self.local.node.system_info.info.cpu_info.as_ref() {
            self_entry.cpu_vendor = cpu.vendor.clone();
            self_entry.cpu_brand = cpu.brand.clone();
            if cpu.num_logical_cpus > 0 {
                self_entry.cpu_logical = Some(cpu.num_logical_cpus as u64);
            }
            if cpu.num_cores > 0 {
                self_entry.cpu_cores = Some(cpu.num_cores as u64);
            }
        }
        if let Some(memory) = self.local.node.system_info.info.mem_info.as_ref()
            && memory.total > 0
        {
            self_entry.memory_total_kb = Some(memory.total);
        }
        if let Some(gpu) = self.local.node.system_info.info.gpu_info.as_ref() {
            if !gpu.vendor.is_empty() {
                self_entry.gpu_vendor = Some(gpu.vendor.clone());
            }
            self_entry.gpu_count = Some(gpu.devices.len() as u64);
            self_entry.gpu_models = gpu
                .devices
                .iter()
                .map(|device| device.name.clone())
                .filter(|name| !name.is_empty())
                .collect();
        }

        let excluded_peers = self.excluded_peers_snapshot().await;
        let mut values = candidates
            .into_values()
            .filter(|candidate| {
                candidate.node_id == self.local.node.id
                    || !excluded_peers.contains(&candidate.node_id)
            })
            .collect::<Vec<_>>();
        values.sort_by_key(|candidate| candidate.node_id);

        for candidate in &mut values {
            if candidate.node_id == self.local.node.id {
                continue;
            }

            let Some(session) = self.deps.registry.session_for_peer(candidate.node_id).await else {
                continue;
            };
            let peer_view = match Self::session_cluster_view(&session).await {
                Ok(view) => view,
                Err(_) => continue,
            };
            if peer_view != source_view {
                continue;
            }

            let node = session.get_node_request().send().pipeline.get_node();
            if let Ok(response) = node.info_request().send().promise.await
                && let Ok(info_reader) = response.get().and_then(|reader| reader.get_info())
            {
                Self::apply_split_node_info(candidate, info_reader);
            }
        }

        Ok(values)
    }

    /// Computes deterministic split assignments and validates selector coverage for all nodes.
    pub(in crate::topology) async fn build_split_assignments(
        &self,
        source_view: ClusterViewId,
        targets: &[SplitTargetSpec],
    ) -> Result<Vec<SplitNodeAssignment>, capnp::Error> {
        let nodes = self.collect_split_node_candidates(source_view).await?;
        build_split_assignments_for_nodes(source_view, targets, &nodes)
    }
}
