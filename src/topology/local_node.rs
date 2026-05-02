use super::*;
use crate::topology::peers::write_peer;

impl Topology {
    /// Returns the current converged scheduling state for the local node.
    pub(super) fn current_scheduling_state(&self) -> PeerSchedulingState {
        self.deps
            .registry
            .peer_scheduling(self.local.node.id)
            .unwrap_or_else(|| PeerSchedulingState::schedulable_default(self.local.node.id))
    }

    /// Returns the current converged label state for the local node.
    pub(super) fn current_label_state(&self) -> crate::topology::peers::PeerLabelState {
        self.deps
            .registry
            .peer_labels(self.local.node.id)
            .unwrap_or_default()
    }

    /// Returns the capability registry used by topology-owned integrations.
    pub fn registry(&self) -> Registry {
        self.deps.registry.clone()
    }

    /// Records the socket address bound by the local server listener.
    pub fn set_bound_addr(&self, sa: SocketAddr) {
        self.local.advertise.set_bound(sa);
    }

    /// Rebuild and persist the local peer row after the runtime learns a more
    /// accurate advertise address.
    ///
    /// Headless TCP tests bind on `127.0.0.1:0`, so the real port is unknown
    /// until the listener comes up. Refreshing the self row here updates the
    /// advertised address and any WireGuard port derived from it without
    /// waiting for unrelated local state changes.
    pub async fn refresh_local_peer_row(&self) -> io::Result<()> {
        let value = self.build_local_peer_value()?;
        self.stores
            .peers
            .upsert(&UuidKey::from(self.local.node.id), value)
            .await
            .map_err(|err| io::Error::other(format!("failed to refresh local peer row: {err}")))
    }

    /// Returns the local node id exported by this topology instance.
    pub fn self_id(&self) -> Uuid {
        self.local.node.id
    }

    /// Overrides the published advertise address with an operator- or test-supplied value.
    pub fn set_advertise_override<S: Into<String>>(&self, s: Option<S>) {
        self.local.advertise.set_override(s);
    }

    /// Sets the server handle to be served to other peers and persists the local peer row before
    /// the node starts accepting control-plane operations that depend on self visibility.
    pub async fn set_server_handle(&self, handle: server::Client) -> Result<(), server::Client> {
        let registry = self.deps.registry.clone();
        let local_id = self.local.node.id;
        let local_incarnation = self.swim_local_incarnation();

        // Compute advertise address before registering. If this fails we abort so the node
        // does not appear joined without a reachable address.
        let value = match self.build_local_peer_value() {
            Ok(value) => value,
            Err(e) => {
                log::error!(
                    "topology: failed to build local peer row during server handle setup: {e}"
                );
                return Err(handle);
            }
        };

        let first_set = self.local.server_handle.set(handle.clone()).is_ok();
        if !first_set {
            log::debug!("server_handle already set, ignoring duplicate set");
        }

        registry.register_peer_handle(local_id, handle).await;

        if let Err(e) = self
            .stores
            .peers
            .upsert(&UuidKey::from(local_id), value)
            .await
        {
            log::warn!("failed to upsert self peer: {e}");
        }

        self.deps
            .health_monitor
            .record_join(local_id, local_incarnation);

        Ok(())
    }

    /// Build the local peer-store row from the node's current runtime state.
    ///
    /// This is used both during initial server-handle publication and later
    /// when the listener learns its actual bound address.
    fn build_local_peer_value(&self) -> io::Result<PeerValue> {
        let advertise = self.compute_advertise_addr()?;
        let preferred_wireguard_port = extract_port(&advertise).ok();
        let host = self
            .local
            .node
            .system_info
            .info
            .hostname
            .clone()
            .unwrap_or_default();
        let public_key = self.local.public_key.to_bytes();
        let verifying_key = self.local.signing_key.verifying_key();
        let signing_pub = verifying_key.to_bytes();
        let identity_sig = crate::node::identity::sign_peer_identity(
            &self.local.signing_key,
            &self.local.node.id,
            &public_key,
            &signing_pub,
        );

        let wireguard = if !config::wireguard_enabled() || !mantissa_net::paths::running_as_root() {
            None
        } else {
            match mantissa_net::wireguard::resolve_wireguard_key_path()
                .and_then(mantissa_net::wireguard::load_or_generate_wireguard_keys)
            {
                Ok(keys) => {
                    match mantissa_net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
                        preferred_wireguard_port,
                        config::wireguard_port_override(),
                    ) {
                        Ok(port) => Some(crate::topology::peers::WireGuardPeerValue {
                            public_key: keys.public_bytes(),
                            port,
                            enabled: self
                                .deps
                                .registry
                                .peer_wireguard(self.local.node.id)
                                .map(|wg| wg.enabled)
                                .unwrap_or(false),
                        }),
                        Err(err) => {
                            log::warn!(
                                "failed to resolve WireGuard listen port; continuing without underlay encryption: {err}"
                            );
                            None
                        }
                    }
                }
                Err(err) => {
                    log::warn!(
                        "failed to load WireGuard keys; continuing without underlay encryption: {err}"
                    );
                    None
                }
            }
        };

        Ok(PeerValue {
            address: advertise,
            hostname: host,
            platform_os: std::env::consts::OS.to_string(),
            platform_arch: std::env::consts::ARCH.to_string(),
            noise_static_pub: public_key,
            signing_pub,
            identity_sig: identity_sig.to_vec(),
            wireguard,
            scheduling: self.current_scheduling_state(),
            labels: self.current_label_state(),
            runtime_support: self.local.runtime_support.clone(),
            root_schema: self.root_schema_info(),
            membership: PeerMembership::active(self.swim_local_incarnation()),
        })
    }

    /// Computes what we publish in NodeInfo.addr / PeerValue.address.
    /// Order of precedence:
    /// 1) explicit override (e.g., operator-configured public address or "inproc://<uuid>")
    /// 2) actual bound addr (if known) — if ip is 0.0.0.0, replace ip but keep the bound port
    /// 3) configured addr (initial value) — if ip is 0.0.0.0, compute a best-effort ip but keep its port
    pub fn compute_advertise_addr(&self) -> io::Result<String> {
        if let Some(s) = self.local.advertise.override_addr() {
            return Ok(s);
        }

        if let Some(bound) = self.local.advertise.bound()
            && !bound.ip().is_unspecified()
        {
            return Ok(bound.to_string());
        }

        if let Ok(cfg_sa) = self.local.advertise.configured().parse::<SocketAddr>()
            && !cfg_sa.ip().is_unspecified()
            && cfg_sa.port() != 0
        {
            return Ok(cfg_sa.to_string());
        }

        if self
            .local
            .advertise
            .configured()
            .parse::<SocketAddr>()
            .is_err()
        {
            return Ok(self.local.advertise.configured().to_string());
        }

        let preferred_family = match config::default_ip_family_policy() {
            crate::ip_family::DefaultIpFamilyPolicy::Ipv6 => Some(crate::ip_family::IpFamily::Ipv6),
            crate::ip_family::DefaultIpFamilyPolicy::Ipv4 => Some(crate::ip_family::IpFamily::Ipv4),
            crate::ip_family::DefaultIpFamilyPolicy::Auto => None,
        };
        let ip = compute_advertise_ip(None, None, preferred_family).map_err(|e| {
            io::Error::new(e.kind(), format!("failed to compute advertise ip: {e}"))
        })?;

        if let Some(bound) = self.local.advertise.bound() {
            return Ok(SocketAddr::new(ip, bound.port()).to_string());
        }

        if let Ok(cfg_sa) = self.local.advertise.configured().parse::<SocketAddr>() {
            return Ok(SocketAddr::new(ip, cfg_sa.port()).to_string());
        }

        Ok(self.local.advertise.configured().to_string())
    }

    /// Returns the locally exported Cap'n Proto server capability, if it has been published.
    pub fn get_server_handle(&self) -> Option<ServerClient> {
        self.local.server_handle.get().cloned()
    }

    /// Return true if we have a stored ticket for `peer_id` in local sessions.
    #[allow(dead_code)]
    pub fn has_ticket(&self, peer_id: Uuid) -> bool {
        matches!(self.stores.local_sessions.get(peer_id), Ok(Some(_)))
    }

    /// Current Peers MST root digest (16 bytes) as seen locally.
    pub async fn peers_root_digest(&self) -> std::io::Result<[u8; 16]> {
        Ok(self.stores.peers.root_digest().await)
    }

    /// Populate a NodeInfo builder with this node's identity and addresses.
    pub fn populate_self_node_info(&self, mut info: crate::topology_capnp::node_info::Builder) {
        let cluster_view = self.active_cluster_view();
        let root_schema = self.root_schema_info();

        set_node_id(info.reborrow().init_id(), &self.local.node.id);
        cluster_view.write_capnp(info.reborrow().init_active_cluster_view());

        if let Some(h) = self.get_server_handle() {
            info.set_handle(h);
        }

        let host = self
            .local
            .node
            .system_info
            .info
            .hostname
            .clone()
            .unwrap_or_default();
        let addr = self
            .compute_advertise_addr()
            .unwrap_or_else(|_| String::new());
        let preferred_wireguard_port = extract_port(&addr).ok();

        let noise_pub = self.local.public_key.to_bytes();
        let signing_pub = self.local.signing_key.verifying_key().to_bytes();
        let identity_sig = crate::node::identity::sign_peer_identity(
            &self.local.signing_key,
            &self.local.node.id,
            &noise_pub,
            &signing_pub,
        );

        let scheduling = self.current_scheduling_state();
        info.set_drain_state(drain_state_from_scheduling(&scheduling));
        let labels = self.current_label_state();

        let wireguard = if config::wireguard_enabled() && mantissa_net::paths::running_as_root() {
            match mantissa_net::wireguard::resolve_wireguard_key_path()
                .and_then(mantissa_net::wireguard::load_or_generate_wireguard_keys)
            {
                Ok(keys) => {
                    match mantissa_net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
                        preferred_wireguard_port,
                        config::wireguard_port_override(),
                    ) {
                        Ok(port) => {
                            let enabled = self
                                .deps
                                .registry
                                .peer_wireguard(self.local.node.id)
                                .map(|wg| wg.enabled)
                                .unwrap_or(false);
                            Some(WireGuardPeerValue {
                                public_key: keys.public_bytes(),
                                port,
                                enabled,
                            })
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "topology",
                                "failed to resolve WireGuard listen port for NodeInfo: {err}"
                            );
                            None
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        target: "topology",
                        "failed to load WireGuard keys for NodeInfo: {err}"
                    );
                    None
                }
            }
        } else {
            None
        };

        let peer = PeerValue {
            address: addr,
            hostname: host,
            platform_os: std::env::consts::OS.to_string(),
            platform_arch: std::env::consts::ARCH.to_string(),
            noise_static_pub: noise_pub,
            signing_pub,
            identity_sig: identity_sig.to_vec(),
            wireguard,
            scheduling,
            labels,
            runtime_support: self.local.runtime_support.clone(),
            root_schema,
            membership: PeerMembership::active(self.swim_local_incarnation()),
        };
        write_peer(info.reborrow().init_peer(), &peer);
    }

    /// True if we already have at least one peer (not ourselves) or any stored ticket.
    pub async fn already_joined(&self) -> std::io::Result<bool> {
        if let Some(local_membership) = self.local_membership()?
            && !local_membership.is_active()
        {
            return Ok(false);
        }

        if !self
            .stores
            .local_sessions
            .list_valid_records(true)?
            .is_empty()
        {
            return Ok(true);
        }

        let (actives, _) = self.stores.peers.load_all_regs()?;
        let me = self.local.node.id;
        Ok(actives.iter().any(|(k, reg)| {
            k.to_uuid() != me
                && PeerValue::select_reg(reg)
                    .map(|value| value.is_active())
                    .unwrap_or(false)
        }))
    }

    /// Returns the currently selected membership state for the local peer row, if present.
    pub(super) fn local_membership(&self) -> std::io::Result<Option<PeerMembership>> {
        let reg = self
            .stores
            .peers
            .get_reg(&UuidKey::from(self.local.node.id))
            .map_err(io::Error::other)?;
        Ok(reg
            .as_ref()
            .and_then(PeerValue::select_reg)
            .map(|value| value.membership))
    }

    /// Returns whether this node should originate outbound cluster traffic right now.
    pub(super) fn local_allows_outbound_cluster_traffic(&self) -> bool {
        match self.local_membership() {
            Ok(Some(membership)) => membership.is_active(),
            Ok(None) => true,
            Err(err) => {
                warn!(
                    target: "topology",
                    "failed to resolve local membership for outbound traffic gate: {err}"
                );
                false
            }
        }
    }
}
