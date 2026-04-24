use anyhow::Result;
use async_trait::async_trait;
use std::net::IpAddr;
use uuid::Uuid;

pub(crate) use crate::network::naming::{
    bridge_name, host_access_host_iface_name, host_access_peer_iface_name, host_iface_name,
    instance_iface_name, vxlan_name,
};
use crate::runtime::types::RuntimeAttachmentTarget;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use linux::AttachmentProvisioner;

#[cfg(target_os = "linux")]
pub use linux::AttachmentProvisioner as PlatformAttachmentProvisioner;

#[cfg(not(target_os = "linux"))]
pub type PlatformAttachmentProvisioner = AttachmentProvisioner;

/// Parameters required to provision an attachment for one runtime-defined network target.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct AttachmentProvisioningRequest<'a> {
    pub bridge_name: &'a str,
    pub mtu: u32,
    pub attachment_id: Uuid,
    pub attachment_target: &'a RuntimeAttachmentTarget,
    pub assigned_ip: &'a str,
    pub prefix: u8,
    pub mac: &'a str,
}

#[async_trait]
pub trait AttachmentProvisionerApi: Send + Sync {
    /// Report whether the host-side attachment interface for this attachment still exists.
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool>;

    /// Create or repair the veth pair and namespace configuration for one runtime attachment.
    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()>;

    /// Remove host-side attachment state when a workload leaves an overlay network.
    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()>;

    /// Program a static VXLAN FDB entry so a task MAC forwards to the remote underlay address.
    #[allow(dead_code)]
    async fn ensure_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<bool>;

    /// Remove one static VXLAN FDB entry when the corresponding remote task is withdrawn.
    #[allow(dead_code)]
    async fn remove_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<()>;

    /// Program the VXLAN flood entry used for unknown unicast and broadcast delivery to a peer.
    #[allow(dead_code)]
    async fn ensure_flood_entry(&self, vxlan_name: &str, dst: IpAddr) -> Result<bool>;

    /// Remove a peer flood entry when the peer no longer participates in the overlay.
    #[allow(dead_code)]
    async fn remove_flood_entry(&self, vxlan_name: &str, dst: IpAddr) -> Result<()>;

    /// List remote VXLAN FDB entries so reconciliation can identify stale forwarding state.
    #[allow(dead_code)]
    async fn list_remote_fdb(&self, vxlan_name: &str) -> Result<Vec<(String, IpAddr)>>;
}

#[cfg(not(target_os = "linux"))]
#[derive(Clone, Default)]
pub struct AttachmentProvisioner;

#[cfg(not(target_os = "linux"))]
impl AttachmentProvisioner {
    /// Create a no-op provisioner on unsupported platforms so higher layers can still compile.
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    /// Return a no-op provisioner after platform initialization failed or is unavailable.
    pub fn unavailable() -> Self {
        Self
    }

    /// Report that unsupported platforms never have host attachment interfaces.
    #[allow(dead_code)]
    pub async fn attachment_exists(&self, _attachment_id: Uuid) -> Result<bool> {
        Ok(false)
    }

    /// Ignore attachment creation on unsupported platforms while preserving the async contract.
    #[allow(dead_code)]
    pub async fn ensure_attachment(
        &self,
        _request: &AttachmentProvisioningRequest<'_>,
    ) -> Result<()> {
        Ok(())
    }

    /// Ignore attachment teardown on unsupported platforms.
    #[allow(dead_code)]
    pub async fn teardown_attachment(&self, _attachment_id: Uuid) -> Result<()> {
        Ok(())
    }

    /// Pretend remote FDB entries are converged when no kernel VXLAN dataplane exists.
    #[allow(dead_code)]
    pub async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    /// Ignore remote FDB removal on unsupported platforms.
    #[allow(dead_code)]
    pub async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    /// Pretend flood entries are converged when no kernel VXLAN dataplane exists.
    #[allow(dead_code)]
    pub async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<bool> {
        Ok(true)
    }

    /// Ignore flood-entry removal on unsupported platforms.
    #[allow(dead_code)]
    pub async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<()> {
        Ok(())
    }

    /// Return an empty FDB inventory on unsupported platforms.
    #[allow(dead_code)]
    pub async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, IpAddr)>> {
        Ok(Vec::new())
    }
}

#[cfg(not(target_os = "linux"))]
#[async_trait]
impl AttachmentProvisionerApi for AttachmentProvisioner {
    /// Report that unsupported platforms never have host attachment interfaces.
    async fn attachment_exists(&self, _attachment_id: Uuid) -> Result<bool> {
        Ok(false)
    }

    /// Ignore attachment creation on unsupported platforms while preserving the trait contract.
    async fn ensure_attachment(&self, _request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        Ok(())
    }

    /// Ignore attachment teardown on unsupported platforms.
    async fn teardown_attachment(&self, _attachment_id: Uuid) -> Result<()> {
        Ok(())
    }

    /// Pretend remote FDB entries are converged when no kernel VXLAN dataplane exists.
    async fn ensure_remote_fdb(&self, _vxlan_name: &str, _mac: &str, _dst: IpAddr) -> Result<bool> {
        Ok(true)
    }

    /// Ignore remote FDB removal on unsupported platforms.
    async fn remove_remote_fdb(&self, _vxlan_name: &str, _mac: &str, _dst: IpAddr) -> Result<()> {
        Ok(())
    }

    /// Pretend flood entries are converged when no kernel VXLAN dataplane exists.
    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<bool> {
        Ok(true)
    }

    /// Ignore flood-entry removal on unsupported platforms.
    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<()> {
        Ok(())
    }

    /// Return an empty FDB inventory on unsupported platforms.
    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, IpAddr)>> {
        Ok(Vec::new())
    }
}
