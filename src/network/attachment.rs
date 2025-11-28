use anyhow::Result;
use async_trait::async_trait;
use std::net::IpAddr;
use uuid::Uuid;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use linux::AttachmentProvisioner;

#[cfg(target_os = "linux")]
pub use linux::AttachmentProvisioner as PlatformAttachmentProvisioner;

#[cfg(not(target_os = "linux"))]
pub type PlatformAttachmentProvisioner = AttachmentProvisioner;

/// Parameters required to provision an attachment for a container network namespace.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct AttachmentProvisioningRequest<'a> {
    pub bridge_name: &'a str,
    pub mtu: u32,
    pub attachment_id: Uuid,
    pub container_pid: i32,
    pub assigned_ip: &'a str,
    pub prefix: u8,
    pub mac: &'a str,
}

#[async_trait]
pub trait AttachmentProvisionerApi: Send + Sync {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool>;

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()>;

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()>;

    #[allow(dead_code)]
    async fn ensure_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<bool>;

    #[allow(dead_code)]
    async fn remove_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<()>;

    #[allow(dead_code)]
    async fn ensure_flood_entry(&self, vxlan_name: &str, dst: IpAddr) -> Result<bool>;

    #[allow(dead_code)]
    async fn remove_flood_entry(&self, vxlan_name: &str, dst: IpAddr) -> Result<()>;
}

#[cfg(not(target_os = "linux"))]
#[derive(Clone, Default)]
pub struct AttachmentProvisioner;

#[cfg(not(target_os = "linux"))]
impl AttachmentProvisioner {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    pub fn unavailable() -> Self {
        Self
    }

    #[allow(dead_code)]
    pub async fn attachment_exists(&self, _attachment_id: Uuid) -> Result<bool> {
        Ok(false)
    }

    #[allow(dead_code)]
    pub async fn ensure_attachment(
        &self,
        _request: &AttachmentProvisioningRequest<'_>,
    ) -> Result<()> {
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn teardown_attachment(&self, _attachment_id: Uuid) -> Result<()> {
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    #[allow(dead_code)]
    pub async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<bool> {
        Ok(true)
    }

    #[allow(dead_code)]
    pub async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<()> {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
#[async_trait]
impl AttachmentProvisionerApi for AttachmentProvisioner {
    async fn attachment_exists(&self, _attachment_id: Uuid) -> Result<bool> {
        Ok(false)
    }

    async fn ensure_attachment(&self, _request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        Ok(())
    }

    async fn teardown_attachment(&self, _attachment_id: Uuid) -> Result<()> {
        Ok(())
    }

    async fn ensure_remote_fdb(&self, _vxlan_name: &str, _mac: &str, _dst: IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(&self, _vxlan_name: &str, _mac: &str, _dst: IpAddr) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn host_iface_name(attachment_id: Uuid) -> String {
    format!("mnth-{}", short_id(attachment_id))
}

#[cfg(target_os = "linux")]
pub(crate) fn container_iface_name(attachment_id: Uuid) -> String {
    format!("mntc-{}", short_id(attachment_id))
}

pub(crate) fn bridge_name(network_id: Uuid) -> String {
    format!("mnt-br-{}", short_id(network_id))
}

/// Compute the deterministic VXLAN device name for an overlay network so dataplane helpers can
/// target the correct interface.
pub(crate) fn vxlan_name(network_id: Uuid) -> String {
    format!("mvx-{}", short_id(network_id))
}

fn short_id(id: Uuid) -> String {
    let hex = id.simple().to_string();
    hex.chars().take(8).collect()
}
