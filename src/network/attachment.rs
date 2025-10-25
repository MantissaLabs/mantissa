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

#[async_trait]
pub trait AttachmentProvisionerApi: Send + Sync {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool>;

    async fn ensure_attachment(
        &self,
        network_id: Uuid,
        bridge_name: &str,
        mtu: u32,
        attachment_id: Uuid,
        container_pid: i32,
        assigned_ip: &str,
        prefix: u8,
        mac: &str,
    ) -> Result<()>;

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()>;

    async fn ensure_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<bool>;

    async fn remove_remote_fdb(&self, vxlan_name: &str, mac: &str, dst: IpAddr) -> Result<()>;

    async fn ensure_flood_entry(&self, vxlan_name: &str, dst: IpAddr) -> Result<bool>;

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

    pub async fn attachment_exists(&self, _attachment_id: Uuid) -> Result<bool> {
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_attachment(
        &self,
        _network_id: Uuid,
        _bridge_name: &str,
        _mtu: u32,
        _attachment_id: Uuid,
        _container_pid: i32,
        _assigned_ip: &str,
        _prefix: u8,
        _mac: &str,
    ) -> Result<()> {
        Ok(())
    }

    pub async fn teardown_attachment(&self, _attachment_id: Uuid) -> Result<()> {
        Ok(())
    }

    pub async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    pub async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    pub async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: IpAddr) -> Result<bool> {
        Ok(true)
    }

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

    async fn ensure_attachment(
        &self,
        _network_id: Uuid,
        _bridge_name: &str,
        _mtu: u32,
        _attachment_id: Uuid,
        _container_pid: i32,
        _assigned_ip: &str,
        _prefix: u8,
        _mac: &str,
    ) -> Result<()> {
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

fn short_id(id: Uuid) -> String {
    let hex = id.simple().to_string();
    hex.chars().take(8).collect()
}
