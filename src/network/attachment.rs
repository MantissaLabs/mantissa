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

    #[allow(dead_code)]
    async fn list_remote_fdb(&self, vxlan_name: &str) -> Result<Vec<(String, IpAddr)>>;
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

    #[allow(dead_code)]
    pub async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, IpAddr)>> {
        Ok(Vec::new())
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

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, IpAddr)>> {
        Ok(Vec::new())
    }
}
