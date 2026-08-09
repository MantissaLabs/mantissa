use super::{
    FilesystemOwnership, VolumeBindingMode, VolumeLabel, VolumeSpec, parse_volume_labels,
    resolve_node_selector,
};
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};

/// Storage driver selected for a newly created managed volume.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VolumeCreateDriver {
    #[default]
    Local,
    Replicated,
}

/// Data required to create one Mantissa-managed volume object.
#[derive(Debug, Clone)]
pub struct VolumeCreateRequest {
    pub name: String,
    pub driver: VolumeCreateDriver,
    pub ownership: FilesystemOwnership,
    pub binding_mode: VolumeBindingMode,
    pub reclaim_policy: super::VolumeReclaimPolicy,
    pub requested_bytes: Option<u64>,
    pub labels: Vec<VolumeLabel>,
    pub node_selector: Option<String>,
}

impl VolumeCreateRequest {
    /// Checks driver, binding, node, and capacity rules before sending the request.
    pub fn validate(&self) -> Result<()> {
        match self.driver {
            VolumeCreateDriver::Local
                if matches!(self.binding_mode, VolumeBindingMode::Immediate)
                    && self.node_selector.is_none() =>
            {
                Err(anyhow!("immediate local volumes require --node"))
            }
            VolumeCreateDriver::Replicated
                if !matches!(self.binding_mode, VolumeBindingMode::WaitForFirstConsumer) =>
            {
                Err(anyhow!(
                    "replicated volumes require wait_for_first_consumer binding"
                ))
            }
            VolumeCreateDriver::Replicated if self.node_selector.is_some() => Err(anyhow!(
                "replicated volumes choose their nodes when the first workload is placed"
            )),
            VolumeCreateDriver::Replicated if self.requested_bytes.is_none() => {
                Err(anyhow!("replicated volumes require a capacity"))
            }
            VolumeCreateDriver::Replicated if self.requested_bytes == Some(0) => Err(anyhow!(
                "replicated volume capacity must be greater than zero"
            )),
            VolumeCreateDriver::Replicated
                if self.requested_bytes.is_some_and(|bytes| bytes % 4096 != 0) =>
            {
                Err(anyhow!(
                    "replicated volume capacity must be a multiple of 4096 bytes"
                ))
            }
            _ => Ok(()),
        }
    }
}

/// Submits one managed volume create request and returns the persisted spec.
pub async fn create(
    cfg: &ClientConfig,
    request: VolumeCreateRequest,
    labels: &[String],
) -> Result<VolumeSpec> {
    let request = VolumeCreateRequest {
        labels: parse_volume_labels(labels)?,
        ..request
    };
    request.validate()?;
    create_with_request(cfg, &request).await
}

/// Submits one managed volume request that already contains normalized labels.
pub async fn create_with_request(
    cfg: &ClientConfig,
    request: &VolumeCreateRequest,
) -> Result<VolumeSpec> {
    request.validate()?;
    let session = connection::get_local_session(cfg).await?;
    let volumes_cap = session.get_volumes_request();
    let volumes = volumes_cap.send().pipeline.get_volumes();
    let mut create = volumes.create_request();

    let bound_node = if let Some(selector) = &request.node_selector {
        Some(resolve_node_selector(cfg, selector).await?)
    } else {
        None
    };

    {
        let mut inner = create.get().init_request();
        inner.set_name(&request.name);
        let driver = inner.reborrow().init_driver();
        let mut ownership = match request.driver {
            VolumeCreateDriver::Local => driver.init_local().init_managed().init_ownership(),
            VolumeCreateDriver::Replicated => driver.init_replicated().init_ownership(),
        };
        match &request.ownership {
            FilesystemOwnership::Daemon => ownership.set_daemon(()),
            FilesystemOwnership::User { uid, gid } => {
                let mut user = ownership.init_user();
                user.set_uid(*uid);
                user.set_gid(*gid);
            }
            FilesystemOwnership::FsGroup { gid } => {
                ownership.init_fs_group().set_gid(*gid);
            }
        }
        inner.set_access_mode(mantissa_protocol::volumes::VolumeAccessMode::ReadWriteOnce);
        inner.set_binding_mode(match request.binding_mode {
            VolumeBindingMode::Immediate => {
                mantissa_protocol::volumes::VolumeBindingMode::Immediate
            }
            VolumeBindingMode::WaitForFirstConsumer => {
                mantissa_protocol::volumes::VolumeBindingMode::WaitForFirstConsumer
            }
        });
        inner.set_reclaim_policy(match request.reclaim_policy {
            super::VolumeReclaimPolicy::Retain => {
                mantissa_protocol::volumes::VolumeReclaimPolicy::Retain
            }
            super::VolumeReclaimPolicy::Delete => {
                mantissa_protocol::volumes::VolumeReclaimPolicy::Delete
            }
        });
        inner.set_requested_bytes(request.requested_bytes.unwrap_or(0));
        let mut labels = inner.reborrow().init_labels(request.labels.len() as u32);
        for (idx, label) in request.labels.iter().enumerate() {
            let mut entry = labels.reborrow().get(idx as u32);
            entry.set_key(&label.key);
            entry.set_value(&label.value);
        }
        if let Some((node_id, _node_name)) = &bound_node {
            inner.set_bound_node_id(node_id.as_bytes());
        } else {
            inner.set_bound_node_id(&[]);
        }
    }

    let response = create
        .send()
        .promise
        .await
        .context("volume create request failed")?;
    let reader = response.get()?.get_volume()?;
    VolumeSpec::from_reader(reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds the smallest valid replicated-volume request for validation tests.
    fn replicated_request() -> VolumeCreateRequest {
        VolumeCreateRequest {
            name: "data".to_string(),
            driver: VolumeCreateDriver::Replicated,
            ownership: FilesystemOwnership::Daemon,
            binding_mode: VolumeBindingMode::WaitForFirstConsumer,
            reclaim_policy: super::super::VolumeReclaimPolicy::Retain,
            requested_bytes: Some(64 * 1024 * 1024),
            labels: Vec::new(),
            node_selector: None,
        }
    }

    /// Checks that replicated requests cannot omit their capacity or binding rule.
    #[test]
    fn replicated_create_requires_capacity_and_deferred_binding() {
        let valid = replicated_request();
        assert!(valid.validate().is_ok());

        let mut missing_capacity = valid.clone();
        missing_capacity.requested_bytes = None;
        assert!(
            missing_capacity
                .validate()
                .expect_err("missing capacity")
                .to_string()
                .contains("require a capacity")
        );

        let mut immediate = valid;
        immediate.binding_mode = VolumeBindingMode::Immediate;
        assert!(
            immediate
                .validate()
                .expect_err("immediate replicated volume")
                .to_string()
                .contains("wait_for_first_consumer")
        );
    }

    /// Checks that replicated requests cannot choose a node or partial block.
    #[test]
    fn replicated_create_rejects_unaligned_capacity_and_fixed_node() {
        let mut unaligned = replicated_request();
        unaligned.requested_bytes = Some(64 * 1024 * 1024 + 1);
        assert!(
            unaligned
                .validate()
                .expect_err("unaligned capacity")
                .to_string()
                .contains("multiple of 4096")
        );

        let mut fixed_node = replicated_request();
        fixed_node.node_selector = Some("storage-1".to_string());
        assert!(
            fixed_node
                .validate()
                .expect_err("fixed replicated node")
                .to_string()
                .contains("choose their nodes")
        );
    }
}
