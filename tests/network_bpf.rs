#![cfg(target_os = "linux")]

use std::path::PathBuf;

use anyhow::Result;
use mantissa::network::bpf::{NetworkBpfManager, NetworkInterfaceContext};
use mantissa::network::types::{BpfProgramSpec, NetworkDriver, NetworkSpecDraft, NetworkSpecValue};
use tokio::runtime::Runtime;
use uuid::Uuid;

struct EnvGuard {
    key: &'static str,
    value: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, val: impl Into<std::ffi::OsString>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, val.into());
        Self {
            key,
            value: previous,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.value {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn build_spec(program: &str) -> NetworkSpecValue {
    let draft = NetworkSpecDraft {
        name: format!("test-{program}"),
        description: String::new(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.200.0.0/24".to_string(),
        vni: 42,
        mtu: 1400,
        sealed: false,
        bpf_programs: vec![BpfProgramSpec::new(program)],
    };
    NetworkSpecValue::new(draft)
}

#[test]
fn ensure_and_teardown_with_stub_loader() -> Result<()> {
    let rt = Runtime::new()?;

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let artifact_dir = manifest_dir.join("target/bpf");
    assert!(
        artifact_dir.exists(),
        "bpf artifacts missing: {}",
        artifact_dir.display()
    );
    assert!(
        artifact_dir.join("vxlan_xdp.bpf.o").exists(),
        "expected compiled vxlan_xdp artifact"
    );

    let _dir_guard = EnvGuard::set("MANTISSA_BPF_DIR", artifact_dir.as_os_str());
    let _noop_guard = EnvGuard::set("MANTISSA_BPF_NO_ATTACH", "1");

    rt.block_on(async {
        let manager = NetworkBpfManager::new()?;
        let spec = build_spec("vxlan_xdp");
        let ctx = NetworkInterfaceContext::new(Uuid::new_v4(), "lo", "lo");

        manager.ensure_network(&spec, &ctx).await?;
        manager.teardown_network(&ctx).await?;
        Ok(())
    })
}
