use super::{NetworkController, collect_orphaned_network_suffixes, is_managed_overlay_link_name};
use anyhow::Context;
use aya::{programs::ProgramError, sys::SyscallError};
use std::collections::HashSet;
use uuid::Uuid;

fn make_syscall_error(errno: i32) -> SyscallError {
    SyscallError {
        call: "bpf_link_create",
        io_error: std::io::Error::from_raw_os_error(errno),
    }
}

#[test]
fn detects_syscall_conflict_directly() {
    let err = Err::<(), _>(make_syscall_error(libc::EEXIST))
        .context("attach xdp")
        .unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected syscall conflict to be detected"
    );
}

#[test]
fn detects_syscall_conflict_wrapped_in_program_error() {
    let program_err: ProgramError = make_syscall_error(libc::EEXIST).into();
    let err = Err::<(), _>(program_err).context("attach xdp").unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected program error conflict to be detected"
    );
}

#[test]
fn detects_xdp_busy_conflict_directly() {
    let err = Err::<(), _>(make_syscall_error(libc::EBUSY))
        .context("attach xdp")
        .unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected xdp busy conflict to be detected"
    );
}

#[test]
fn detects_xdp_busy_conflict_wrapped_in_program_error() {
    let program_err: ProgramError = make_syscall_error(libc::EBUSY).into();
    let err = Err::<(), _>(program_err).context("attach xdp").unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected wrapped xdp busy conflict to be detected"
    );
}

#[test]
fn collects_only_orphaned_managed_network_suffixes() {
    let live =
        Uuid::parse_str("21523dac-bdaa-6cf5-359f-57139c6464a8").expect("valid live network id");
    let desired = HashSet::from([live]);
    let suffixes = collect_orphaned_network_suffixes(
        &desired,
        [
            "mnhost-21523dac",
            "mnhp-21523dac",
            "mvx-21523dac",
            "mnt-br-21523dac",
            "mnhost-b3d339cd",
            "mnt-br-b3d339cd",
            "mvx-b3d339cd",
            "mnhp-b3d339cd",
            "docker0",
            "mnhost-nothexzz",
        ],
    );

    assert_eq!(
        suffixes,
        vec!["b3d339cd".to_string()],
        "only managed suffixes that are absent from desired network ids should be collected"
    );
}

#[test]
fn identifies_all_managed_overlay_link_names() {
    assert!(is_managed_overlay_link_name("mvx-21523dac"));
    assert!(is_managed_overlay_link_name("mnt-br-21523dac"));
    assert!(is_managed_overlay_link_name("mnhost-21523dac"));
    assert!(is_managed_overlay_link_name("mnhp-21523dac"));
    assert!(is_managed_overlay_link_name("mnth-21523dac"));
    assert!(is_managed_overlay_link_name("mntc-21523dac"));
    assert!(!is_managed_overlay_link_name("eth0"));
    assert!(!is_managed_overlay_link_name("docker0"));
}
