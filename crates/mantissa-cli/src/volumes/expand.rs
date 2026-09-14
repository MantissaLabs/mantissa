use crate::output;
use crate::resources::format_bytes;
use anyhow::Result;
use mantissa_client::config::ClientConfig;

/// Saves one larger desired total and reports the current replicated capacity.
pub async fn expand(cfg: &ClientConfig, selector: &str, target_capacity_bytes: u64) -> Result<()> {
    let result = mantissa_client::volumes::expand(cfg, selector, target_capacity_bytes).await?;

    output::emit_line(expand_result_message(selector, result));
    output::emit_line(format!(
        "Use `mantissa volumes inspect {selector}` to follow progress."
    ));
    Ok(())
}

/// Describes whether the durable desired capacity changed or was already current.
fn expand_result_message(
    selector: &str,
    result: mantissa_client::volumes::VolumeExpandResult,
) -> String {
    let replicated = format_bytes(result.replicated_capacity_bytes);
    let desired = format_bytes(result.desired_capacity_bytes);
    match (
        result.desired_capacity_changed,
        result.desired_capacity_bytes > result.replicated_capacity_bytes,
    ) {
        (true, true) => {
            format!("Requested expansion for volume '{selector}': {replicated} -> {desired}.")
        }
        (false, true) => format!(
            "Volume '{selector}' already has an expansion request from {replicated} to {desired}."
        ),
        (true, false) => format!(
            "Cancelled the pending expansion for volume '{selector}'; desired capacity is {desired}."
        ),
        (false, false) => format!(
            "Volume '{selector}' already has the requested capacity {desired}; no expansion is pending."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantissa_client::volumes::VolumeExpandResult;
    use uuid::Uuid;

    /// Builds one accepted result for concise output-branch tests.
    fn result(
        replicated_capacity_bytes: u64,
        desired_capacity_bytes: u64,
        desired_capacity_changed: bool,
    ) -> VolumeExpandResult {
        VolumeExpandResult {
            volume_id: Uuid::from_u128(1),
            initial_capacity_bytes: 64 << 20,
            desired_capacity_bytes,
            replicated_capacity_bytes,
            desired_capacity_changed,
        }
    }

    /// Output distinguishes a changed request, an idempotent retry, and cancellation.
    #[test]
    fn expansion_result_names_the_durable_outcome() {
        assert!(
            expand_result_message("data", result(64 << 20, 128 << 20, true))
                .starts_with("Requested expansion")
        );
        assert!(
            expand_result_message("data", result(64 << 20, 128 << 20, false))
                .contains("already has an expansion request")
        );
        assert!(
            expand_result_message("data", result(64 << 20, 64 << 20, true))
                .starts_with("Cancelled the pending expansion")
        );
        assert!(
            expand_result_message("data", result(64 << 20, 64 << 20, false))
                .contains("no expansion is pending")
        );
    }
}
