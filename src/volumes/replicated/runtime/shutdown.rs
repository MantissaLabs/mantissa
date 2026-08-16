//! Ordered replicated-volume shutdown and bounded retry handling.

use super::{Context, Duration, FuturesUnordered, ReplicatedVolumeRuntime, Result, StreamExt};

impl ReplicatedVolumeRuntime {
    /// Stops local resources within one wall-clock budget without consuming retry state.
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_with_timeout(self.shutdown_timeout).await
    }

    /// Stops local resources within the caller's remaining daemon-shutdown budget.
    pub(crate) async fn shutdown_with_timeout(&self, timeout: Duration) -> Result<()> {
        self.begin_shutdown();
        tokio::time::timeout(timeout, self.shutdown_attempt())
            .await
            .context("replicated-volume shutdown attempt timed out")?
    }

    /// Advances every owned resource toward terminal shutdown in dependency order.
    pub(super) async fn shutdown_attempt(&self) -> Result<()> {
        let _lifecycle = self.driver_lifecycle.write().await;
        let attachments = self
            .replicas
            .discover_attachments(self.max_saved_replicas)?;
        let attachment_cleanup = async {
            let mut stopping = FuturesUnordered::new();
            for attachment in attachments {
                let key = attachment.key();
                stopping.push(async move { (key, self.quiesce_attachment_resources(key).await) });
            }
            let mut failed = 0_usize;
            let mut first_failure = None;
            while let Some((key, result)) = stopping.next().await {
                if let Err(error) = result {
                    failed += 1;
                    if first_failure.is_none() {
                        first_failure = Some((key, error));
                    }
                }
            }
            if let Some((key, error)) = first_failure {
                anyhow::bail!(
                    "{failed} replicated-volume attachment cleanup(s) remain pending; first \
                     failure for {key:?}: {error:#}"
                );
            }
            Ok(())
        };
        let (maintenance, attachments) = tokio::join!(self.stop_maintenance(), attachment_cleanup);
        match (maintenance, attachments) {
            (Ok(()), Ok(())) => {}
            (Err(maintenance), Ok(())) => return Err(maintenance),
            (Ok(()), Err(attachments)) => return Err(attachments),
            (Err(maintenance), Err(attachments)) => {
                return Err(anyhow::anyhow!(
                    "replica maintenance remains pending: {maintenance:#}; attachment cleanup \
                     remains pending: {attachments:#}"
                ));
            }
        }
        tokio::time::timeout(
            self.shutdown_timeout,
            self.data_connections.close_all_and_wait(),
        )
        .await
        .context("replica data connection shutdown timed out")?;
        self.lifecycle_calls.stop(self.shutdown_timeout).await?;
        self.replica_file_workers
            .stop(self.shutdown_timeout)
            .await?;
        self.groups.shutdown(self.shutdown_timeout).await?;
        tokio::time::timeout(self.shutdown_timeout, self.transport.shutdown())
            .await
            .context("replicated-volume transport shutdown timed out")??;
        Ok(())
    }
}
