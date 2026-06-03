use uuid::Uuid;

use crate::registry::Registry;

use super::Scheduler;
use super::summary::SchedulerSummary;

impl Scheduler {
    /// Fetches one remote scheduler summary using an already-resolved peer session handle.
    async fn fetch_remote_summary_via_handle(
        registry: &Registry,
        client: &mantissa_protocol::server::Client,
        peer_id: Uuid,
        include_details: bool,
    ) -> Result<SchedulerSummary, capnp::Error> {
        let session = registry
            .scheduler_session_via_handle(client, peer_id)
            .await
            .ok_or_else(|| {
                capnp::Error::failed(format!(
                    "unable to open scheduler session with peer {peer_id}"
                ))
            })?;

        let scheduler_client = session
            .get_scheduler_request()
            .send()
            .promise
            .await?
            .get()?
            .get_scheduler()?;

        let mut summary_req = scheduler_client.summary_request();
        {
            let mut inner = summary_req.get().init_request();
            inner.set_peer_id(&[]);
            inner.set_include_details(include_details);
        }

        let response = summary_req.send().promise.await?;
        let reader = response.get()?.get_summary()?;

        SchedulerSummary::from_reader(reader)
    }

    /// Fetches one scheduler summary from a remote peer, refreshing stale handles once.
    pub async fn fetch_remote_summary(
        &self,
        peer_id: Uuid,
        include_details: bool,
    ) -> Result<SchedulerSummary, capnp::Error> {
        let self_id = self.store_key.to_uuid();

        if peer_id == self_id {
            return Err(capnp::Error::failed(
                "peer id references local node for scheduler summary".into(),
            ));
        }

        let mut client = match self.registry.server_handle_for(peer_id).await {
            Some(handle) => handle,
            None => self
                .registry
                .refresh_peer_handle(peer_id)
                .await
                .ok_or_else(|| {
                    capnp::Error::failed(format!("no handle available for peer {peer_id}"))
                })?,
        };

        for attempt in 0..=1 {
            match Self::fetch_remote_summary_via_handle(
                &self.registry,
                &client,
                peer_id,
                include_details,
            )
            .await
            {
                Ok(summary) => return Ok(summary),
                Err(err) => {
                    if attempt == 1 {
                        return Err(err);
                    }

                    client = match self.registry.refresh_peer_handle(peer_id).await {
                        Some(new_client) => new_client,
                        None => return Err(err),
                    };
                }
            }
        }

        Err(capnp::Error::failed(
            "scheduler summary retry loop ended without a result".into(),
        ))
    }
}
