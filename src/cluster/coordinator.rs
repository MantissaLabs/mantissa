use crate::cluster::participant::{ClusterParticipantReport, ClusterTransitionParticipant};
use crate::cluster::transition::ClusterTransition;

/// Runs transition participants in a deterministic order for one commit.
pub struct ClusterTransitionCoordinator {
    participants: Vec<Box<dyn ClusterTransitionParticipant>>,
}

impl ClusterTransitionCoordinator {
    /// Creates a coordinator with participants registered in execution order.
    pub fn new(participants: Vec<Box<dyn ClusterTransitionParticipant>>) -> Self {
        Self { participants }
    }

    /// Executes commit hooks for all participants and returns one report per participant.
    pub async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<Vec<ClusterParticipantReport>, capnp::Error> {
        let mut reports = Vec::with_capacity(self.participants.len());
        for participant in &self.participants {
            let report = participant.on_commit(transition).await?;
            reports.push(report);
        }
        Ok(reports)
    }
}
