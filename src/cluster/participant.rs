use crate::cluster::transition::ClusterTransition;
use async_trait::async_trait;

/// Structured participant report emitted after one transition hook runs.
#[derive(Clone, Debug)]
pub struct ClusterParticipantReport {
    pub name: &'static str,
    pub details: Vec<(String, String)>,
}

impl ClusterParticipantReport {
    /// Creates an empty report for the provided participant name.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            details: Vec::new(),
        }
    }

    /// Appends one key/value detail to the report for logging and diagnostics.
    pub fn add_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.push((key.into(), value.into()));
        self
    }

    /// Renders report details into one compact string suitable for tracing fields.
    pub fn render(&self) -> String {
        if self.details.is_empty() {
            return "noop".to_string();
        }
        self.details
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Domain hook invoked by the cluster transition coordinator during operation commit.
#[async_trait(?Send)]
pub trait ClusterTransitionParticipant {
    /// Returns the stable participant identifier for tracing.
    fn name(&self) -> &'static str;

    /// Applies commit-time side effects for one transition.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error>;
}
