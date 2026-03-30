pub use crate::workload::model::{
    WorkloadEnvironmentVariable as TaskEnvironmentVariable, WorkloadEvent as TaskEvent,
    WorkloadSecretFile as TaskSecretFile, WorkloadSecretReference as TaskSecretReference,
    WorkloadServiceMetadata as TaskServiceMetadata, WorkloadSpec as TaskSpec,
    WorkloadStateFilter as TaskStateFilter, WorkloadStateKind as TaskStateKind,
    WorkloadStatus as TaskStatus, WorkloadValue as TaskValue, WorkloadValueDraft as TaskValueDraft,
    WorkloadVolumeMount as TaskVolumeMount,
};
pub use crate::workload::types::{
    WorkloadLivenessProbe as TaskLivenessProbe, WorkloadLivenessProbeKind as TaskLivenessProbeKind,
    WorkloadRestartPolicy as TaskRestartPolicy, WorkloadRestartPolicyKind as TaskRestartPolicyKind,
};
