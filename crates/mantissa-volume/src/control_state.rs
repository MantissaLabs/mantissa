//! Bounded Raft control state for one replicated-volume generation.
//!
//! This module deliberately contains only facts that require consensus. Local
//! devices, mounts, filesystem work, copy progress, and cleanup remain outside
//! the control state and are rediscovered by reconcilers.

use std::collections::BTreeSet;

use mantissa_raft::{ApplicationCommand, ApplicationResponse};
use thiserror::Error;

use crate::{
    DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeDescriptor, VolumeGeneration,
    VolumeNodeId,
};

#[cfg(test)]
mod tests;

/// Whether one retained-capable volume generation may receive writer grants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VolumeDisposition {
    /// The generation may receive writer grants.
    #[default]
    Live,

    /// Data is preserved, but writer grants are forbidden until restoration.
    Retained,
}

/// The only session allowed to issue foreground data operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterGrant {
    /// Node that owns the writer session.
    pub node_id: VolumeNodeId,

    /// Locally durable driver creation attempt.
    pub session_id: DriverSessionId,
}

/// Authorization to align selected copies from one canonical survivor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryGrant {
    /// Stable identity retained only while this recovery is authorized.
    pub id: RecoveryId,

    /// Node responsible for performing the recovery copy.
    pub coordinator_node_id: VolumeNodeId,

    /// Existing active copy selected as the canonical data image.
    pub source_node_id: VolumeNodeId,

    /// Two or three active voters that must be aligned before serving.
    pub target_node_ids: BTreeSet<VolumeNodeId>,
}

/// Authorization for one learner to replace or restore one replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplacementGrant {
    /// Stable identity retained only while this replacement is authorized.
    pub id: ReplacementId,

    /// Node responsible for coordinating copy and membership observations.
    pub coordinator_node_id: VolumeNodeId,

    /// Voter being replaced, or none when restoring an existing third voter.
    pub old_node_id: Option<VolumeNodeId>,

    /// Inactive target that may become an active data copy after adoption.
    pub new_node_id: VolumeNodeId,

    /// Active copy from which the target data image is built.
    pub source_node_id: VolumeNodeId,
}

/// Data-plane facts published to every local request admission gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataControlState {
    /// Monotonic value that rejects every older data operation.
    pub fence: FenceEpoch,

    /// Two or three copies required to acknowledge foreground durability.
    pub copies: BTreeSet<VolumeNodeId>,

    /// Exact foreground writer, or none while fenced.
    pub writer: Option<WriterGrant>,

    /// Exact recovery authorization, or none during ordinary operation.
    pub recovery: Option<RecoveryGrant>,
}

/// Complete bounded application state stored by one volume Raft group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeControlState {
    descriptor: Option<VolumeDescriptor>,
    revision: u64,
    disposition: VolumeDisposition,
    data: Option<DataControlState>,
    replacement: Option<ReplacementGrant>,
}

impl Default for VolumeControlState {
    /// Creates the only valid state before the initialization command applies.
    fn default() -> Self {
        Self {
            descriptor: None,
            revision: 0,
            disposition: VolumeDisposition::Live,
            data: None,
            replacement: None,
        }
    }
}

impl VolumeControlState {
    /// Reconstructs one decoded snapshot only after all invariants validate.
    pub(crate) fn from_parts(
        descriptor: Option<VolumeDescriptor>,
        revision: u64,
        disposition: VolumeDisposition,
        data: Option<DataControlState>,
        replacement: Option<ReplacementGrant>,
    ) -> Result<Self, VolumeControlStateInvariantError> {
        let state = Self {
            descriptor,
            revision,
            disposition,
            data,
            replacement,
        };
        state.validate()?;
        Ok(state)
    }

    /// Returns the immutable descriptor after initialization.
    #[must_use]
    pub const fn descriptor(&self) -> Option<&VolumeDescriptor> {
        self.descriptor.as_ref()
    }

    /// Returns the revision used by compare-and-set volume commands.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns whether the generation is live or retained.
    #[must_use]
    pub const fn disposition(&self) -> VolumeDisposition {
        self.disposition
    }

    /// Returns current data control state after initialization.
    #[must_use]
    pub const fn data(&self) -> Option<&DataControlState> {
        self.data.as_ref()
    }

    /// Returns the one authorized replica replacement, when present.
    #[must_use]
    pub const fn replacement(&self) -> Option<ReplacementGrant> {
        self.replacement
    }

    /// Validates every relationship that must hold in a persisted snapshot.
    pub fn validate(&self) -> Result<(), VolumeControlStateInvariantError> {
        match (&self.descriptor, &self.data) {
            (None, None) => {
                if self.revision != 0
                    || self.disposition != VolumeDisposition::Live
                    || self.replacement.is_some()
                {
                    return Err(VolumeControlStateInvariantError::InvalidUninitializedState);
                }
                return Ok(());
            }
            (Some(_), Some(_)) => {}
            _ => return Err(VolumeControlStateInvariantError::DescriptorDataMismatch),
        }

        if self.revision == 0 {
            return Err(VolumeControlStateInvariantError::ZeroInitializedRevision);
        }
        let data = self
            .data
            .as_ref()
            .ok_or(VolumeControlStateInvariantError::DescriptorDataMismatch)?;
        if !(2..=3).contains(&data.copies.len()) {
            return Err(VolumeControlStateInvariantError::InvalidCopyCount);
        }
        if data
            .writer
            .is_some_and(|writer| !data.copies.contains(&writer.node_id))
        {
            return Err(VolumeControlStateInvariantError::WriterOutsideCopySet);
        }
        if let Some(recovery) = data.recovery.as_ref()
            && (data.writer.is_some()
                || self.replacement.is_some()
                || recovery.coordinator_node_id != recovery.source_node_id
                || recovery.target_node_ids != data.copies
                || !recovery.target_node_ids.contains(&recovery.source_node_id))
        {
            return Err(VolumeControlStateInvariantError::InvalidRecovery);
        }
        if let Some(replacement) = self.replacement
            && (replacement.coordinator_node_id != replacement.source_node_id
                || !data.copies.contains(&replacement.source_node_id)
                || data
                    .writer
                    .is_some_and(|writer| writer.node_id != replacement.source_node_id)
                || data.copies.contains(&replacement.new_node_id)
                || replacement.old_node_id == Some(replacement.new_node_id)
                || replacement.old_node_id == Some(replacement.source_node_id)
                || replacement.new_node_id == replacement.source_node_id
                || replacement_copy_set(&data.copies, replacement).len() != 3)
        {
            return Err(VolumeControlStateInvariantError::InvalidReplacement);
        }
        if self.disposition != VolumeDisposition::Live
            && (data.writer.is_some() || data.recovery.is_some() || self.replacement.is_some())
        {
            return Err(VolumeControlStateInvariantError::GrantOnInactiveVolume);
        }
        Ok(())
    }

    /// Evaluates one committed semantic command without storing retry history.
    #[must_use]
    pub fn evaluate(&self, command: &VolumeCommand) -> CommandPlan {
        match command {
            VolumeCommand::Initialize(command) => self.initialize(command),
            VolumeCommand::SetDisposition(command) => self.set_disposition(*command),
            VolumeCommand::GrantWriter(command) => self.grant_writer(*command),
            VolumeCommand::FenceWriter(command) => self.fence_writer(*command),
            VolumeCommand::BeginRecovery(command) => self.begin_recovery(command),
            VolumeCommand::RevokeRecovery(command) => self.revoke_recovery(*command),
            VolumeCommand::BeginReplacement(command) => self.begin_replacement(*command),
            VolumeCommand::CancelReplacement(command) => self.cancel_replacement(*command),
            VolumeCommand::AdoptReplacement(command) => self.adopt_replacement(command),
        }
    }

    /// Applies the initialization postcondition or rejects a conflicting group.
    fn initialize(&self, command: &InitializeVolume) -> CommandPlan {
        if self.initialization_is_current(command) {
            return self.current();
        }
        if self.descriptor.is_some() {
            return self.rejected(VolumeCommandRejection::AlreadyInitialized);
        }
        if command.initial_copies.len() != 3 {
            return self.rejected(VolumeCommandRejection::InvalidCopySet);
        }
        let next = Self {
            descriptor: Some(command.descriptor.clone()),
            revision: 1,
            disposition: VolumeDisposition::Live,
            data: Some(DataControlState {
                fence: FenceEpoch::initial(),
                copies: command.initial_copies.clone(),
                writer: None,
                recovery: None,
            }),
            replacement: None,
        };
        CommandPlan::applied(next)
    }

    /// Changes the generation disposition without waiting for local cleanup.
    fn set_disposition(&self, command: SetVolumeDisposition) -> CommandPlan {
        if self.disposition == command.disposition
            && self.expected_generation_is_current(command.expected)
        {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        let Some(revision) = self.revision.checked_add(1) else {
            return self.rejected(VolumeCommandRejection::RevisionExhausted);
        };
        let mut next = self.clone();
        next.revision = revision;
        next.disposition = command.disposition;
        if command.disposition != VolumeDisposition::Live {
            let Some(fence) = next_fence(&next) else {
                return self.rejected(VolumeCommandRejection::FenceExhausted);
            };
            let data = next.data_mut();
            data.fence = fence;
            data.writer = None;
            data.recovery = None;
            next.replacement = None;
        }
        CommandPlan::applied(next)
    }

    /// Grants one exact saved driver session after all recovery is complete.
    fn grant_writer(&self, command: GrantVolumeWriter) -> CommandPlan {
        if self.expected_generation_is_current(command.expected)
            && self
                .data
                .as_ref()
                .is_some_and(|data| data.writer == Some(command.writer) && data.recovery.is_none())
            && self.disposition == VolumeDisposition::Live
        {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        if self.disposition != VolumeDisposition::Live {
            return self.rejected(VolumeCommandRejection::VolumeNotLive);
        }
        if self.replacement.is_some() {
            return self.rejected(VolumeCommandRejection::ReplacementInProgress);
        }
        let data = self.data_ref();
        if !data.copies.contains(&command.writer.node_id) {
            return self.rejected(VolumeCommandRejection::WriterOutsideCopySet);
        }
        if data.recovery.is_some() {
            return self.rejected(VolumeCommandRejection::RecoveryInProgress);
        }
        let Some((revision, fence)) = self.next_revision_and_fence() else {
            return self.exhausted();
        };
        let mut next = self.clone();
        next.revision = revision;
        let data = next.data_mut();
        data.fence = fence;
        data.writer = Some(command.writer);
        data.recovery = None;
        CommandPlan::applied(next)
    }

    /// Revokes the exact current writer and immediately advances its fence.
    fn fence_writer(&self, command: FenceVolumeWriter) -> CommandPlan {
        if self.expected_generation_is_current(command.expected)
            && self.data.as_ref().is_some_and(|data| data.writer.is_none())
        {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        if self.data_ref().writer != Some(command.writer) {
            return self.rejected(VolumeCommandRejection::WrongWriter);
        }
        let Some((revision, fence)) = self.next_revision_and_fence() else {
            return self.exhausted();
        };
        let mut next = self.clone();
        next.revision = revision;
        let data = next.data_mut();
        data.fence = fence;
        data.writer = None;
        CommandPlan::applied(next)
    }

    /// Fences foreground work and commits one canonical recovery selection.
    fn begin_recovery(&self, command: &BeginVolumeRecovery) -> CommandPlan {
        if self.expected_generation_is_current(command.expected)
            && self.data.as_ref().is_some_and(|data| {
                data.writer.is_none()
                    && data.recovery.as_ref() == Some(&command.recovery)
                    && data.copies == command.recovery.target_node_ids
            })
            && self.replacement.is_none()
        {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        if self.disposition != VolumeDisposition::Live {
            return self.rejected(VolumeCommandRejection::VolumeNotLive);
        }
        if self.replacement.is_some() {
            return self.rejected(VolumeCommandRejection::ReplacementInProgress);
        }
        let data = self.data_ref();
        if data.writer != command.expected_writer {
            return self.rejected(VolumeCommandRejection::WrongWriter);
        }
        match (&data.recovery, command.replaced_recovery_id) {
            (None, None) => {}
            (Some(current), _) if current.id == command.recovery.id => {
                return self.rejected(VolumeCommandRejection::RecoveryIdConflict);
            }
            (Some(current), Some(id)) if current.id == id => {}
            (Some(_), _) => return self.rejected(VolumeCommandRejection::RecoveryInProgress),
            (None, Some(_)) => return self.rejected(VolumeCommandRejection::NoRecovery),
        }
        if !(2..=3).contains(&command.recovery.target_node_ids.len())
            || command.recovery.coordinator_node_id != command.recovery.source_node_id
            || !command
                .recovery
                .target_node_ids
                .contains(&command.recovery.source_node_id)
            || !data.copies.contains(&command.recovery.source_node_id)
            || !command.recovery.target_node_ids.is_subset(&data.copies)
        {
            return self.rejected(VolumeCommandRejection::InvalidRecovery);
        }
        let Some((revision, fence)) = self.next_revision_and_fence() else {
            return self.exhausted();
        };
        let mut next = self.clone();
        next.revision = revision;
        let data = next.data_mut();
        data.fence = fence;
        data.copies.clone_from(&command.recovery.target_node_ids);
        data.writer = None;
        data.recovery = Some(command.recovery.clone());
        next.replacement = None;
        CommandPlan::applied(next)
    }

    /// Revokes one completed recovery grant without manufacturing a writer.
    fn revoke_recovery(&self, command: RevokeVolumeRecovery) -> CommandPlan {
        if self.expected_generation_is_current(command.expected)
            && self
                .data
                .as_ref()
                .is_some_and(|data| data.recovery.is_none())
        {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        if self.disposition != VolumeDisposition::Live {
            return self.rejected(VolumeCommandRejection::VolumeNotLive);
        }
        match self.data_ref().recovery.as_ref() {
            Some(recovery) if recovery.id == command.recovery_id => {}
            Some(_) => return self.rejected(VolumeCommandRejection::WrongRecovery),
            None => return self.rejected(VolumeCommandRejection::NoRecovery),
        }
        let Some(revision) = self.revision.checked_add(1) else {
            return self.rejected(VolumeCommandRejection::RevisionExhausted);
        };
        let mut next = self.clone();
        next.revision = revision;
        next.data_mut().recovery = None;
        CommandPlan::applied(next)
    }

    /// Commits one bounded learner and data-copy replacement authorization.
    fn begin_replacement(&self, command: BeginReplicaReplacement) -> CommandPlan {
        if self.expected_generation_is_current(command.expected)
            && self.replacement == Some(command.replacement)
        {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        if self.disposition != VolumeDisposition::Live {
            return self.rejected(VolumeCommandRejection::VolumeNotLive);
        }
        let data = self.data_ref();
        if data.recovery.is_some() {
            return self.rejected(VolumeCommandRejection::RecoveryInProgress);
        }
        if let Some(current) = self.replacement {
            return self.rejected(if current.id == command.replacement.id {
                VolumeCommandRejection::ReplacementIdConflict
            } else {
                VolumeCommandRejection::ReplacementInProgress
            });
        }
        let replacement = command.replacement;
        if replacement.coordinator_node_id != replacement.source_node_id
            || !data.copies.contains(&replacement.source_node_id)
            || data
                .writer
                .is_some_and(|writer| writer.node_id != replacement.source_node_id)
            || data.copies.contains(&replacement.new_node_id)
            || replacement.new_node_id == replacement.source_node_id
            || replacement.old_node_id == Some(replacement.new_node_id)
            || replacement.old_node_id == Some(replacement.source_node_id)
            || replacement_copy_set(&data.copies, replacement).len() != 3
        {
            return self.rejected(VolumeCommandRejection::InvalidReplacement);
        }
        let Some(revision) = self.revision.checked_add(1) else {
            return self.rejected(VolumeCommandRejection::RevisionExhausted);
        };
        let mut next = self.clone();
        next.revision = revision;
        next.replacement = Some(replacement);
        CommandPlan::applied(next)
    }

    /// Cancels one exact replacement without retaining its local progress.
    fn cancel_replacement(&self, command: CancelReplicaReplacement) -> CommandPlan {
        if self.replacement.is_none() && self.expected_generation_is_current(command.expected) {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        let Some(replacement) = self.replacement else {
            return self.current();
        };
        if replacement.id != command.replacement_id {
            return self.rejected(VolumeCommandRejection::WrongReplacement);
        }
        let Some(revision) = self.revision.checked_add(1) else {
            return self.rejected(VolumeCommandRejection::RevisionExhausted);
        };
        let mut next = self.clone();
        next.revision = revision;
        next.replacement = None;
        CommandPlan::applied(next)
    }

    /// Adopts one rebuilt copy after membership and data checks complete.
    fn adopt_replacement(&self, command: &AdoptReplicaReplacement) -> CommandPlan {
        if self.expected_generation_is_current(command.expected)
            && self.disposition == VolumeDisposition::Live
            && self.replacement.is_none()
            && self.data.as_ref().is_some_and(|data| {
                data.copies == command.new_copies && data.writer == command.expected_writer
            })
        {
            return self.current();
        }
        if let Some(plan) = self.check_expected(command.expected) {
            return plan;
        }
        if self.disposition != VolumeDisposition::Live {
            return self.rejected(VolumeCommandRejection::VolumeNotLive);
        }
        let Some(replacement) = self.replacement else {
            return self.rejected(VolumeCommandRejection::NoReplacement);
        };
        if replacement.id != command.replacement_id {
            return self.rejected(VolumeCommandRejection::WrongReplacement);
        }
        let data = self.data_ref();
        if data.writer != command.expected_writer {
            return self.rejected(VolumeCommandRejection::WrongWriter);
        }
        if !valid_adoption(&data.copies, replacement, &command.new_copies) {
            return self.rejected(VolumeCommandRejection::InvalidReplacement);
        }
        if command
            .expected_writer
            .is_some_and(|writer| !command.new_copies.contains(&writer.node_id))
        {
            return self.rejected(VolumeCommandRejection::InvalidReplacement);
        }
        let Some((revision, fence)) = self.next_revision_and_fence() else {
            return self.exhausted();
        };
        let mut next = self.clone();
        next.revision = revision;
        let data = next.data_mut();
        data.fence = fence;
        data.copies.clone_from(&command.new_copies);
        next.replacement = None;
        CommandPlan::applied(next)
    }

    /// Checks compare-and-set identity after exact-postcondition handling.
    fn check_expected(&self, expected: ExpectedVolumeRevision) -> Option<CommandPlan> {
        let Some(descriptor) = self.descriptor.as_ref() else {
            return Some(self.rejected(VolumeCommandRejection::NotInitialized));
        };
        if descriptor.generation() != expected.generation {
            return Some(self.rejected(VolumeCommandRejection::WrongGeneration));
        }
        if self.revision != expected.revision {
            return Some(CommandPlan {
                state: self.clone(),
                response: VolumeCommandResponse::Conflict {
                    current_revision: self.revision,
                },
            });
        }
        None
    }

    /// Checks only generation identity for exact postcondition recognition.
    fn expected_generation_is_current(&self, expected: ExpectedVolumeRevision) -> bool {
        self.descriptor
            .as_ref()
            .is_some_and(|descriptor| descriptor.generation() == expected.generation)
    }

    /// Checks whether a pristine initialized state exactly matches a retry.
    fn initialization_is_current(&self, command: &InitializeVolume) -> bool {
        self.revision == 1
            && self.disposition == VolumeDisposition::Live
            && self.descriptor.as_ref() == Some(&command.descriptor)
            && self.replacement.is_none()
            && self.data.as_ref().is_some_and(|data| {
                data.fence == FenceEpoch::initial()
                    && data.copies == command.initial_copies
                    && data.writer.is_none()
                    && data.recovery.is_none()
            })
    }

    /// Returns current data control state after callers establish initialization.
    fn data_ref(&self) -> &DataControlState {
        // Every caller first checks the expected volume revision, which rejects the only
        // valid state without data. Keeping this invariant local avoids making
        // an impossible branch look recoverable to transition code.
        match self.data.as_ref() {
            Some(data) => data,
            None => unreachable!("checked initialized control state has data"),
        }
    }

    /// Returns mutable data control state after callers establish initialization.
    fn data_mut(&mut self) -> &mut DataControlState {
        // Transition construction begins from a checked initialized state.
        match self.data.as_mut() {
            Some(data) => data,
            None => unreachable!("checked initialized control state has data"),
        }
    }

    /// Calculates one atomic revision and fence advance without wrapping.
    fn next_revision_and_fence(&self) -> Option<(u64, FenceEpoch)> {
        Some((self.revision.checked_add(1)?, next_fence(self)?))
    }

    /// Returns the response for a command whose postcondition already holds.
    fn current(&self) -> CommandPlan {
        CommandPlan {
            state: self.clone(),
            response: VolumeCommandResponse::Current {
                revision: self.revision,
                fence: self.data.as_ref().map(|data| data.fence),
            },
        }
    }

    /// Returns a deterministic rejection without changing any state byte.
    fn rejected(&self, rejection: VolumeCommandRejection) -> CommandPlan {
        CommandPlan {
            state: self.clone(),
            response: VolumeCommandResponse::Rejected(rejection),
        }
    }

    /// Distinguishes revision exhaustion from fence exhaustion.
    fn exhausted(&self) -> CommandPlan {
        if self.revision == u64::MAX {
            self.rejected(VolumeCommandRejection::RevisionExhausted)
        } else {
            self.rejected(VolumeCommandRejection::FenceExhausted)
        }
    }
}

/// Returns the next durable data fence for initialized control state.
fn next_fence(state: &VolumeControlState) -> Option<FenceEpoch> {
    state.data.as_ref()?.fence.next()
}

/// Checks that adoption adds one built target and removes at most its old copy.
fn valid_adoption(
    current: &BTreeSet<VolumeNodeId>,
    replacement: ReplacementGrant,
    next: &BTreeSet<VolumeNodeId>,
) -> bool {
    if next.len() != 3 || !next.contains(&replacement.new_node_id) {
        return false;
    }
    replacement_copy_set(current, replacement) == *next
}

/// Calculates the only copy set one replacement grant may adopt.
fn replacement_copy_set(
    current: &BTreeSet<VolumeNodeId>,
    replacement: ReplacementGrant,
) -> BTreeSet<VolumeNodeId> {
    let mut expected = current.clone();
    if let Some(old_node_id) = replacement.old_node_id {
        expected.remove(&old_node_id);
    }
    expected.insert(replacement.new_node_id);
    expected
}

/// Compare-and-set fields shared by commands after initialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpectedVolumeRevision {
    /// Destructive generation the command is allowed to change.
    pub generation: VolumeGeneration,

    /// Exact bounded control state revision observed by the caller.
    pub revision: u64,
}

/// Initializes one pristine group with exactly three active copies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitializeVolume {
    /// Immutable descriptor shared by every replica and request.
    pub descriptor: VolumeDescriptor,

    /// Exact three-node set used for common bootstrap.
    pub initial_copies: BTreeSet<VolumeNodeId>,
}

/// Changes whether the generation is live or retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetVolumeDisposition {
    /// Expected volume revision against which this change was selected.
    pub expected: ExpectedVolumeRevision,

    /// Requested generation disposition.
    pub disposition: VolumeDisposition,
}

/// Grants foreground writer grant after recovery grant is absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrantVolumeWriter {
    /// Expected volume revision against which this writer was selected.
    pub expected: ExpectedVolumeRevision,

    /// Exact local session receiving the next data fence.
    pub writer: WriterGrant,
}

/// Revokes one exact foreground session without waiting for local cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FenceVolumeWriter {
    /// Expected volume revision against which this revocation was selected.
    pub expected: ExpectedVolumeRevision,

    /// Exact writer that must still be current.
    pub writer: WriterGrant,
}

/// Fences foreground I/O and authorizes canonical-image recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginVolumeRecovery {
    /// Expected volume revision against which recovery was selected.
    pub expected: ExpectedVolumeRevision,

    /// Exact writer being revoked, or none if already fenced.
    pub expected_writer: Option<WriterGrant>,

    /// Exact prior recovery replaced by this authorization, when any.
    pub replaced_recovery_id: Option<RecoveryId>,

    /// New bounded recovery grant.
    pub recovery: RecoveryGrant,
}

/// Revokes a recovery grant after every selected copy is durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RevokeVolumeRecovery {
    /// Expected volume revision after local recovery work completed.
    pub expected: ExpectedVolumeRevision,

    /// Exact recovery grant whose permits must be revoked.
    pub recovery_id: RecoveryId,
}

/// Authorizes one prepared learner and data-copy replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeginReplicaReplacement {
    /// Expected volume revision against which replacement was selected.
    pub expected: ExpectedVolumeRevision,

    /// New bounded replacement grant.
    pub replacement: ReplacementGrant,
}

/// Removes one exact replacement authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelReplicaReplacement {
    /// Expected volume revision against which cancellation was selected.
    pub expected: ExpectedVolumeRevision,

    /// Replacement that must still be current.
    pub replacement_id: ReplacementId,
}

/// Atomically adopts one rebuilt copy and advances the data fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdoptReplicaReplacement {
    /// Expected volume revision against which adoption was selected.
    pub expected: ExpectedVolumeRevision,

    /// Replacement whose target has been fully verified.
    pub replacement_id: ReplacementId,

    /// Exact resulting three-copy set checked against current membership.
    pub new_copies: BTreeSet<VolumeNodeId>,

    /// Exact writer preserved across adoption, or none while detached.
    pub expected_writer: Option<WriterGrant>,
}

/// One bounded semantic control-state change stored in the Raft log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VolumeCommand {
    /// Initializes a pristine group.
    Initialize(InitializeVolume),

    /// Changes the generation disposition.
    SetDisposition(SetVolumeDisposition),

    /// Grants one exact writer session.
    GrantWriter(GrantVolumeWriter),

    /// Revokes one exact writer session.
    FenceWriter(FenceVolumeWriter),

    /// Selects canonical recovery grant.
    BeginRecovery(BeginVolumeRecovery),

    /// Revokes one durably completed recovery grant.
    RevokeRecovery(RevokeVolumeRecovery),

    /// Starts one bounded replica replacement.
    BeginReplacement(BeginReplicaReplacement),

    /// Cancels one unusable replacement.
    CancelReplacement(CancelReplicaReplacement),

    /// Adopts one fully built replacement copy.
    AdoptReplacement(AdoptReplicaReplacement),
}

impl ApplicationCommand for VolumeCommand {}

/// Reason a semantic command cannot change current control state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VolumeCommandRejection {
    /// The group has no committed descriptor or data control state.
    NotInitialized,

    /// Initialization already committed different or subsequently changed state.
    AlreadyInitialized,

    /// The command targets another destructive generation.
    WrongGeneration,

    /// Only a live generation can receive operational grants.
    VolumeNotLive,

    /// A writer is outside the active data-copy set.
    WriterOutsideCopySet,

    /// The named writer is not current.
    WrongWriter,

    /// The selected active-copy set is not two or three valid nodes.
    InvalidCopySet,

    /// The recovery selection violates current data control state.
    InvalidRecovery,

    /// Another recovery remains authorized.
    RecoveryInProgress,

    /// No recovery exists to complete or replace.
    NoRecovery,

    /// The named recovery is not current.
    WrongRecovery,

    /// A recovery ID was reused with different immutable fields.
    RecoveryIdConflict,

    /// The replacement selection violates current data control state.
    InvalidReplacement,

    /// Another replacement remains authorized.
    ReplacementInProgress,

    /// No replacement exists to cancel or adopt.
    NoReplacement,

    /// The named replacement is not current.
    WrongReplacement,

    /// A replacement ID was reused with different immutable fields.
    ReplacementIdConflict,

    /// No larger control revision can be represented.
    RevisionExhausted,

    /// No larger data-plane fence can be represented.
    FenceExhausted,
}

/// Deterministic result of evaluating one committed control state command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VolumeCommandResponse {
    /// The command changed control state and the returned state must be persisted.
    Applied {
        /// New control revision.
        revision: u64,

        /// New or current fence after the transition.
        fence: Option<FenceEpoch>,
    },

    /// The exact semantic postcondition already holds.
    Current {
        /// Current control revision, which is not changed by a retry.
        revision: u64,

        /// Current fence after the earlier or later equivalent effect.
        fence: Option<FenceEpoch>,
    },

    /// The caller observed an older revision and must reread control state.
    Conflict {
        /// Current revision that made the compare-and-set fail.
        current_revision: u64,
    },

    /// The command is invalid for current control state and made no state change.
    Rejected(VolumeCommandRejection),
}

impl ApplicationResponse for VolumeCommandResponse {}

/// Pure state and response produced before the durable apply boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandPlan {
    /// Complete next state, unchanged for current, conflict, and rejection.
    pub state: VolumeControlState,

    /// Deterministic response returned through Raft.
    pub response: VolumeCommandResponse,
}

impl CommandPlan {
    /// Builds the response for one validated state transition.
    fn applied(state: VolumeControlState) -> Self {
        debug_assert!(state.validate().is_ok());
        Self {
            response: VolumeCommandResponse::Applied {
                revision: state.revision,
                fence: state.data.as_ref().map(|data| data.fence),
            },
            state,
        }
    }
}

/// Explains why decoded or constructed control state is unsafe to publish.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum VolumeControlStateInvariantError {
    /// Uninitialized control state contains a revision, grant, or disposition.
    #[error("uninitialized volume control state contains initialized state")]
    InvalidUninitializedState,

    /// Descriptor and data control state must appear together.
    #[error("volume descriptor and data control state presence differ")]
    DescriptorDataMismatch,

    /// An initialized state must have a non-zero compare-and-set revision.
    #[error("initialized volume control state has revision zero")]
    ZeroInitializedRevision,

    /// Foreground operation requires exactly two or three active copies.
    #[error("active data control state does not contain two or three copies")]
    InvalidCopyCount,

    /// The selected writer is not one of the active copies.
    #[error("volume writer is outside the active data-copy set")]
    WriterOutsideCopySet,

    /// Recovery grant and current data facts disagree.
    #[error("volume recovery grant is internally inconsistent")]
    InvalidRecovery,

    /// Replacement grant and current data facts disagree.
    #[error("volume replacement grant is internally inconsistent")]
    InvalidReplacement,

    /// Retained generations cannot contain operational grants.
    #[error("inactive volume generation contains an operational grant")]
    GrantOnInactiveVolume,
}
