use thiserror::Error;

/// Caller-selected limits for one local Raft group runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeLimitSettings {
    /// Largest number of durable groups accepted during startup.
    pub max_saved_groups: usize,

    /// Largest number of groups allowed to run at once.
    pub max_active_groups: usize,

    /// Largest number of groups allowed to start at once.
    pub max_parallel_starts: usize,

    /// Largest number of shared background jobs allowed at once.
    pub max_background_jobs: usize,
}

/// Checked runtime limits with no built-in product defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeLimits {
    max_saved_groups: usize,
    max_active_groups: usize,
    max_parallel_starts: usize,
    max_background_jobs: usize,
}

impl RuntimeLimits {
    /// Checks caller-selected limits before they control runtime resources.
    pub fn new(settings: RuntimeLimitSettings) -> Result<Self, InvalidRuntimeLimits> {
        if settings.max_saved_groups == 0 {
            return Err(InvalidRuntimeLimits::Zero {
                field: "max_saved_groups",
            });
        }
        if settings.max_active_groups == 0 {
            return Err(InvalidRuntimeLimits::Zero {
                field: "max_active_groups",
            });
        }
        if settings.max_parallel_starts == 0 {
            return Err(InvalidRuntimeLimits::Zero {
                field: "max_parallel_starts",
            });
        }
        if settings.max_background_jobs == 0 {
            return Err(InvalidRuntimeLimits::Zero {
                field: "max_background_jobs",
            });
        }
        if settings.max_parallel_starts > settings.max_active_groups {
            return Err(InvalidRuntimeLimits::StartsExceedActive {
                starts: settings.max_parallel_starts,
                active: settings.max_active_groups,
            });
        }

        Ok(Self {
            max_saved_groups: settings.max_saved_groups,
            max_active_groups: settings.max_active_groups,
            max_parallel_starts: settings.max_parallel_starts,
            max_background_jobs: settings.max_background_jobs,
        })
    }

    /// Returns the startup discovery limit.
    #[must_use]
    pub const fn max_saved_groups(self) -> usize {
        self.max_saved_groups
    }

    /// Returns the live group limit.
    #[must_use]
    pub const fn max_active_groups(self) -> usize {
        self.max_active_groups
    }

    /// Returns the parallel group start limit.
    #[must_use]
    pub const fn max_parallel_starts(self) -> usize {
        self.max_parallel_starts
    }

    /// Returns the shared background job limit.
    #[must_use]
    pub const fn max_background_jobs(self) -> usize {
        self.max_background_jobs
    }
}

/// Rejects a runtime resource limit that cannot be applied safely.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidRuntimeLimits {
    /// Every resource limit must permit at least one item.
    #[error("{field} must be greater than zero")]
    Zero {
        /// Name of the invalid settings field.
        field: &'static str,
    },

    /// Parallel starts cannot exceed the total number of live groups.
    #[error(
        "max_parallel_starts ({starts}) cannot exceed max_active_groups \
         ({active})"
    )]
    StartsExceedActive {
        /// Requested number of parallel starts.
        starts: usize,

        /// Requested number of live groups.
        active: usize,
    },
}
