use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::types::{SandboxId, SandboxResources};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AdmissionLimits {
    pub max_active_sandboxes: u64,
    pub max_starting_sandboxes: u64,
    pub max_total_sandboxes: u64,
    pub max_allocated_vcpus: u64,
    pub max_allocated_memory_mib: u64,
    pub max_allocated_disk_mib: u64,
    pub unknown_disk_reservation_mib: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AdmissionUsage {
    pub active_sandboxes: u64,
    pub starting_sandboxes: u64,
    pub total_sandboxes: u64,
    pub allocated_vcpus: u64,
    pub allocated_memory_mib: u64,
    pub allocated_disk_mib: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AllocationState {
    Starting,
    Active,
    Paused,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Allocation {
    state: AllocationState,
    resources: SandboxResources,
}

impl Allocation {
    fn usage(self) -> AdmissionUsage {
        let is_active = !matches!(self.state, AllocationState::Paused);
        AdmissionUsage {
            active_sandboxes: u64::from(is_active),
            starting_sandboxes: u64::from(matches!(self.state, AllocationState::Starting)),
            total_sandboxes: 1,
            allocated_vcpus: if is_active {
                u64::from(self.resources.cpu_count)
            } else {
                0
            },
            allocated_memory_mib: if is_active {
                u64::from(self.resources.memory_mib)
            } else {
                0
            },
            allocated_disk_mib: u64::from(self.resources.disk_size_mib),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdmissionError {
    #[error(
        "sandbox admission denied: {resource} limit {limit}, currently used {used}, requested {requested}"
    )]
    Denied {
        resource: &'static str,
        limit: u64,
        used: u64,
        requested: u64,
    },

    #[error("sandbox {sandbox_id} already has an admission allocation")]
    DuplicateSandbox { sandbox_id: SandboxId },

    #[error("sandbox {sandbox_id} does not have a paused admission allocation")]
    NotPaused { sandbox_id: SandboxId },
}

#[derive(Debug, Default)]
struct AdmissionState {
    allocations: HashMap<SandboxId, Allocation>,
    usage: AdmissionUsage,
}

impl AdmissionState {
    fn insert(&mut self, sandbox_id: SandboxId, allocation: Allocation) {
        let previous = self.allocations.insert(sandbox_id, allocation);
        debug_assert!(previous.is_none());
        self.usage.add(allocation.usage());
        self.debug_assert_consistent();
    }

    fn replace(&mut self, sandbox_id: SandboxId, allocation: Allocation) -> Option<Allocation> {
        let previous = self.allocations.get(&sandbox_id).copied()?;
        self.allocations.insert(sandbox_id, allocation);
        self.usage.subtract(previous.usage());
        self.usage.add(allocation.usage());
        self.debug_assert_consistent();
        Some(previous)
    }

    fn remove(&mut self, sandbox_id: &SandboxId) -> Option<Allocation> {
        let removed = self.allocations.remove(sandbox_id)?;
        self.usage.subtract(removed.usage());
        self.debug_assert_consistent();
        Some(removed)
    }

    fn debug_assert_consistent(&self) {
        #[cfg(debug_assertions)]
        {
            let mut recomputed = AdmissionUsage::default();
            for allocation in self.allocations.values() {
                recomputed.add(allocation.usage());
            }
            debug_assert_eq!(self.usage, recomputed);
        }
    }
}

impl AdmissionUsage {
    fn add(&mut self, other: Self) {
        self.active_sandboxes = self.active_sandboxes.saturating_add(other.active_sandboxes);
        self.starting_sandboxes = self
            .starting_sandboxes
            .saturating_add(other.starting_sandboxes);
        self.total_sandboxes = self.total_sandboxes.saturating_add(other.total_sandboxes);
        self.allocated_vcpus = self.allocated_vcpus.saturating_add(other.allocated_vcpus);
        self.allocated_memory_mib = self
            .allocated_memory_mib
            .saturating_add(other.allocated_memory_mib);
        self.allocated_disk_mib = self
            .allocated_disk_mib
            .saturating_add(other.allocated_disk_mib);
    }

    fn subtract(&mut self, other: Self) {
        self.active_sandboxes = self.active_sandboxes.saturating_sub(other.active_sandboxes);
        self.starting_sandboxes = self
            .starting_sandboxes
            .saturating_sub(other.starting_sandboxes);
        self.total_sandboxes = self.total_sandboxes.saturating_sub(other.total_sandboxes);
        self.allocated_vcpus = self.allocated_vcpus.saturating_sub(other.allocated_vcpus);
        self.allocated_memory_mib = self
            .allocated_memory_mib
            .saturating_sub(other.allocated_memory_mib);
        self.allocated_disk_mib = self
            .allocated_disk_mib
            .saturating_sub(other.allocated_disk_mib);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmissionController {
    limits: AdmissionLimits,
    state: Arc<Mutex<AdmissionState>>,
}

impl AdmissionController {
    pub fn new(
        limits: AdmissionLimits,
        paused: impl IntoIterator<Item = (SandboxId, SandboxResources)>,
    ) -> Self {
        let mut state = AdmissionState::default();
        for (sandbox_id, resources) in paused {
            if state.allocations.contains_key(&sandbox_id) {
                continue;
            }
            state.insert(
                sandbox_id,
                Allocation {
                    state: AllocationState::Paused,
                    resources,
                },
            );
        }
        Self {
            limits,
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn reserve_create(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) -> Result<StartReservation, AdmissionError> {
        self.reserve_creates([(sandbox_id, resources)])
    }

    pub fn reserve_creates(
        &self,
        allocations: impl IntoIterator<Item = (SandboxId, SandboxResources)>,
    ) -> Result<StartReservation, AdmissionError> {
        let allocations = allocations
            .into_iter()
            .map(|(sandbox_id, resources)| {
                (sandbox_id, self.resources_for_new_allocation(resources))
            })
            .collect::<Vec<_>>();
        let mut unique_ids = HashSet::with_capacity(allocations.len());
        let mut requested = AdmissionUsage::default();
        for (sandbox_id, resources) in &allocations {
            if !unique_ids.insert(*sandbox_id) {
                return Err(AdmissionError::DuplicateSandbox {
                    sandbox_id: *sandbox_id,
                });
            }
            requested.add(
                Allocation {
                    state: AllocationState::Starting,
                    resources: *resources,
                }
                .usage(),
            );
        }

        let mut state = self.state.lock();
        for (sandbox_id, _) in &allocations {
            if state.allocations.contains_key(sandbox_id) {
                return Err(AdmissionError::DuplicateSandbox {
                    sandbox_id: *sandbox_id,
                });
            }
        }
        self.limits.ensure_available(state.usage, requested)?;
        for (sandbox_id, resources) in &allocations {
            state.insert(
                *sandbox_id,
                Allocation {
                    state: AllocationState::Starting,
                    resources: *resources,
                },
            );
        }
        drop(state);

        Ok(StartReservation::new(
            self.clone(),
            allocations
                .into_iter()
                .map(|(sandbox_id, _)| (sandbox_id, RollbackState::Remove)),
        ))
    }

    pub fn reserve_resume(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) -> Result<StartReservation, AdmissionError> {
        let mut state = self.state.lock();
        if !state.allocations.contains_key(&sandbox_id) {
            state.insert(
                sandbox_id,
                Allocation {
                    state: AllocationState::Paused,
                    resources,
                },
            );
        }
        let previous = state.allocations[&sandbox_id];
        if previous.state != AllocationState::Paused {
            return Err(AdmissionError::NotPaused { sandbox_id });
        }

        let mut used_without_paused = state.usage;
        used_without_paused.subtract(previous.usage());
        let starting = Allocation {
            state: AllocationState::Starting,
            resources,
        };
        self.limits
            .ensure_available(used_without_paused, starting.usage())?;
        state.replace(sandbox_id, starting);
        drop(state);

        Ok(StartReservation::new(
            self.clone(),
            [(sandbox_id, RollbackState::Paused(resources))],
        ))
    }

    pub fn mark_paused(&self, sandbox_id: SandboxId, resources: SandboxResources) {
        let mut state = self.state.lock();
        let paused = Allocation {
            state: AllocationState::Paused,
            resources,
        };
        if state.replace(sandbox_id, paused).is_none() {
            state.insert(sandbox_id, paused);
        }
    }

    pub fn remove(&self, sandbox_id: &SandboxId) {
        self.state.lock().remove(sandbox_id);
    }

    #[cfg(test)]
    pub fn usage(&self) -> AdmissionUsage {
        self.state.lock().usage
    }

    fn resources_for_new_allocation(&self, mut resources: SandboxResources) -> SandboxResources {
        if resources.disk_size_mib == 0 {
            resources.disk_size_mib = self.limits.unknown_disk_reservation_mib;
        }
        resources
    }

    fn mark_active(&self, sandbox_id: SandboxId, resources: SandboxResources) {
        let mut state = self.state.lock();
        let active = Allocation {
            state: AllocationState::Active,
            resources,
        };
        if state.replace(sandbox_id, active).is_none() {
            state.insert(sandbox_id, active);
        }
    }

    fn rollback(&self, sandbox_id: SandboxId, rollback: RollbackState) {
        match rollback {
            RollbackState::Remove => self.remove(&sandbox_id),
            RollbackState::Paused(resources) => self.mark_paused(sandbox_id, resources),
        }
    }
}

impl AdmissionLimits {
    fn ensure_available(
        self,
        used: AdmissionUsage,
        requested: AdmissionUsage,
    ) -> Result<(), AdmissionError> {
        for (resource, limit, used, requested) in [
            (
                "active sandboxes",
                self.max_active_sandboxes,
                used.active_sandboxes,
                requested.active_sandboxes,
            ),
            (
                "starting sandboxes",
                self.max_starting_sandboxes,
                used.starting_sandboxes,
                requested.starting_sandboxes,
            ),
            (
                "total sandboxes",
                self.max_total_sandboxes,
                used.total_sandboxes,
                requested.total_sandboxes,
            ),
            (
                "allocated vCPUs",
                self.max_allocated_vcpus,
                used.allocated_vcpus,
                requested.allocated_vcpus,
            ),
            (
                "allocated memory MiB",
                self.max_allocated_memory_mib,
                used.allocated_memory_mib,
                requested.allocated_memory_mib,
            ),
            (
                "allocated disk MiB",
                self.max_allocated_disk_mib,
                used.allocated_disk_mib,
                requested.allocated_disk_mib,
            ),
        ] {
            if limit != 0 && requested > limit.saturating_sub(used) {
                metrics::counter!(
                    "agentenv_orchestrator_admission_denied_total",
                    "resource" => resource,
                )
                .increment(1);
                return Err(AdmissionError::Denied {
                    resource,
                    limit,
                    used,
                    requested,
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum RollbackState {
    Remove,
    Paused(SandboxResources),
}

#[derive(Debug)]
pub(crate) struct StartReservation {
    controller: AdmissionController,
    pending: HashMap<SandboxId, RollbackState>,
}

impl StartReservation {
    fn new(
        controller: AdmissionController,
        pending: impl IntoIterator<Item = (SandboxId, RollbackState)>,
    ) -> Self {
        Self {
            controller,
            pending: pending.into_iter().collect(),
        }
    }

    pub fn commit_active(&mut self, sandbox_id: SandboxId, resources: SandboxResources) {
        if self.pending.remove(&sandbox_id).is_some() {
            self.controller.mark_active(sandbox_id, resources);
        }
    }

    pub fn rollback_one(&mut self, sandbox_id: SandboxId) {
        if let Some(rollback) = self.pending.remove(&sandbox_id) {
            self.controller.rollback(sandbox_id, rollback);
        }
    }
}

impl Drop for StartReservation {
    fn drop(&mut self) {
        for (sandbox_id, rollback) in self.pending.drain() {
            self.controller.rollback(sandbox_id, rollback);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    fn resources(cpu_count: u32, memory_mib: u32, disk_size_mib: u32) -> SandboxResources {
        SandboxResources {
            cpu_count,
            memory_mib,
            disk_size_mib,
        }
    }

    #[test]
    fn reservation_is_rolled_back_when_dropped() {
        let controller = AdmissionController::new(
            AdmissionLimits {
                max_active_sandboxes: 1,
                ..Default::default()
            },
            [],
        );
        let sandbox_id = SandboxId::new();
        let reservation = controller
            .reserve_create(sandbox_id, resources(2, 256, 1024))
            .unwrap();
        assert_eq!(controller.usage().starting_sandboxes, 1);

        drop(reservation);

        assert_eq!(controller.usage(), AdmissionUsage::default());
    }

    #[test]
    fn batch_reservation_is_atomic() {
        let controller = AdmissionController::new(
            AdmissionLimits {
                max_allocated_memory_mib: 1024,
                ..Default::default()
            },
            [],
        );
        let result = controller.reserve_creates([
            (SandboxId::new(), resources(1, 768, 1)),
            (SandboxId::new(), resources(1, 768, 1)),
        ]);

        assert!(matches!(result, Err(AdmissionError::Denied { .. })));
        assert_eq!(controller.usage(), AdmissionUsage::default());
    }

    #[test]
    fn unknown_disk_size_uses_the_configured_reservation() {
        let controller = AdmissionController::new(
            AdmissionLimits {
                max_allocated_disk_mib: 4096,
                unknown_disk_reservation_mib: 8192,
                ..Default::default()
            },
            [],
        );

        let result = controller.reserve_create(SandboxId::new(), resources(1, 128, 0));

        assert!(matches!(
            result,
            Err(AdmissionError::Denied {
                resource: "allocated disk MiB",
                ..
            })
        ));
    }

    #[test]
    fn resume_rollback_restores_paused_accounting() {
        let sandbox_id = SandboxId::new();
        let resources = resources(2, 512, 4096);
        let controller = AdmissionController::new(
            AdmissionLimits {
                max_active_sandboxes: 1,
                ..Default::default()
            },
            [(sandbox_id, resources)],
        );

        let reservation = controller.reserve_resume(sandbox_id, resources).unwrap();
        assert_eq!(controller.usage().active_sandboxes, 1);
        assert_eq!(controller.usage().starting_sandboxes, 1);
        drop(reservation);

        assert_eq!(controller.usage().active_sandboxes, 0);
        assert_eq!(controller.usage().allocated_disk_mib, 4096);
        assert_eq!(controller.usage().total_sandboxes, 1);
    }

    #[test]
    fn concurrent_reservations_never_exceed_limit() {
        let controller = Arc::new(AdmissionController::new(
            AdmissionLimits {
                max_starting_sandboxes: 8,
                ..Default::default()
            },
            [],
        ));
        let mut workers = Vec::new();
        for _ in 0..64 {
            let controller = Arc::clone(&controller);
            workers.push(thread::spawn(move || {
                controller
                    .reserve_create(SandboxId::new(), resources(1, 1, 1))
                    .ok()
            }));
        }
        let reservations = workers
            .into_iter()
            .filter_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(reservations.len(), 8);
        assert_eq!(controller.usage().starting_sandboxes, 8);
        drop(reservations);
        assert_eq!(controller.usage(), AdmissionUsage::default());
    }

    #[test]
    fn committed_allocation_survives_reservation_drop() {
        let controller = AdmissionController::new(AdmissionLimits::default(), []);
        let sandbox_id = SandboxId::new();
        let mut reservation = controller
            .reserve_create(sandbox_id, resources(1, 128, 1024))
            .unwrap();
        reservation.commit_active(sandbox_id, resources(2, 256, 2048));
        drop(reservation);

        assert_eq!(
            controller.usage(),
            AdmissionUsage {
                active_sandboxes: 1,
                total_sandboxes: 1,
                allocated_vcpus: 2,
                allocated_memory_mib: 256,
                allocated_disk_mib: 2048,
                ..Default::default()
            }
        );
        controller.mark_paused(sandbox_id, resources(2, 256, 2048));
        assert_eq!(controller.usage().active_sandboxes, 0);
        controller.remove(&sandbox_id);
        assert_eq!(controller.usage(), AdmissionUsage::default());
    }
}
