use std::{
    collections::VecDeque,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use envconfig::Envconfig;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio_util::sync::CancellationToken;

use crate::{
    utils::ReadString,
    worker_engine::{
        errors::{
            AttachError, ForceRecycleError, IOError, LeaseSlotError, OfferTemplateError,
            ReleaseError,
        },
        messages::{ConsumerReply, EngineMessage},
        traits::{ConsumerIO, EngineIO, EngineInbox, MetricIO, PostgresClient},
    },
};

#[derive(Envconfig, Debug, Clone, Copy)]
pub struct WorkerEngineConfig {
    #[envconfig(from = "PGTEST_POOL_INITIAL_SIZE", default = "16")]
    pub initial_slots: u16,
    #[envconfig(from = "PGTEST_POOL_MAXIMUM_SIZE", default = "96")]
    pub maximum_slots: u16,
    #[envconfig(from = "PGTEST_POOL_STARVATION_THRESHOLD", default = "8")]
    pub starvation_threshold: u16,
    #[envconfig(from = "PGTEST_POOL_GROW_BATCH_SIZE", default = "16")]
    pub grow_batch_size: u16,

    /// Maximum retired databases queued for or undergoing background deletion.
    #[envconfig(from = "PGTEST_CLEANUP_MAX_PENDING", default = "16")]
    pub cleanup_max_pending: u16,
    #[envconfig(from = "PGTEST_CLEANUP_CONCURRENCY", default = "2")]
    pub cleanup_concurrency: u16,

    #[envconfig(from = "PGTEST_LEASE_CLAIM_TIMEOUT_MS", default = "30000")]
    pub lease_claim_timeout_ms: u64,
    /// Counts every distinct admitted ID, including pending and closed leases.
    #[envconfig(from = "PGTEST_MAX_LEASE_RECORDS", default = "100000")]
    pub max_lease_records: usize,
}

pub type LeaseId = ReadString;

pub fn is_valid_lease_id(lease: &str) -> bool {
    !lease.is_empty() && lease.len() <= 256 && !lease.contains(['/', '\0'])
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LeaseStatus {
    Open,
    Closed,
}

struct Retirement {
    lease: LeaseId,
    generation: u64,
    database_name: ReadString,
    scheduled: bool,
}

#[derive(Clone)]
#[cfg_attr(test, derive(Debug))]
pub(crate) enum Slot {
    Empty,
    Creating { db_name: ReadString },
    Ready { db_name: ReadString },
    Leased { db_name: ReadString, lease: LeaseId },
    Done { db_name: ReadString },
}

impl Slot {
    fn is_ready(&self) -> bool {
        matches!(self, Slot::Ready { db_name: _ })
    }

    fn state_name(&self) -> &'static str {
        match self {
            Slot::Empty => "Empty",
            Slot::Creating { .. } => "Creating",
            Slot::Ready { .. } => "Ready",
            Slot::Leased { .. } => "Leased",
            Slot::Done { .. } => "Done",
        }
    }

    fn db_name(&self) -> Option<ReadString> {
        match self {
            Slot::Empty => None,
            Slot::Creating { db_name }
            | Slot::Done { db_name }
            | Slot::Leased { db_name, lease: _ }
            | Slot::Ready { db_name } => Some(db_name.clone()),
        }
    }
}

pub type SlotIdx = usize;

#[cfg_attr(test, derive(Clone, Debug))]
pub(crate) struct LeaseEntry {
    pub(crate) slot_idx: SlotIdx,
    pub(crate) conns: u16,
    pub(crate) generation: u64,
    pub(crate) cancellation: CancellationToken,
}

#[cfg_attr(test, derive(Clone, Debug))]
pub(crate) struct PoolCapacity {
    pub(crate) maximum: u16,
    pub(crate) current: u16,
    pub(crate) starvation_threshold: u16,
}

impl PoolCapacity {
    pub fn new(pool_config: WorkerEngineConfig) -> PoolCapacity {
        PoolCapacity {
            maximum: pool_config.maximum_slots,
            current: pool_config.initial_slots,
            starvation_threshold: pool_config.starvation_threshold,
        }
    }

    pub fn is_at_maximum_capacity(&self) -> bool {
        self.current >= self.maximum
    }
}

// TBR: Once test are completed, it's time to revisit how the data it's
// persisted.
pub(crate) struct WorkerEngine<Consumer, IO, Inbox, Metrics, Postgres>
where
    Consumer: ConsumerIO,
    IO: EngineIO<Consumer, Postgres>,
    Inbox: EngineInbox<Consumer>,
    Metrics: MetricIO,
    Postgres: PostgresClient,
{
    pub(crate) slots: Box<[Slot]>,
    pub(crate) leases: FxHashMap<LeaseId, LeaseEntry>,
    // Identity records survive physical cleanup. Slots and connection counts do not.
    lease_records: FxHashMap<LeaseId, LeaseStatus>,
    next_generation: u64,
    retirements: FxHashMap<SlotIdx, Retirement>,
    pub(crate) capacity: PoolCapacity,
    config: WorkerEngineConfig,
    pg_client: Arc<Postgres>,
    pub(crate) ready_slots: VecDeque<SlotIdx>,
    /// One reservation per creation or replacement, including queued scheduling
    /// retries.
    pub(crate) pending_creations: FxHashSet<SlotIdx>,
    /// Connections waiting for a free slot to lease.
    pub(crate) waiters: VecDeque<LeaseId>,
    group_waiters: FxHashMap<LeaseId, Vec<(Consumer, Instant)>>,
    pub(crate) counters: EngineCounters,
    engine_io: IO,
    // TBD: Once the proxy is completed and I have discovered all useful metrics, implement the
    // metrics worker to save all changes on append mode. Until then, this property is marked
    // as a PhantomData. This metrics will be useful for debugging and understand how the engine
    // behave in an exact point.
    metrics_io: PhantomData<Metrics>,
    inbox: Inbox,
    consumer: PhantomData<Consumer>,
    root_cancellation_token: CancellationToken,
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone, Debug))]
pub struct EngineCounters {
    pub rejected_attach_max_lifetime: u64,
    pub waiter_timeouts: u64,
    pub template_create_failures: u64,
    pub detach_on_zero: u64,
    pub non_ready_slots: u64,
    pub unable_to_start_database_slots: u64,
}

impl<Consumer, IO, Inbox, Metrics, Postgres> WorkerEngine<Consumer, IO, Inbox, Metrics, Postgres>
where
    Consumer: ConsumerIO,
    IO: EngineIO<Consumer, Postgres>,
    Inbox: EngineInbox<Consumer>,
    Metrics: MetricIO,
    Postgres: PostgresClient,
{
    pub fn new(
        pool_worker_config: WorkerEngineConfig,
        postgres_manager: Arc<Postgres>,
        engine_io: IO,
        inbox: Inbox,
    ) -> WorkerEngine<Consumer, IO, Inbox, Metrics, Postgres> {
        let slots: Box<[Slot]> =
            vec![Slot::Empty; pool_worker_config.maximum_slots as usize].into_boxed_slice();
        let capacity = PoolCapacity::new(pool_worker_config);
        let leases = FxHashMap::default();
        let ready_slots: VecDeque<SlotIdx> = VecDeque::new();
        let root_cancellation_token = CancellationToken::new();

        WorkerEngine::<Consumer, IO, Inbox, Metrics, Postgres> {
            slots,
            leases,
            lease_records: FxHashMap::default(),
            next_generation: 0,
            retirements: FxHashMap::default(),
            capacity,
            config: pool_worker_config.clone(), // TODO: Do we need all the config?
            pg_client: postgres_manager,
            ready_slots,
            pending_creations: FxHashSet::default(),
            waiters: VecDeque::new(),
            group_waiters: FxHashMap::default(),
            counters: EngineCounters::default(),
            engine_io,
            metrics_io: PhantomData,
            inbox,
            consumer: PhantomData,
            root_cancellation_token,
        }
    }

    #[hotpath::measure]
    pub async fn try_init(&mut self) {
        for index in 0..self.capacity.current {
            match self.pg_client.create_database().await {
                Ok(template_name) => {
                    tracing::info!("Created {template_name}");
                    if let Some(elem) = self.slots.get_mut(index as usize) {
                        *elem = Slot::Ready { db_name: template_name.clone() };
                        self.ready_slots.push_back(index as usize);
                    }
                }
                Err(_error) => {
                    dbg!(_error);
                    tracing::error!(
                        "initial database creation failed with a non-transient error restart the \
                         service"
                    );
                    panic!("failed to create the initial batch of template databases");
                }
            };
        }
    }

    #[hotpath::measure]
    pub async fn run(&mut self) {
        let mut retry = tokio::time::interval(Duration::from_secs(1));
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let message = tokio::select! {
            message = self.inbox.wait_for_message() => message,
            _ = retry.tick(), if self.retirements.values().any(|job| !job.scheduled) => {
                let indexes: Vec<_> = self.retirements.iter()
                    .filter_map(|(&index, job)| (!job.scheduled).then_some(index)).collect();
                for index in indexes {
                    self.spawn_retirement(index);
                }
                continue;
            }
            };
            let Some(msg) = message else { break };
            match msg {
                EngineMessage::AttachOrJoin { lease, reply, message_time } => {
                    self.attach_or_join(lease, reply, message_time);
                }
                EngineMessage::ReleaseLease { lease, reply } => {
                    let result = self.release_lease(&lease);
                    // Closure and cleanup belong to the engine even if this
                    // reply is lost.
                    let _ = reply.reply(ConsumerReply::ReleaseResult(result));
                }
                EngineMessage::DeleteLease { lease, generation } => {
                    if let Some(entry) = self.leases.get(&lease) {
                        if entry.generation == generation {
                            let index = entry.slot_idx;
                            self.leases.remove(&lease);
                            self.retirements.remove(&index);
                        }
                    }
                }
                EngineMessage::TemplateCreated { index, result } => {
                    if !self.pending_creations.remove(&index) {
                        tracing::warn!(index, "ignoring completion without a pending creation");
                        continue;
                    }
                    let template_name = match result {
                        Ok(template_name) => template_name,
                        Err(_error) => {
                            self.counters.template_create_failures += 1;
                            self.slots[index] = Slot::Empty;
                            self.grow();
                            continue;
                        }
                    };

                    match self.offer_template(template_name, index) {
                        Ok(()) => (),
                        Err(OfferTemplateError::WorkerSlotNotInitialized(slot)) => {
                            tracing::error!(
                                "worker slot {slot} is not initialized; restarting the service is \
                                 recommended"
                            );
                        }
                    }
                    self.grow();
                }
                EngineMessage::RetryDatabaseCreation { index, try_number } => {
                    if !self.pending_creations.contains(&index) {
                        continue;
                    }
                    self.spawn_creation(index, try_number);
                    self.grow();
                }
                EngineMessage::Detach { lease, generation } => match self.leases.get_mut(&lease) {
                    Some(leased_worker) if leased_worker.generation == generation => {
                        leased_worker.conns = if leased_worker.conns == 0 {
                            self.counters.detach_on_zero += 1;
                            leased_worker.conns
                        } else {
                            leased_worker.conns - 1
                        };
                    }
                    _ => {}
                },
                EngineMessage::LeaseMaxTimeReached { lease, generation } => {
                    if !self.leases.get(&lease).is_some_and(|entry| entry.generation == generation)
                    {
                        continue;
                    }
                    self.counters.rejected_attach_max_lifetime += 1;

                    match self.force_recycle(&lease) {
                        Ok(_) => {
                            continue;
                        }
                        Err(ForceRecycleError::UnknownLease(_)) => {
                            tracing::warn!(
                                "received a max-lease-time message for {lease}, but the lease id \
                                 does not exist"
                            );
                        }
                        Err(ForceRecycleError::DatabaseIsDown) => {
                            tracing::error!(
                                "halting the worker engine: unable to drop or create a database \
                                 in postgres"
                            );
                            panic!("Unable to communicate with postgresql")
                        }
                        Err(ForceRecycleError::WorkerSlotNotInitialized(index)) => {
                            tracing::error!(
                                "worker slot {index} is not initialized; restarting the service \
                                 is recommended"
                            );
                        }
                    }
                }
                #[cfg(test)]
                EngineMessage::Barrier { reply } => {
                    let _ = reply.send(());
                }
                EngineMessage::Shutdown => {
                    self.root_cancellation_token.cancel();
                    return;
                }
            }
        }
    }

    fn admit_lease(&mut self, lease: &LeaseId) -> Result<LeaseStatus, ReleaseError> {
        if !is_valid_lease_id(lease) {
            return Err(ReleaseError::InvalidLeaseId);
        }
        if let Some(status) = self.lease_records.get(lease) {
            return Ok(*status);
        }
        if self.lease_records.len() >= self.config.max_lease_records {
            return Err(ReleaseError::LeaseRecordLimitReached);
        }
        self.lease_records.insert(lease.clone(), LeaseStatus::Open);
        Ok(LeaseStatus::Open)
    }

    fn release_lease(&mut self, lease: &LeaseId) -> Result<(), ReleaseError> {
        if self.admit_lease(lease)? == LeaseStatus::Closed {
            return Ok(());
        }
        // Validate the active ownership before committing closure.
        if let Some(entry) = self.leases.get(lease) {
            if !matches!(self.slots.get(entry.slot_idx),
                Some(Slot::Leased { lease: owner, .. }) if owner == lease)
                && !matches!(self.slots.get(entry.slot_idx), Some(Slot::Done { .. }))
            {
                return Err(ReleaseError::InvalidSlot);
            }
        }
        self.lease_records.insert(lease.clone(), LeaseStatus::Closed);
        if let Some(waiters) = self.group_waiters.remove(lease) {
            for (reply, _) in waiters {
                let _ = reply.reply(ConsumerReply::AttachRejected(AttachError::LeaseClosed));
            }
        }
        self.waiters.retain(|waiting| waiting != lease);
        if self.leases.contains_key(lease) {
            self.force_recycle(lease).map_err(|_| ReleaseError::InvalidSlot)?;
        }
        Ok(())
    }

    fn attach_or_join(&mut self, lease: LeaseId, reply: Consumer, message_time: Instant) {
        match self.admit_lease(&lease) {
            Ok(LeaseStatus::Closed) => {
                let _ = reply.reply(ConsumerReply::AttachRejected(AttachError::LeaseClosed));
                return;
            }
            Err(error) => {
                let error = match error {
                    ReleaseError::InvalidLeaseId => AttachError::InvalidLeaseId,
                    _ => AttachError::LeaseRecordLimitReached,
                };
                let _ = reply.reply(ConsumerReply::AttachRejected(error));
                return;
            }
            Ok(LeaseStatus::Open) => {}
        }
        if let Some(entry) = self.leases.get(&lease) {
            let database_name = match self.slots.get(entry.slot_idx) {
                Some(Slot::Leased { db_name, lease: owner }) if owner == &lease => db_name.clone(),
                _ => {
                    let _ = reply.reply(ConsumerReply::FailedToAttach);
                    return;
                }
            };
            self.reply_attached(&lease, database_name, reply);
            return;
        }
        let Some(index) = self.ready_slots.pop_front() else {
            if !self.group_waiters.contains_key(&lease) {
                self.waiters.push_back(lease.clone());
            }
            self.group_waiters.entry(lease).or_default().push((reply, message_time));
            self.grow();
            return;
        };
        match self.lease_slot(index, &lease) {
            Ok(database_name) => {
                self.reply_attached(&lease, database_name, reply);
                self.restore_unclaimed_slot(&lease);
            }
            Err(error) => {
                tracing::error!(%error, "unable to assign a ready slot");
                let _ = reply.reply(ConsumerReply::FailedToAttach);
            }
        }
        self.grow();
    }

    fn reply_attached(&mut self, lease: &LeaseId, database_name: ReadString, reply: Consumer) {
        let entry = self.leases.get_mut(lease).expect("reply requires an assigned lease");
        let Some(conns) = entry.conns.checked_add(1) else {
            let _ = reply.reply(ConsumerReply::FailedToAttach);
            return;
        };
        entry.conns = conns;
        if reply
            .reply(ConsumerReply::Attached {
                database_name,
                generation: entry.generation,
                cancellation: entry.cancellation.clone(),
            })
            .is_err()
        {
            entry.conns -= 1;
        }
    }

    fn restore_unclaimed_slot(&mut self, lease: &LeaseId) {
        if !self.leases.get(lease).is_some_and(|entry| entry.conns == 0) {
            return;
        }
        let entry = self.leases.remove(lease).unwrap();
        entry.cancellation.cancel();
        if let Some(db_name) = self.slots[entry.slot_idx].db_name() {
            self.slots[entry.slot_idx] = Slot::Ready { db_name };
            self.ready_slots.push_front(entry.slot_idx);
        }
    }

    #[hotpath::measure]
    fn lease_slot(
        &mut self,
        worker_index: SlotIdx,
        lease: &LeaseId,
    ) -> Result<ReadString, LeaseSlotError> {
        let Some(worker_state) = self.slots.get_mut(worker_index) else {
            return Err(LeaseSlotError::WorkerSlotNotInitialized(worker_index));
        };
        if !worker_state.is_ready() {
            return Err(LeaseSlotError::WorkerSlotNotReady {
                slot: worker_index,
                state: worker_state.state_name(),
            });
        }
        let Some(database_name) = worker_state.db_name() else {
            return Err(LeaseSlotError::WorkerSlotNotInitialized(worker_index));
        };

        let new_state = Slot::Leased { db_name: database_name.clone(), lease: lease.clone() };
        *worker_state = new_state;
        self.next_generation =
            self.next_generation.checked_add(1).expect("lease generation exhausted");
        let generation = self.next_generation;
        let child_token = self.root_cancellation_token.child_token();
        self.leases.insert(
            lease.clone(),
            LeaseEntry {
                slot_idx: worker_index,
                conns: 0,
                generation,
                cancellation: child_token.clone(),
            },
        );

        if self.config.lease_claim_timeout_ms > 0 {
            if self
                .engine_io
                .send_delayed_message(
                    EngineMessage::LeaseMaxTimeReached { lease: lease.clone(), generation },
                    self.config.lease_claim_timeout_ms as u32,
                    child_token,
                )
                .is_err()
            {
                tracing::error!(
                    "communication with the worker engine is down and messages are being lost; \
                     restart the service, and if it happens again run it in debug mode"
                );
            };
        }

        Ok(database_name)
    }

    #[hotpath::measure]
    fn offer_template(
        &mut self,
        database_name: ReadString,
        worker_index: SlotIdx,
    ) -> Result<(), OfferTemplateError> {
        let Some(slot) = self.slots.get_mut(worker_index) else {
            return Err(OfferTemplateError::WorkerSlotNotInitialized(worker_index));
        };
        *slot = Slot::Ready { db_name: database_name.clone() };
        while let Some(lease) = self.waiters.pop_front() {
            let Some(waiters) = self.group_waiters.remove(&lease) else {
                continue;
            };
            if self.lease_records.get(&lease) == Some(&LeaseStatus::Closed) {
                for (reply, _) in waiters {
                    let _ = reply.reply(ConsumerReply::AttachRejected(AttachError::LeaseClosed));
                }
                continue;
            }
            let already_assigned = self.leases.contains_key(&lease);
            for (reply, message_time) in waiters {
                if self.config.lease_claim_timeout_ms > 0
                    && message_time.elapsed().as_millis()
                        > u128::from(self.config.lease_claim_timeout_ms)
                {
                    self.counters.waiter_timeouts += 1;
                    continue;
                }
                let name = if let Some(entry) = self.leases.get(&lease) {
                    match self.slots.get(entry.slot_idx) {
                        Some(Slot::Leased { db_name, lease: owner }) if owner == &lease => {
                            db_name.clone()
                        }
                        _ => {
                            let _ = reply.reply(ConsumerReply::FailedToAttach);
                            continue;
                        }
                    }
                } else {
                    self.lease_slot(worker_index, &lease)
                        .map_err(|_| OfferTemplateError::WorkerSlotNotInitialized(worker_index))?
                };
                self.reply_attached(&lease, name, reply);
            }
            if already_assigned {
                continue;
            }
            if let Some(entry) = self.leases.get(&lease) {
                if entry.conns > 0 {
                    return Ok(());
                }
                // No receiver accepted this assignment. The clone remains
                // available.
                let entry = self.leases.remove(&lease).unwrap();
                entry.cancellation.cancel();
                self.slots[worker_index] = Slot::Ready { db_name: database_name.clone() };
            }
        }
        self.ready_slots.push_back(worker_index);
        Ok(())
    }

    pub(crate) fn grow(&mut self) {
        let batch_size = usize::from(self.config.grow_batch_size);
        if batch_size == 0 || self.capacity.is_at_maximum_capacity() {
            return;
        }

        let waiting = self
            .group_waiters
            .iter()
            .filter(|(lease, replies)| {
                !self.leases.contains_key(*lease)
                    && replies.iter().any(|(_, queued_at)| {
                        self.config.lease_claim_timeout_ms == 0
                            || queued_at.elapsed().as_millis()
                                <= u128::from(self.config.lease_claim_timeout_ms)
                    })
            })
            .count();
        // Preserve the inclusive starvation threshold after covering live
        // leases.
        let required = waiting + usize::from(self.capacity.starvation_threshold) + 1;

        while !self.capacity.is_at_maximum_capacity() {
            let deficit =
                required.saturating_sub(self.ready_slots.len() + self.pending_creations.len());
            if deficit == 0 {
                return;
            }

            let remaining = usize::from(self.capacity.maximum - self.capacity.current);
            let count = (deficit.div_ceil(batch_size) * batch_size).min(remaining);
            let start = usize::from(self.capacity.current);
            let end = start + count;

            // Reserve the entire batch before spawning. Later attaches can grow
            // again immediately, but only when this work cannot
            // cover their demand.
            self.capacity.current = end as u16;
            for index in start..end {
                debug_assert!(matches!(self.slots[index], Slot::Empty));
                self.slots[index] = Slot::Creating { db_name: ReadString::from("pending") };
                self.pending_creations.insert(index);
            }
            for index in start..end {
                self.spawn_creation(index, 1);
            }
        }
    }

    fn spawn_creation(&mut self, index: SlotIdx, attempt: usize) {
        match self.engine_io.spawn_create_database(index, self.pg_client.clone()) {
            Ok(()) => return,
            Err(IOError::FailedToStartABackgroundProcess(_)) => {
                if attempt < 3 {
                    if self
                        .engine_io
                        .send_message(EngineMessage::RetryDatabaseCreation {
                            index,
                            try_number: attempt + 1,
                        })
                        .is_ok()
                    {
                        // The retry owns the same reservation, not another
                        // creation.
                        return;
                    }
                    self.counters.non_ready_slots += 1;
                    tracing::error!(index, "unable to schedule database creation retry");
                } else {
                    self.counters.unable_to_start_database_slots += 1;
                    tracing::error!(
                        index,
                        "unable to start database creation after three attempts"
                    );
                }
            }
            Err(error) => {
                self.counters.non_ready_slots += 1;
                tracing::error!(index, %error, "unable to schedule database creation");
            }
        }

        self.pending_creations.remove(&index);
        self.slots[index] = Slot::Empty;
    }

    #[hotpath::measure]
    fn force_recycle(&mut self, lease_id: &LeaseId) -> Result<(), ForceRecycleError> {
        let Some(leased_worker) = self.leases.get(lease_id) else {
            return Err(ForceRecycleError::UnknownLease(lease_id.clone()));
        };

        let Some(worker_state) = self.slots.get_mut(leased_worker.slot_idx) else {
            return Err(ForceRecycleError::WorkerSlotNotInitialized(leased_worker.slot_idx));
        };

        // Timer cancellation cannot recall messages already in the inbox. Only
        // the owning lease may transition Leased -> Done and start replacement
        // DDL.
        if !matches!(worker_state, Slot::Leased { lease, .. } if lease == lease_id) {
            return Ok(());
        }

        let Some(database_name) = worker_state.db_name() else {
            return Err(ForceRecycleError::WorkerSlotNotInitialized(leased_worker.slot_idx));
        };

        *worker_state = Slot::Done { db_name: database_name.clone() };
        leased_worker.cancellation.cancel();
        let index = leased_worker.slot_idx;
        self.pending_creations.insert(index);
        self.retirements.insert(
            index,
            Retirement {
                lease: lease_id.clone(),
                generation: leased_worker.generation,
                database_name,
                scheduled: false,
            },
        );
        self.spawn_retirement(index);
        Ok(())
    }

    fn spawn_retirement(&mut self, index: SlotIdx) {
        let Some(job) = self.retirements.get_mut(&index) else { return };
        if job.scheduled {
            return;
        }
        match self.engine_io.spawn_recreate_database(
            index,
            job.database_name.clone(),
            job.lease.clone(),
            job.generation,
            self.pg_client.clone(),
        ) {
            Ok(()) => job.scheduled = true,
            Err(error) => {
                tracing::error!(index, %error, "retirement scheduling failed; retaining job for retry");
            }
        }
    }

    #[cfg(test)]
    pub fn snapshot<'a>(&'a self) -> &'a Self {
        self
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Default for WorkerEngineConfig {
    fn default() -> Self {
        Self {
            initial_slots: 4,
            maximum_slots: 12,
            starvation_threshold: 2,
            grow_batch_size: 4,
            cleanup_max_pending: 16,
            cleanup_concurrency: 2,

            lease_claim_timeout_ms: 30_000,
            max_lease_records: 100_000,
        }
    }
}
