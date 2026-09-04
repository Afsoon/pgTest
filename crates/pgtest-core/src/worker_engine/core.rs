use std::{collections::VecDeque, marker::PhantomData, sync::Arc, time::Instant};

use envconfig::Envconfig;
use rustc_hash::FxHashMap;
use tokio_util::sync::CancellationToken;

use crate::{
    utils::ReadString,
    worker_engine::{
        errors::{ForceRecycleError, IOError, LeaseSlotError, OfferTemplateError, RecycleError},
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

    #[envconfig(from = "PGTEST_LEASE_GRACE_MS", default = "500")]
    pub lease_grace_ms: u32,
    #[envconfig(from = "PGTEST_LEASE_CLAIM_TIMEOUT_MS", default = "30000")]
    pub lease_claim_timeout_ms: u64,
}

pub type LeaseId = ReadString;

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

    fn to_done(&mut self, database_name: ReadString) -> Self {
        Self::Done { db_name: database_name }
    }
}

pub type SlotIdx = usize;

#[cfg_attr(test, derive(Clone, Debug))]
pub(crate) struct LeaseEntry {
    pub(crate) slot_idx: SlotIdx,
    pub(crate) conns: u16,
}

#[cfg_attr(test, derive(Clone, Debug))]
pub(crate) struct PoolCapacity {
    pub(crate) maximum: u16,
    pub(crate) current: u16,
    pub(crate) free_slots: u16, // Change for VecDeque
    pub(crate) starvation_threshold: u16,
    pub growing: bool,
}

impl PoolCapacity {
    pub fn new(pool_config: WorkerEngineConfig) -> PoolCapacity {
        PoolCapacity {
            maximum: pool_config.maximum_slots,
            current: pool_config.initial_slots,
            free_slots: pool_config.initial_slots,
            starvation_threshold: pool_config.starvation_threshold,
            growing: false,
        }
    }

    pub fn is_starving(&self) -> bool {
        self.starvation_threshold >= self.free_slots
    }

    pub fn is_at_maximum_capacity(&self) -> bool {
        self.maximum == self.current
    }

    pub fn is_growing(&self) -> bool {
        self.growing == true
    }

    pub fn occupy_slot(&mut self) {
        self.free_slots = self.free_slots.saturating_sub(1);
    }

    pub fn set_pool_growing(&mut self) {
        self.growing = true;
    }

    pub fn recycle_slot(&mut self) {
        if self.free_slots == self.maximum {
            return;
        }

        self.free_slots += 1;
    }

    pub fn grow(&mut self, batch_size: u16) {
        self.current += batch_size;
        self.free_slots += batch_size;
        self.growing = false;
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
    pub(crate) capacity: PoolCapacity,
    config: WorkerEngineConfig,
    pg_client: Arc<Postgres>,
    pub(crate) ready_slots: VecDeque<SlotIdx>,
    /// Connections waiting for a free slot to lease.
    pub(crate) waiters: VecDeque<(LeaseId, Consumer, Instant)>,
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
    grace_cancellation_map: FxHashMap<LeaseId, CancellationToken>,
    lease_lifetime_cancellation_map: FxHashMap<LeaseId, CancellationToken>,
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone, Debug))]
pub struct EngineCounters {
    pub rejected_attach_grace_expired: u64,
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
            capacity,
            config: pool_worker_config.clone(), // TODO: Do we need all the config?
            pg_client: postgres_manager,
            ready_slots,
            waiters: VecDeque::new(),
            counters: EngineCounters::default(),
            engine_io,
            metrics_io: PhantomData,
            inbox,
            consumer: PhantomData,
            root_cancellation_token,
            grace_cancellation_map: FxHashMap::default(),
            lease_lifetime_cancellation_map: FxHashMap::default(),
        }
    }

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

    pub async fn run(&mut self) {
        while let Some(msg) = self.inbox.wait_for_message().await {
            match msg {
                EngineMessage::AttachOrJoin { lease: lease_id, reply, message_time } => {
                    // I can't move this line inside the `Slot::Leased` arm
                    // branch because Rust borrow checker can't
                    // infer we are modifying a property distinct from our
                    // immutable reference. self.slots it's
                    // the reason our &mut self goes to &self afterwards.
                    self.delete_grace_cancellation_token(&lease_id);
                    tracing::debug!("Attach Or Join message received");
                    match self.leases.get_mut(&lease_id) {
                        Some(lease) => {
                            let Some(worker_state) = self.slots.get(lease.slot_idx) else {
                                tracing::error!(
                                    "{lease_id} is linked to a worker that doesn't exist. \
                                     Removing leasing and replying back with failed attach"
                                );
                                self.leases.remove(&lease_id);
                                if reply.reply(ConsumerReply::FailedToAttach).is_err() {
                                    tracing::error!(
                                        "failed to reply FailedToAttach to {lease_id}; the \
                                         consumer reply channel is gone"
                                    );
                                }
                                continue;
                            };

                            match worker_state {
                                Slot::Leased { db_name, lease: _ } => {
                                    lease.conns += 1;

                                    if reply
                                        .reply(ConsumerReply::Attached {
                                            database_name: db_name.clone(),
                                        })
                                        .is_err()
                                    {
                                        tracing::error!(
                                            "failed to reply Attached to {lease_id}; the consumer \
                                             reply channel is gone"
                                        );
                                    }
                                }
                                Slot::Done { db_name: _ } => {
                                    self.counters.rejected_attach_grace_expired += 1;
                                    if reply.reply(ConsumerReply::FailedToAttach).is_err() {
                                        tracing::error!(
                                            "failed to reply FailedToAttach to {lease_id}; the \
                                             consumer reply channel is gone"
                                        );
                                    }
                                }
                                _ => {
                                    // Invariant
                                    continue;
                                }
                            }
                        }
                        None => {
                            let Some(worker_index) = self.ready_slots.pop_front() else {
                                self.waiters.push_back((lease_id, reply, message_time));
                                continue;
                            };

                            let database_name = match self.lease_slot(worker_index, &lease_id) {
                                Ok(database_name) => database_name,
                                Err(LeaseSlotError::WorkerSlotNotReady { slot, state }) => {
                                    tracing::warn!(
                                        "worker slot {slot} is not Ready; current state is {state}"
                                    );
                                    continue;
                                }
                                Err(LeaseSlotError::WorkerSlotNotInitialized(slot)) => {
                                    tracing::error!(
                                        "worker slot {slot} is not initialized; restarting the \
                                         service is recommended"
                                    );
                                    continue;
                                }
                            };

                            self.grow();
                            if reply
                                .reply(ConsumerReply::Attached {
                                    database_name: database_name.clone(),
                                })
                                .is_err()
                            {
                                tracing::error!(
                                    "failed to reply Attached to {lease_id}; the consumer reply \
                                     channel is gone"
                                );
                            }
                        }
                    }
                }
                EngineMessage::DeleteLease { lease } => {
                    self.leases.remove(&lease);
                }
                EngineMessage::TemplateCreated { index, result } => {
                    let template_name = match result {
                        Ok(template_name) => template_name,
                        Err(_error) => {
                            self.counters.template_create_failures += 1;
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
                }
                EngineMessage::RetryDatabaseCreation { index, try_number } => {
                    let pg_client = self.pg_client.clone();
                    if try_number >= 3 {
                        tracing::error!(
                            "after 3 attempts the engine could not initialize the worker at slot \
                             {index}; restarting the service is recommended over running with \
                             fewer workers than expected"
                        );
                        self.counters.unable_to_start_database_slots += 1;
                        continue;
                    }

                    match self.engine_io.spawn_create_database(index, pg_client) {
                        Ok(_) => {
                            continue;
                        }
                        Err(IOError::FailedToStartABackgroundProcess(_)) => {
                            tracing::error!(
                                "failed to start the background task that creates a fresh \
                                 database from the template"
                            );
                            let _ =
                                self.engine_io.send_message(EngineMessage::RetryDatabaseCreation {
                                    index,
                                    try_number: try_number + 1,
                                });
                        }
                        Err(error) => {
                            tracing::error!("{}", error.to_string());
                        }
                    }
                }
                EngineMessage::Detach { lease } => {
                    match self.leases.get_mut(&lease) {
                        Some(leased_worker) => {
                            leased_worker.conns = if leased_worker.conns == 0 {
                                self.counters.detach_on_zero += 1;
                                leased_worker.conns
                            } else {
                                leased_worker.conns - 1
                            };

                            if leased_worker.conns > 0 {
                                continue;
                            }

                            if self.config.lease_grace_ms > 0 {
                                let child_token = self.upsert_grace_cancellation_token(&lease);

                                let _ = self.engine_io.send_delayed_message(
                                    EngineMessage::GraceExpired { lease },
                                    self.config.lease_grace_ms,
                                    child_token,
                                );
                            } else {
                                tracing::debug!("NOT waiting so sending the message");
                                if let Err(_error) =
                                    self.engine_io.send_message(EngineMessage::GraceExpired {
                                        lease: lease.clone(),
                                    })
                                {
                                    tracing::error!(
                                        "communication with the worker engine is down and \
                                         messages are being lost; restart the service, and if it \
                                         happens again run it in debug mode"
                                    );
                                }
                            }
                        }
                        None => {
                            // TODO metrics of detaching in an unexisting
                        }
                    }
                }
                EngineMessage::GraceExpired { lease } => match self.recycle(&lease) {
                    Ok(_) => {
                        continue;
                    }
                    Err(RecycleError::ConnectionNotClosedYet(_)) => {
                        tracing::info!(
                            "stale grace message for lease {lease}: it still has open connections"
                        );
                        continue;
                    }
                    Err(RecycleError::UnknownLease(_)) => {
                        tracing::debug!(
                            "received a request to remove lease {lease}, but it does not exist"
                        );
                    }
                    Err(RecycleError::DatabaseIsDown) => {
                        tracing::error!(
                            "halting the worker engine: unable to drop or create a database in \
                             postgres"
                        );
                        panic!("Unable to communicate with postgresql")
                    }
                    Err(RecycleError::WorkerSlotNotInitialized(index)) => {
                        tracing::error!(
                            "worker slot {index} is not initialized; restarting the service is \
                             recommended"
                        );
                    }
                },
                EngineMessage::LeaseMaxTimeReached { lease } => {
                    self.counters.rejected_attach_max_lifetime += 1;
                    self.deletesert_lease_cancellation_token(&lease);

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

    fn delete_grace_cancellation_token(&mut self, lease: &LeaseId) {
        if self.grace_cancellation_map.contains_key(lease) {
            self.grace_cancellation_map.remove(lease).unwrap();
        }
    }

    fn upsert_grace_cancellation_token(&mut self, lease: &LeaseId) -> CancellationToken {
        if self.grace_cancellation_map.contains_key(lease) {
            let old_token = self.grace_cancellation_map.remove(lease).unwrap();
            old_token.cancel();
            let lease_cancel_token = self.root_cancellation_token.child_token();
            self.grace_cancellation_map.insert(lease.clone(), lease_cancel_token.clone());
            lease_cancel_token
        } else {
            let lease_cancel_token = self.root_cancellation_token.child_token();
            self.grace_cancellation_map.insert(lease.clone(), lease_cancel_token.clone());
            lease_cancel_token
        }
    }

    fn deletesert_lease_cancellation_token(&mut self, lease: &LeaseId) -> CancellationToken {
        if self.lease_lifetime_cancellation_map.contains_key(lease) {
            let token = self.lease_lifetime_cancellation_map.remove(lease).unwrap();
            token.cancel();
            token
        } else {
            let lease_cancel_token = self.root_cancellation_token.child_token();
            self.lease_lifetime_cancellation_map.insert(lease.clone(), lease_cancel_token.clone());
            lease_cancel_token
        }
    }

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
        self.capacity.occupy_slot();
        self.leases.insert(lease.clone(), LeaseEntry { slot_idx: worker_index, conns: 1 });

        let child_token = self.deletesert_lease_cancellation_token(lease);

        if self.config.lease_claim_timeout_ms > 0 {
            if self
                .engine_io
                .send_delayed_message(
                    EngineMessage::LeaseMaxTimeReached { lease: lease.clone() },
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

    fn offer_template(
        &mut self,
        database_name: ReadString,
        worker_index: SlotIdx,
    ) -> Result<(), OfferTemplateError> {
        while let Some((lease, reply, message_time)) = self.waiters.pop_front() {
            if message_time.elapsed().as_millis() > self.config.lease_claim_timeout_ms as u128 {
                self.counters.waiter_timeouts += 1;
                continue;
            }

            if let Some(leased_worker) = self.leases.get_mut(&lease) {
                let existing_database_name =
                    self.slots.get(leased_worker.slot_idx).and_then(Slot::db_name).ok_or(
                        OfferTemplateError::WorkerSlotNotInitialized(leased_worker.slot_idx),
                    )?;

                if reply
                    .reply(ConsumerReply::Attached { database_name: existing_database_name })
                    .is_err()
                {
                    tracing::error!(
                        "failed to reply Attached to waiter {lease}; the consumer reply channel \
                         is gone"
                    );
                    continue;
                }

                leased_worker.conns += 1;

                break;
            }

            let Some(worker_state) = self.slots.get_mut(worker_index) else {
                return Err(OfferTemplateError::WorkerSlotNotInitialized(worker_index));
            };

            if reply
                .reply(ConsumerReply::Attached { database_name: database_name.clone() })
                .is_err()
            {
                tracing::error!(
                    "failed to reply Attached to waiter {lease}; the consumer reply channel is \
                     gone"
                );
                continue;
            }

            *worker_state = Slot::Leased { db_name: database_name.clone(), lease: lease.clone() };
            self.capacity.occupy_slot();
            self.leases.insert(lease.clone(), LeaseEntry { slot_idx: worker_index, conns: 1 });

            return Ok(());
        }

        let Some(worker_state) = self.slots.get_mut(worker_index) else {
            return Err(OfferTemplateError::WorkerSlotNotInitialized(worker_index));
        };

        *worker_state = Slot::Ready { db_name: database_name };
        self.capacity.recycle_slot();

        self.ready_slots.push_back(worker_index);

        Ok(())
    }

    pub(crate) fn grow(&mut self) {
        if self.capacity.is_at_maximum_capacity() {
            return;
        }

        if !self.capacity.is_starving() {
            return;
        }

        if self.capacity.is_growing() {
            return;
        }

        self.capacity.set_pool_growing();
        let batch_size =
            self.config.grow_batch_size.min(self.capacity.maximum - self.capacity.current);

        for idx in self.capacity.current..(self.capacity.current + batch_size) {
            match self.slots.get_mut(idx as usize) {
                Some(slot @ Slot::Empty) => {
                    *slot = Slot::Creating { db_name: ReadString::from("pending") };
                    let pg_client = self.pg_client.clone();

                    match self.engine_io.spawn_create_database(idx as usize, pg_client) {
                        Ok(_) => {
                            continue;
                        }
                        Err(IOError::FailedToStartABackgroundProcess(_)) => {
                            tracing::error!(
                                "failed to start the background task that creates a fresh \
                                 database from the template"
                            );

                            let _ =
                                self.engine_io.send_message(EngineMessage::RetryDatabaseCreation {
                                    index: idx as usize,
                                    try_number: 1,
                                });
                        }
                        Err(error) => {
                            self.counters.non_ready_slots += 1;
                            tracing::error!("{}", error.to_string());
                        }
                    }
                }
                Some(slot) => {
                    let slot_state_name = slot.state_name();
                    tracing::warn!(
                        "The engine is trying to claim non empty worker slot. The current state \
                         is {slot_state_name}"
                    );
                }
                None => {
                    tracing::error!(
                        "The engine is trying to access a position that hasn't been preallocated. \
                         Shutting itself"
                    );
                    if let Err(_error) = self.engine_io.send_message(EngineMessage::Shutdown) {
                        tracing::error!(
                            "Unable to gracefully shutdown the engine. Executing a panic to stop"
                        );
                        panic!(
                            "Non gracefully shutdown after trying to access a non preallocated \
                             worker"
                        );
                    }
                }
            }
        }

        self.capacity.grow(batch_size);
    }

    fn force_recycle(&mut self, lease_id: &LeaseId) -> Result<(), ForceRecycleError> {
        let Some(leased_worker) = self.leases.get(lease_id) else {
            return Err(ForceRecycleError::UnknownLease(lease_id.clone()));
        };

        let Some(worker_state) = self.slots.get_mut(leased_worker.slot_idx) else {
            return Err(ForceRecycleError::WorkerSlotNotInitialized(leased_worker.slot_idx));
        };

        let Some(database_name) = worker_state.db_name() else {
            return Err(ForceRecycleError::WorkerSlotNotInitialized(leased_worker.slot_idx));
        };

        let new_state = worker_state.to_done(database_name.clone());
        *worker_state = new_state;

        let postgres_client = self.pg_client.clone();

        match self.engine_io.spawn_recreate_database(
            leased_worker.slot_idx,
            database_name,
            lease_id.clone(),
            postgres_client,
        ) {
            Ok(_) => Ok(()),
            Err(_error) => {
                tracing::error!(
                    "Unable to reset the worker leased by {lease_id}. The pool isn't working at \
                     maximum capacity."
                );
                Err(ForceRecycleError::DatabaseIsDown)
            }
        }
    }

    fn recycle(&mut self, lease_id: &LeaseId) -> Result<(), RecycleError> {
        let Some(leased_worker) = self.leases.get(lease_id) else {
            return Err(RecycleError::UnknownLease(lease_id.clone()));
        };

        if leased_worker.conns > 0 {
            return Err(RecycleError::ConnectionNotClosedYet(lease_id.clone()));
        }

        let Some(worker_state) = self.slots.get_mut(leased_worker.slot_idx) else {
            return Err(RecycleError::WorkerSlotNotInitialized(leased_worker.slot_idx));
        };

        let Some(database_name) = worker_state.db_name() else {
            return Err(RecycleError::WorkerSlotNotInitialized(leased_worker.slot_idx));
        };

        let new_state = worker_state.to_done(database_name.clone());
        *worker_state = new_state;

        let postgres_client = self.pg_client.clone();

        match self.engine_io.spawn_recreate_database(
            leased_worker.slot_idx,
            database_name,
            lease_id.clone(),
            postgres_client,
        ) {
            Ok(_) => {
                self.deletesert_lease_cancellation_token(&lease_id);
                self.upsert_grace_cancellation_token(&lease_id);
                Ok(())
            }
            Err(_error) => {
                tracing::error!(
                    "Unable to recycle the worker leased by {lease_id}. The pool isn't working at \
                     maximum capacity."
                );
                Err(RecycleError::DatabaseIsDown)
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

            lease_grace_ms: 500,
            lease_claim_timeout_ms: 30_000,
        }
    }
}
