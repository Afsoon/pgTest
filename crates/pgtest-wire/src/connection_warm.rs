use std::{
    collections::{BTreeMap, HashMap, VecDeque, hash_map::Entry},
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

use pgtest::worker_engine::database_jobs::DatabaseId;
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};
use tokio_retry2::strategy::ExponentialFactorBackoff;
use tokio_util::sync::CancellationToken;

use crate::postgres_upstream::{self, UpstreamSession};

mod attempt;
pub(crate) use attempt::WarmAttemptError;
mod scheduler;

/// Configuration for preparing fresh, single-use upstream connections.
///
/// The default disables warming, sets a global capacity of 32 connections and
/// four concurrent warm attempts, and allows five seconds for initial warm-up.
/// Startup parameters default to an empty map; the configured PostgreSQL user
/// is supplied later when resolving the warm profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionWarmConfig {
    /// Target number of spare connections per database. Zero disables warming.
    per_database: u16,
    /// Global cap on idle connections plus in-flight warm attempts.
    /// Connections already handed to clients do not count toward this cap.
    max_total: usize,
    /// Maximum number of simultaneous attempts to establish warm connections.
    concurrency: usize,
    /// Maximum initial warm-up wait before accepting clients.
    /// Zero selects background-only warming.
    startup_wait: Duration,
    /// Startup parameters for the warm profile, before resolving the default
    /// user.
    startup_params: BTreeMap<String, String>,
}

impl Default for ConnectionWarmConfig {
    fn default() -> Self {
        Self {
            per_database: 0,
            max_total: 32,
            concurrency: 4,
            startup_wait: Duration::from_secs(5),
            startup_params: BTreeMap::default(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionWarmConfigError {
    #[error("connection warm pool can't be zero")]
    ConnectionWarmPoolSizeCanNotBeZero,
    #[error("connection warm concurrency can't be zero")]
    ConnectionWarmConcurrencyCanNotBeZero,
    #[error("connection params can't contains 'database' or 'replication' params")]
    ConnectionWarmInvalidConnectionParams,
}

impl ConnectionWarmConfig {
    /// Validates and stores settings without resolving the startup profile.
    ///
    /// Returns an error if `max_total` or `concurrency` is zero, or if
    /// `startup_params` contains `database` or `replication`. These checks
    /// also apply when warming is disabled.
    ///
    /// - `per_database` sets the spare-connection target; zero disables
    ///   warming.
    /// - `max_total` caps idle connections plus in-flight warm attempts
    ///   globally, excluding connections already handed to clients.
    /// - `concurrency` limits simultaneous warm connection attempts.
    /// - `startup_wait` bounds initial warm-up; zero selects background-only
    ///   mode.
    /// - `startup_params` defines the warm profile. A missing `user` is
    ///   resolved later from the configured PostgreSQL user. An explicit user
    ///   is retained.
    ///
    /// Constructing this value does not open connections or start background
    /// work.
    pub fn try_new(
        per_database: u16,
        max_total: usize,
        concurrency: usize,
        startup_wait: Duration,
        startup_params: BTreeMap<String, String>,
    ) -> Result<Self, ConnectionWarmConfigError> {
        if max_total == 0 {
            return Err(ConnectionWarmConfigError::ConnectionWarmPoolSizeCanNotBeZero);
        }

        if concurrency == 0 {
            return Err(ConnectionWarmConfigError::ConnectionWarmConcurrencyCanNotBeZero);
        }

        if startup_params.contains_key("database") || startup_params.contains_key("replication") {
            return Err(ConnectionWarmConfigError::ConnectionWarmInvalidConnectionParams);
        }

        Ok(Self { per_database, max_total, concurrency, startup_params, startup_wait })
    }

    pub(crate) fn resolve_profile(&self, default_user: &str) -> WarmStartupProfile {
        let mut startup_profile_params = self.startup_params.clone();
        startup_profile_params.entry("user".to_owned()).or_insert_with(|| default_user.to_owned());
        WarmStartupProfile { connection_params: startup_profile_params }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.per_database != 0
    }

    fn attempt_limit(&self) -> usize {
        self.concurrency.min(self.max_total).min(Semaphore::MAX_PERMITS)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WarmStartupProfile {
    connection_params: BTreeMap<String, String>,
}

impl WarmStartupProfile {
    pub(crate) fn parameters(&self) -> &BTreeMap<String, String> {
        return &self.connection_params;
    }

    pub(crate) fn matches(&self, client_params: &BTreeMap<String, String>) -> bool {
        if client_params.contains_key("replication") {
            return false;
        }

        self.connection_params == postgres_upstream::forwardable(client_params)
    }
}

pub(crate) struct ConnectionWarmPool {
    config: ConnectionWarmConfig,
    profile: WarmStartupProfile,
    state: Mutex<WarmPoolState>,
    attempt_permits: Semaphore,
    cancellation: CancellationToken,
    changed: Notify,
    scheduler_running: AtomicBool,
}

#[derive(Default)]
struct WarmPoolState {
    schedule_order: VecDeque<DatabaseId>,
    databases: HashMap<DatabaseId, DatabaseWarmState>,
    capacity_used: usize,
}

struct DatabaseWarmState {
    database_name: String,
    idle: VecDeque<UpstreamSession>,
    in_flight: usize,
    retry_at: Option<Instant>,
    retry_strategy: ExponentialFactorBackoff,
    retiring: bool,
    cancellation: CancellationToken,
}

fn warm_retry_strategy() -> ExponentialFactorBackoff {
    ExponentialFactorBackoff::from_millis(1_000, 2.0).max_delay(Duration::from_secs(30))
}

impl ConnectionWarmPool {
    pub(crate) fn new(config: ConnectionWarmConfig, default_user: &str) -> Self {
        let profile = config.resolve_profile(default_user);
        // Attempts cannot exceed reserved capacity. Capping at Tokio's limit
        // also keeps very large, otherwise valid settings from panicking.
        let attempt_permits = Semaphore::new(config.attempt_limit());
        Self {
            config,
            profile,
            state: Mutex::new(WarmPoolState::default()),
            attempt_permits,
            cancellation: CancellationToken::new(),
            changed: Notify::new(),
            scheduler_running: AtomicBool::new(false),
        }
    }

    pub(crate) fn register_database(&self, database_id: DatabaseId, database_name: String) -> bool {
        if !self.config.is_enabled() {
            return false;
        }

        let Ok(mut state_lock) = self.state.lock() else {
            tracing::error!("lock poisoned during register database");
            return false;
        };

        if self.cancellation.is_cancelled() {
            return false;
        }

        let registered = match state_lock.databases.entry(database_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(DatabaseWarmState {
                    database_name,
                    idle: VecDeque::default(),
                    in_flight: 0,
                    retry_at: None,
                    retry_strategy: warm_retry_strategy(),
                    retiring: false,
                    cancellation: self.cancellation.child_token(),
                });
                state_lock.schedule_order.push_back(database_id);
                true
            }
        };
        drop(state_lock);
        if registered {
            self.changed.notify_one();
        }
        registered
    }

    /// Disable this database and cancel its warm work. This is a notification
    /// of logical retirement, not an acknowledgement that attempts have
    /// drained.
    pub(crate) fn retire_database(&self, database_id: DatabaseId) -> bool {
        let Ok(mut state) = self.state.lock() else {
            tracing::error!("lock poisoned during database retirement");
            return false;
        };
        let current_capacity = state.capacity_used;
        let Some(entry) = state.databases.get_mut(&database_id) else {
            return false;
        };
        if entry.retiring {
            return false;
        }

        let remaining_capacity = current_capacity
            .checked_sub(entry.idle.len())
            .expect("idle sessions must occupy capacity");
        entry.retiring = true;
        entry.retry_at = None;
        let cancellation = entry.cancellation.clone();
        let idle = std::mem::take(&mut entry.idle);
        state.capacity_used = remaining_capacity;
        state.schedule_order.retain(|id| *id != database_id);
        // Retain the entry so outstanding reservations can settle and duplicate
        // registration cannot reactivate the same physical identity. Removal
        // will be coordinated with the later drain-before-delete barrier.
        drop(state);
        cancellation.cancel();
        drop(idle);
        self.changed.notify_one();
        true
    }

    fn reserve_locked(
        self: &Arc<Self>,
        state: &mut WarmPoolState,
        database_id: DatabaseId,
    ) -> Option<WarmReservation> {
        if self.cancellation.is_cancelled() || state.capacity_used >= self.config.max_total {
            return None;
        }

        let Some(entry) = state.databases.get_mut(&database_id) else {
            return None;
        };

        if entry.retiring || entry.cancellation.is_cancelled() {
            return None;
        }

        if entry.retry_at.is_some_and(|deadline| Instant::now() < deadline) {
            return None;
        }

        if entry.idle.len() + entry.in_flight >= usize::from(self.config.per_database) {
            return None;
        }

        entry.in_flight = entry.in_flight.saturating_add(1);
        let database_name = entry.database_name.clone();
        let cancellation = entry.cancellation.clone();
        state.capacity_used = state.capacity_used.saturating_add(1);

        Some(WarmReservation {
            pool: self.clone(),
            database_id,
            database_name,
            cancellation,
            active: true,
        })
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        database_id: DatabaseId,
    ) -> Option<WarmReservation> {
        if !self.config.is_enabled() {
            return None;
        }

        let Ok(mut state_lock) = self.state.lock() else {
            tracing::error!("lock poisoned during try reserve");
            return None;
        };

        self.reserve_locked(&mut state_lock, database_id)
    }

    /// Reserve one slot in round-robin registration order, scanning at most
    /// one pass and skipping databases whose spare target is already covered.
    pub(crate) fn reserve_next(self: &Arc<Self>) -> Option<WarmReservation> {
        if !self.config.is_enabled() {
            return None;
        }

        let Ok(mut state_lock) = self.state.lock() else {
            tracing::error!("lock poisoned during reserve next");
            return None;
        };

        if state_lock.capacity_used >= self.config.max_total {
            return None;
        }

        let current_queue_size = state_lock.schedule_order.len();

        for _ in 0..current_queue_size {
            let database_id = state_lock.schedule_order.pop_front()?;
            if !state_lock.databases.contains_key(&database_id) {
                continue;
            }

            state_lock.schedule_order.push_back(database_id);
            if let Some(reservation) = self.reserve_locked(&mut state_lock, database_id) {
                return Some(reservation);
            }
        }

        None
    }

    pub(crate) fn try_checkout(
        &self,
        database_id: DatabaseId,
        client_params: &BTreeMap<String, String>,
    ) -> Option<UpstreamSession> {
        if !self.config.is_enabled() {
            return None;
        }

        if !self.profile.matches(client_params) {
            return None;
        }

        let Ok(mut state_lock) = self.state.lock() else {
            tracing::error!("lock poisoned during checkout");
            return None;
        };

        let current_capacity = state_lock.capacity_used;

        if self.cancellation.is_cancelled() {
            return None;
        }

        let Some(entry) = state_lock.databases.get_mut(&database_id) else {
            return None;
        };

        if entry.retiring || entry.cancellation.is_cancelled() || entry.idle.is_empty() {
            return None;
        }

        let remaining_capacity =
            current_capacity.checked_sub(1).expect("idle session must occupy capacity");

        let warmed_session = entry.idle.pop_front().expect("expected to obtain a session ready");
        state_lock.capacity_used = remaining_capacity;
        drop(state_lock);
        self.changed.notify_one();

        Some(warmed_session)
    }
}

#[must_use]
pub(crate) struct WarmReservation {
    pool: Arc<ConnectionWarmPool>,
    database_id: DatabaseId,
    database_name: String,
    cancellation: CancellationToken,
    active: bool,
}

impl WarmReservation {
    pub(crate) fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Establish and publish one fresh session, consuming this reservation.
    ///
    /// The reservation captures database/pool cancellation; the caller may
    /// additionally cancel this attempt. Dropping this future releases its
    /// permit, reservation, and partially connected socket. Failures update
    /// retry eligibility before releasing capacity; scheduling the next
    /// attempt belongs to the caller.
    pub(crate) async fn establish(
        self,
        upstream_host: &str,
        upstream_port: u16,
        cancellation: &CancellationToken,
    ) -> Result<bool, WarmAttemptError> {
        // Keep the pool alive independently of self so publication can consume
        // the reservation while the borrowed permit remains held.
        let pool = self.pool.clone();
        let _permit = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => return Err(WarmAttemptError::Cancelled),
            _ = cancellation.cancelled() => return Err(WarmAttemptError::Cancelled),
            permit = pool.attempt_permits.acquire() => {
                permit.map_err(|_| WarmAttemptError::ConcurrencyClosed)?
            }
        };

        let session = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => return Err(WarmAttemptError::Cancelled),
            result = attempt::connect_warm_session(
                self.database_name(),
                &pool.profile,
                upstream_host,
                upstream_port,
                cancellation,
            ) => result,
        }
        .inspect_err(|error| {
            if matches!(error, WarmAttemptError::TimedOut | WarmAttemptError::Upstream(_)) {
                self.record_failure();
            }
        })?;

        // Publication also checks retirement under the state lock, covering
        // cancellation that races with a successful connection attempt.
        if cancellation.is_cancelled() || self.cancellation.is_cancelled() {
            return Err(WarmAttemptError::Cancelled);
        }
        Ok(self.publish(session))
    }

    fn record_failure(&self) {
        let Ok(mut state) = self.pool.state.lock() else {
            tracing::error!("lock poisoned while recording warm attempt failure");
            return;
        };
        let entry = state
            .databases
            .get_mut(&self.database_id)
            .expect("reserved database must remain registered");
        if self.pool.cancellation.is_cancelled()
            || entry.retiring
            || entry.cancellation.is_cancelled()
        {
            return;
        }
        let delay = entry.retry_strategy.next().expect("exponential backoff is unbounded");
        entry.retry_at = Some(Instant::now() + delay);
        drop(state);
        self.pool.changed.notify_one();
    }

    pub(crate) fn publish(mut self, session: UpstreamSession) -> bool {
        if !self.active {
            return false;
        }

        let Ok(mut state_lock) = self.pool.state.lock() else {
            tracing::error!("lock poisoned during publish");
            return false;
        };

        assert!(state_lock.capacity_used > 0, "reservation must occupy capacity");

        let entry = state_lock
            .databases
            .get_mut(&self.database_id)
            .expect("reserved database must remain registered");

        let update_in_flight =
            entry.in_flight.checked_sub(1).expect("reservation must be in flight");

        if self.pool.cancellation.is_cancelled()
            || entry.retiring
            || entry.cancellation.is_cancelled()
        {
            // Release the mutex before the rejected session and the active
            // reservation are dropped. Reservation Drop settles its slot once.
            drop(state_lock);
            return false;
        }

        entry.idle.push_back(session);
        entry.in_flight = update_in_flight;
        entry.retry_at = None;
        entry.retry_strategy = warm_retry_strategy();
        self.active = false;
        drop(state_lock);
        self.pool.changed.notify_one();

        true
    }
}

impl Drop for WarmReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let Ok(mut state_lock) = self.pool.state.lock() else {
            tracing::error!("lock poisoned during drop warm reservation");
            return;
        };

        let capacity_used =
            state_lock.capacity_used.checked_sub(1).expect("reservation must occupy capacity");

        let entry = state_lock
            .databases
            .get_mut(&self.database_id)
            .expect("reserved database must remain registered");

        let in_flight = entry.in_flight.checked_sub(1).expect("reservation must be in flight");
        entry.in_flight = in_flight;
        state_lock.capacity_used = capacity_used;
        drop(state_lock);
        self.pool.changed.notify_one();
    }
}

#[cfg(test)]
mod publication_tests;

#[cfg(test)]
mod attempt_tests;

#[cfg(test)]
mod scheduler_tests;

#[cfg(test)]
mod checkout_tests;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;

    mod reservations {
        use std::{
            panic::{AssertUnwindSafe, catch_unwind},
            sync::{Arc, Barrier},
        };

        use super::*;

        fn pool(per_database: u16, max_total: usize) -> Arc<ConnectionWarmPool> {
            // Concurrency governs socket attempts later, not reservation
            // capacity.
            let config = ConnectionWarmConfig::try_new(
                per_database,
                max_total,
                1,
                Duration::ZERO,
                BTreeMap::new(),
            )
            .unwrap();
            let pool = Arc::new(ConnectionWarmPool::new(config, "postgres"));
            for (id, name) in [(1, "physical_One_雪"), (2, "physical_two")] {
                assert_eq!(
                    pool.register_database(DatabaseId(id), name.to_owned()),
                    per_database > 0
                );
            }
            pool
        }

        fn assert_counts(pool: &ConnectionWarmPool, total: usize, expected: &[(u64, usize)]) {
            let state = pool.state.try_lock().expect("a reservation must not retain the mutex");
            assert_eq!(state.capacity_used, total);
            assert_eq!(state.databases.len(), expected.len());
            for &(id, in_flight) in expected {
                let database = &state.databases[&DatabaseId(id)];
                assert_eq!(database.in_flight, in_flight, "database {id}");
                assert!(database.idle.is_empty());
            }
            assert_eq!(total, expected.iter().map(|(_, count)| count).sum::<usize>());
        }

        #[test]
        fn disabled_and_unknown_databases_do_not_reserve_capacity() {
            let disabled = pool(0, 3);
            assert!(disabled.try_reserve(DatabaseId(1)).is_none());
            assert_counts(&disabled, 0, &[]);

            let enabled = pool(2, 3);
            assert!(enabled.try_reserve(DatabaseId(99)).is_none());
            assert_counts(&enabled, 0, &[(1, 0), (2, 0)]);
            let reservation = enabled.try_reserve(DatabaseId(1)).unwrap();
            assert!(enabled.try_reserve(DatabaseId(99)).is_none());
            assert_counts(&enabled, 1, &[(1, 1), (2, 0)]);
            drop(reservation);
            assert_counts(&enabled, 0, &[(1, 0), (2, 0)]);
        }

        #[test]
        fn reservation_owns_the_registered_name_and_releases_on_drop() {
            let pool = pool(2, 4);
            let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
            assert_eq!(reservation.database_name(), "physical_One_雪");
            assert!(!pool.register_database(DatabaseId(1), "replacement".to_owned()));
            assert_eq!(reservation.database_name(), "physical_One_雪");
            assert_counts(&pool, 1, &[(1, 1), (2, 0)]);

            let moved_reservation = reservation;
            drop(moved_reservation);
            assert_counts(&pool, 0, &[(1, 0), (2, 0)]);
            assert_eq!(
                pool.state.lock().unwrap().databases[&DatabaseId(1)].database_name,
                "physical_One_雪"
            );
        }

        #[test]
        fn per_database_limit_preserves_other_databases_and_allows_reuse() {
            let pool = pool(2, 8);
            let first = pool.try_reserve(DatabaseId(1)).unwrap();
            let second = pool.try_reserve(DatabaseId(1)).unwrap();
            assert!(pool.try_reserve(DatabaseId(1)).is_none());
            assert_counts(&pool, 2, &[(1, 2), (2, 0)]);
            let other = pool.try_reserve(DatabaseId(2)).unwrap();
            assert_counts(&pool, 3, &[(1, 2), (2, 1)]);

            drop(first);
            assert_counts(&pool, 2, &[(1, 1), (2, 1)]);
            let replacement = pool.try_reserve(DatabaseId(1)).unwrap();
            assert!(pool.try_reserve(DatabaseId(1)).is_none());
            assert_counts(&pool, 3, &[(1, 2), (2, 1)]);
            drop((second, replacement));
            assert_counts(&pool, 1, &[(1, 0), (2, 1)]);
            drop(other);
            assert_counts(&pool, 0, &[(1, 0), (2, 0)]);
        }

        #[test]
        fn global_limit_is_shared_across_databases_and_reusable() {
            let pool = pool(4, 2);
            let first = pool.try_reserve(DatabaseId(1)).unwrap();
            let second = pool.try_reserve(DatabaseId(2)).unwrap();
            for id in [1, 2] {
                assert!(pool.try_reserve(DatabaseId(id)).is_none());
                assert_counts(&pool, 2, &[(1, 1), (2, 1)]);
            }
            drop(first);
            assert_counts(&pool, 1, &[(1, 0), (2, 1)]);
            let replacement = pool.try_reserve(DatabaseId(2)).unwrap();
            assert_counts(&pool, 2, &[(1, 0), (2, 2)]);
            assert!(pool.try_reserve(DatabaseId(1)).is_none());
            drop((second, replacement));
            assert_counts(&pool, 0, &[(1, 0), (2, 0)]);
        }

        #[test]
        fn reservation_keeps_pool_alive_until_it_is_dropped() {
            let pool = pool(1, 1);
            let weak = Arc::downgrade(&pool);
            let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
            drop(pool);
            let retained = weak.upgrade().expect("reservation must own the pool");
            assert_counts(&retained, 1, &[(1, 1), (2, 0)]);
            drop(retained);
            drop(reservation);
            assert!(weak.upgrade().is_none(), "reservation must not leave an Arc cycle");
        }

        #[test]
        fn scope_unwinding_releases_the_reservation() {
            let pool = pool(1, 1);
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _reservation = pool.try_reserve(DatabaseId(1)).unwrap();
                assert_counts(&pool, 1, &[(1, 1), (2, 0)]);
                panic!("simulate an abandoned warm attempt");
            }));
            assert!(result.is_err());
            assert_counts(&pool, 0, &[(1, 0), (2, 0)]);
            let replacement = pool.try_reserve(DatabaseId(1)).unwrap();
            drop(replacement);
            assert_counts(&pool, 0, &[(1, 0), (2, 0)]);
        }

        #[test]
        fn inactive_reservation_does_not_release_capacity_again() {
            let pool = pool(2, 3);
            let mut settled = pool.try_reserve(DatabaseId(1)).unwrap();
            let outstanding = pool.try_reserve(DatabaseId(2)).unwrap();
            {
                // Simulate prior settlement until the consuming publication API
                // exists.
                let mut state = pool.state.lock().unwrap();
                state.databases.get_mut(&DatabaseId(1)).unwrap().in_flight -= 1;
                state.capacity_used -= 1;
            }
            settled.active = false;
            drop(settled);
            assert_counts(&pool, 1, &[(1, 0), (2, 1)]);
            drop(outstanding);
            assert_counts(&pool, 0, &[(1, 0), (2, 0)]);
        }

        #[test]
        fn drop_rejects_missing_database_without_decrementing_global_capacity() {
            let pool = pool(2, 3);
            let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
            pool.state.lock().unwrap().databases.remove(&DatabaseId(1));

            let result = catch_unwind(AssertUnwindSafe(|| drop(reservation)));
            assert!(result.is_err(), "a live reservation requires its database entry");
            // Only tests inspect poisoned state, to verify the failed
            // transition made no changes. Production code must
            // continue to reject it.
            let state = pool.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(state.capacity_used, 1);
            assert_eq!(state.databases.len(), 1);
            assert!(!state.databases.contains_key(&DatabaseId(1)));
            assert_eq!(state.databases[&DatabaseId(2)].in_flight, 0);
        }

        #[test]
        fn drop_rejects_global_underflow_without_decrementing_database_count() {
            let pool = pool(2, 3);
            let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
            pool.state.lock().unwrap().capacity_used = 0;

            let result = catch_unwind(AssertUnwindSafe(|| drop(reservation)));
            assert!(result.is_err(), "global capacity underflow must not be saturated");
            // Inspect the deliberately broken state after the caught panic.
            let state = pool.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(state.capacity_used, 0);
            assert_eq!(state.databases.len(), 2);
            assert_eq!(state.databases[&DatabaseId(1)].in_flight, 1);
            assert_eq!(state.databases[&DatabaseId(2)].in_flight, 0);
        }

        #[test]
        fn drop_rejects_database_underflow_without_decrementing_global_capacity() {
            let pool = pool(2, 3);
            let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
            pool.state.lock().unwrap().databases.get_mut(&DatabaseId(1)).unwrap().in_flight = 0;

            let result = catch_unwind(AssertUnwindSafe(|| drop(reservation)));
            assert!(result.is_err(), "database in-flight underflow must not be saturated");
            // A checked global decrement must not be committed before the
            // database count has also been validated.
            let state = pool.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(state.capacity_used, 1);
            assert_eq!(state.databases.len(), 2);
            assert_eq!(state.databases[&DatabaseId(1)].in_flight, 0);
            assert_eq!(state.databases[&DatabaseId(2)].in_flight, 0);
        }

        #[test]
        fn poisoned_pool_rejects_reservations_and_drop_does_not_panic() {
            let pool = pool(2, 3);
            let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
            let poisoned = catch_unwind(AssertUnwindSafe(|| {
                let _guard = pool.state.lock().unwrap();
                panic!("simulate a panic during a state transition");
            }));
            // Catch both operations before asserting so a failing Drop cannot
            // double-panic while a failed assertion is already unwinding.
            let attempt =
                catch_unwind(AssertUnwindSafe(|| pool.try_reserve(DatabaseId(2)).is_none()));
            let release = catch_unwind(AssertUnwindSafe(|| drop(reservation)));
            assert!(poisoned.is_err());
            assert!(pool.state.is_poisoned());
            assert!(matches!(attempt, Ok(true)));
            assert!(release.is_ok());
        }

        #[test]
        fn simultaneous_reservations_cannot_exceed_either_limit() {
            const CALLERS: usize = 8;
            // First isolate the global cap, then the per-database target.
            for (per_database, max_total) in [(8, 1), (1, 8)] {
                let pool = pool(per_database, max_total);
                let start = Barrier::new(CALLERS + 1);
                let reservations = std::thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for _ in 0..CALLERS {
                        let pool = Arc::clone(&pool);
                        let start = &start;
                        handles.push(scope.spawn(move || {
                            start.wait();
                            pool.try_reserve(DatabaseId(1))
                        }));
                    }
                    start.wait();
                    // Successful results stay owned by the handles or this
                    // vector until every caller has
                    // finished, preventing early slot reuse.
                    handles
                        .into_iter()
                        .filter_map(|handle| handle.join().unwrap())
                        .collect::<Vec<_>>()
                });
                assert_eq!(reservations.len(), 1, "target={per_database}, cap={max_total}");
                assert_counts(&pool, 1, &[(1, 1), (2, 0)]);
                drop(reservations);
                assert_counts(&pool, 0, &[(1, 0), (2, 0)]);
            }
        }
    }

    fn registration_pool() -> ConnectionWarmPool {
        let config =
            ConnectionWarmConfig::try_new(2, 1, 1, Duration::ZERO, BTreeMap::new()).unwrap();
        ConnectionWarmPool::new(config, "postgres")
    }

    #[test]
    fn disabled_pool_does_not_register_databases() {
        let pool = ConnectionWarmPool::new(ConnectionWarmConfig::default(), "postgres");
        for id in [DatabaseId(1), DatabaseId(2)] {
            assert!(!pool.register_database(id, "physical_database".to_owned()));
        }
        let state = pool.state.lock().unwrap();
        assert!(state.databases.is_empty());
        assert_eq!(state.capacity_used, 0);
    }

    #[test]
    fn registration_stores_exact_identity_and_name_with_empty_state() {
        let pool = registration_pool();
        let id = DatabaseId(42);
        let name = "Template_MixedCase_雪";
        assert!(pool.register_database(id, name.to_owned()));

        let state = pool.state.lock().unwrap();
        assert_eq!(state.databases.len(), 1);
        let database = state.databases.get(&id).unwrap();
        assert_eq!(database.database_name, name);
        assert!(database.idle.is_empty());
        assert_eq!(database.in_flight, 0);
        assert_eq!(state.capacity_used, 0);
    }

    #[test]
    fn duplicate_registration_preserves_existing_name_and_accounting() {
        let pool = registration_pool();
        let id = DatabaseId(7);
        assert!(pool.register_database(id, "original_database".to_owned()));
        {
            // Seed an outstanding reservation until the reservation API exists.
            let mut state = pool.state.lock().unwrap();
            state.databases.get_mut(&id).unwrap().in_flight = 1;
            state.capacity_used = 1;
        }

        for name in ["original_database", "replacement_database"] {
            assert!(!pool.register_database(id, name.to_owned()));
            let state = pool.state.lock().unwrap();
            assert_eq!(state.databases.len(), 1);
            let database = state.databases.get(&id).unwrap();
            assert_eq!(database.database_name, "original_database");
            assert!(database.idle.is_empty());
            assert_eq!(database.in_flight, 1);
            assert_eq!(state.capacity_used, 1);
        }
    }

    #[test]
    fn distinct_database_ids_remain_independent_even_with_equal_names() {
        let pool = registration_pool();
        assert!(pool.register_database(DatabaseId(1), "same_name".to_owned()));
        assert!(pool.register_database(DatabaseId(2), "same_name".to_owned()));
        assert!(pool.register_database(DatabaseId(3), "different_name".to_owned()));

        let state = pool.state.lock().unwrap();
        assert_eq!(state.databases.len(), 3);
        for (id, name) in [(1, "same_name"), (2, "same_name"), (3, "different_name")] {
            let database = state.databases.get(&DatabaseId(id)).unwrap();
            assert_eq!(database.database_name, name);
            assert!(database.idle.is_empty());
            assert_eq!(database.in_flight, 0);
        }
        assert_eq!(state.capacity_used, 0);
    }

    #[test]
    fn registration_does_not_consume_capacity_or_require_a_free_connection_slot() {
        let pool = registration_pool();
        assert_eq!(pool.config.max_total, 1);
        for id in 1..=3 {
            assert!(pool.register_database(DatabaseId(id), format!("database_{id}")));
            assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
        }
        {
            // Metadata registration must still work when connection capacity is
            // full.
            let mut state = pool.state.lock().unwrap();
            state.databases.get_mut(&DatabaseId(1)).unwrap().in_flight = 1;
            state.capacity_used = 1;
        }
        assert!(pool.register_database(DatabaseId(4), "database_4".to_owned()));

        let state = pool.state.lock().unwrap();
        assert_eq!(state.databases.len(), 4);
        assert_eq!(state.capacity_used, 1);
        assert_eq!(state.databases[&DatabaseId(1)].in_flight, 1);
        let database = &state.databases[&DatabaseId(4)];
        assert!(database.idle.is_empty());
        assert_eq!(database.in_flight, 0);
    }

    #[test]
    fn default_pool_is_disabled_and_empty_with_a_resolved_user() {
        let config = ConnectionWarmConfig::default();
        let pool = ConnectionWarmPool::new(config.clone(), "postgres");

        assert_eq!(pool.config, config);
        assert!(!pool.config.is_enabled());
        assert_eq!(
            pool.profile.parameters(),
            &BTreeMap::from([("user".to_owned(), "postgres".to_owned())])
        );
        let state = pool.state.lock().unwrap();
        assert!(state.databases.is_empty());
        assert_eq!(state.capacity_used, 0);
    }

    #[test]
    fn enabled_pool_retains_settings_and_starts_empty() {
        let params = BTreeMap::from([
            ("application_name".to_owned(), "vitest".to_owned()),
            ("options".to_owned(), "-c search_path=public".to_owned()),
        ]);
        for wait in [Duration::ZERO, Duration::from_millis(250)] {
            let config = ConnectionWarmConfig::try_new(8, 2, 1, wait, params.clone()).unwrap();
            let pool = ConnectionWarmPool::new(config.clone(), "test_user");

            assert_eq!(pool.config, config);
            assert!(pool.config.is_enabled());
            let mut expected = params.clone();
            expected.insert("user".to_owned(), "test_user".to_owned());
            assert_eq!(pool.profile.parameters(), &expected);
            let state = pool.state.lock().unwrap();
            assert!(state.databases.is_empty());
            assert_eq!(state.capacity_used, 0);
        }
    }

    #[test]
    fn pool_constructor_preserves_explicit_users_even_when_disabled() {
        for count in [0, 1] {
            for user in ["explicit_user", ""] {
                let params = BTreeMap::from([
                    ("user".to_owned(), user.to_owned()),
                    ("application_name".to_owned(), String::new()),
                ]);
                let config =
                    ConnectionWarmConfig::try_new(count, 2, 1, Duration::ZERO, params.clone())
                        .unwrap();
                let pool = ConnectionWarmPool::new(config.clone(), "postgres");

                assert_eq!(pool.config, config);
                assert_eq!(pool.profile.parameters(), &params);
                let state = pool.state.lock().unwrap();
                assert!(state.databases.is_empty());
                assert_eq!(state.capacity_used, 0);
            }
        }
    }

    fn matching_profile() -> WarmStartupProfile {
        let params = BTreeMap::from([
            ("user".to_owned(), "test_user".to_owned()),
            ("application_name".to_owned(), "vitest".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("options".to_owned(), "-c search_path=public".to_owned()),
        ]);
        ConnectionWarmConfig::try_new(1, 32, 4, Duration::from_secs(5), params)
            .unwrap()
            .resolve_profile("postgres")
    }

    #[test]
    fn warming_is_enabled_only_for_a_positive_target() {
        assert!(!ConnectionWarmConfig::default().is_enabled());
        for count in [0, 1, u16::MAX] {
            for wait in [Duration::ZERO, Duration::from_secs(5)] {
                let config =
                    ConnectionWarmConfig::try_new(count, 1, 1, wait, BTreeMap::new()).unwrap();
                assert_eq!(config.is_enabled(), count > 0, "count={count}, wait={wait:?}");
            }
        }
    }

    #[test]
    fn exact_profiles_match_independently_of_the_routed_database() {
        for profile in
            [ConnectionWarmConfig::default().resolve_profile("postgres"), matching_profile()]
        {
            assert!(profile.matches(profile.parameters()));
            for database in ["template/lease_a", "template/lease_b", "other_template/lease"] {
                let mut client = profile.parameters().clone();
                client.insert("database".to_owned(), database.to_owned());
                assert!(profile.matches(&client), "database={database}");
            }
        }
    }

    #[test]
    fn missing_or_changed_forwarded_parameters_do_not_match() {
        let profile = matching_profile();
        for (key, changed_value) in [
            ("user", "TEST_USER"),
            ("application_name", "Vitest"),
            ("client_encoding", "utf8"),
            ("options", "-c  search_path=public"),
        ] {
            let mut client = profile.parameters().clone();
            client.insert("database".to_owned(), "template/lease".to_owned());
            client.remove(key);
            assert!(!profile.matches(&client), "missing {key}");
            client.insert(key.to_owned(), changed_value.to_owned());
            assert!(!profile.matches(&client), "changed {key}");
        }
    }

    #[test]
    fn extra_forwarded_parameters_do_not_match_even_when_empty() {
        let profile = matching_profile();
        for key in ["extra", "User", "Database", "Replication"] {
            let mut client = profile.parameters().clone();
            client.insert(key.to_owned(), String::new());
            assert!(!profile.matches(&client), "extra key {key}");
        }

        let minimal = ConnectionWarmConfig::default().resolve_profile("postgres");
        let mut client = minimal.parameters().clone();
        client.insert("application_name".to_owned(), String::new());
        assert!(!minimal.matches(&client));
    }

    #[test]
    fn matching_never_supplies_a_missing_client_user() {
        for user in ["test_user", ""] {
            let config = ConnectionWarmConfig::try_new(
                1,
                32,
                4,
                Duration::ZERO,
                BTreeMap::from([("user".to_owned(), user.to_owned())]),
            )
            .unwrap();
            let profile = config.resolve_profile("postgres");
            let mut client = BTreeMap::from([("database".to_owned(), "template/lease".to_owned())]);
            assert!(!profile.matches(&client), "missing user must not match {user:?}");
            client.insert("user".to_owned(), String::new());
            assert_eq!(profile.matches(&client), user.is_empty());
            client.insert("user".to_owned(), user.to_owned());
            assert!(profile.matches(&client));
        }
    }

    #[test]
    fn replication_presence_never_matches_regardless_of_value() {
        let profile = matching_profile();
        for value in ["", "false", "0", "true", "database"] {
            let mut client = profile.parameters().clone();
            client.insert("database".to_owned(), "template/lease".to_owned());
            client.insert("replication".to_owned(), value.to_owned());
            assert!(!profile.matches(&client), "replication={value:?}");
        }
    }

    #[test]
    fn matching_leaves_client_parameters_and_profile_unchanged() {
        let profile = matching_profile();
        let original_profile = profile.clone();
        for mismatch in [false, true] {
            let mut client = profile.parameters().clone();
            client.insert("database".to_owned(), "template/lease".to_owned());
            if mismatch {
                client.insert("application_name".to_owned(), "another_client".to_owned());
            }
            let original_client = client.clone();
            assert_eq!(profile.matches(&client), !mismatch);
            assert_eq!(client, original_client);
            assert_eq!(profile, original_profile);
        }
    }

    #[test]
    fn empty_profile_resolves_to_only_the_default_user() {
        let config = ConnectionWarmConfig::default();
        let profile = config.resolve_profile("postgres");
        assert_eq!(
            profile.parameters(),
            &BTreeMap::from([("user".to_owned(), "postgres".to_owned())])
        );
        assert_eq!(config, ConnectionWarmConfig::default());
    }

    #[test]
    fn resolving_preserves_explicit_users_and_all_parameters() {
        for user in ["test_user", ""] {
            let params = BTreeMap::from([
                ("user".to_owned(), user.to_owned()),
                ("application_name".to_owned(), "vitest".to_owned()),
                ("options".to_owned(), "-c search_path=public".to_owned()),
                ("client_encoding".to_owned(), String::new()),
            ]);
            let config =
                ConnectionWarmConfig::try_new(1, 32, 4, Duration::from_secs(5), params.clone())
                    .unwrap();
            let original = config.clone();

            for default_user in ["postgres", "another_user"] {
                let profile = config.resolve_profile(default_user);
                assert_eq!(profile.parameters(), &params, "explicit user {user:?}");
            }
            assert_eq!(config, original);
        }
    }

    #[test]
    fn resolving_with_different_defaults_does_not_mutate_config_or_prior_profile() {
        let params = BTreeMap::from([
            ("application_name".to_owned(), String::new()),
            ("options".to_owned(), "-c search_path=public".to_owned()),
        ]);
        let config =
            ConnectionWarmConfig::try_new(1, 32, 4, Duration::from_secs(5), params.clone())
                .unwrap();
        let original = config.clone();
        let first = config.resolve_profile("first_user");
        let second = config.resolve_profile("second_user");

        let mut expected_first = params.clone();
        expected_first.insert("user".to_owned(), "first_user".to_owned());
        let mut expected_second = params;
        expected_second.insert("user".to_owned(), "second_user".to_owned());
        assert_eq!(first.parameters(), &expected_first);
        assert_eq!(second.parameters(), &expected_second);
        assert_eq!(config, original);
    }

    #[test]
    fn zero_global_capacity_is_rejected_even_when_warming_is_disabled() {
        for per_database in [0, 1] {
            let result = ConnectionWarmConfig::try_new(
                per_database,
                0,
                4,
                Duration::from_secs(5),
                BTreeMap::new(),
            );
            assert!(
                matches!(
                    result,
                    Err(ConnectionWarmConfigError::ConnectionWarmPoolSizeCanNotBeZero)
                ),
                "per_database={per_database}: {result:?}",
            );
        }
    }

    #[test]
    fn zero_concurrency_is_rejected_even_when_warming_is_disabled() {
        for per_database in [0, 1] {
            let result = ConnectionWarmConfig::try_new(
                per_database,
                32,
                0,
                Duration::from_secs(5),
                BTreeMap::new(),
            );
            assert!(
                matches!(
                    result,
                    Err(ConnectionWarmConfigError::ConnectionWarmConcurrencyCanNotBeZero)
                ),
                "per_database={per_database}: {result:?}",
            );
        }
    }

    #[test]
    fn reserved_parameters_are_rejected_even_when_warming_is_disabled() {
        for per_database in [0, 1] {
            for parameter in ["database", "replication"] {
                // Presence alone is invalid, including an empty value.
                let params = BTreeMap::from([(parameter.to_owned(), String::new())]);
                let result = ConnectionWarmConfig::try_new(
                    per_database,
                    32,
                    4,
                    Duration::from_secs(5),
                    params,
                );
                assert!(
                    matches!(
                        result,
                        Err(ConnectionWarmConfigError::ConnectionWarmInvalidConnectionParams)
                    ),
                    "per_database={per_database}, parameter={parameter}: {result:?}",
                );
            }
        }
    }

    #[test]
    fn disabled_warming_and_background_only_startup_are_valid() {
        let config = ConnectionWarmConfig::try_new(0, 1, 1, Duration::ZERO, BTreeMap::new())
            .expect("zero target and wait are valid with positive resource limits");
        assert_eq!(config.per_database, 0);
        assert_eq!(config.startup_wait, Duration::ZERO);
        assert!(config.startup_params.is_empty());
    }

    #[test]
    fn target_above_global_capacity_preserves_supplied_settings() {
        let params = BTreeMap::from([
            ("user".to_owned(), "test_user".to_owned()),
            ("application_name".to_owned(), "vitest".to_owned()),
        ]);
        let config =
            ConnectionWarmConfig::try_new(8, 2, 1, Duration::from_millis(250), params.clone())
                .expect("the global cap may be smaller than the per-database target");
        assert_eq!(config.per_database, 8);
        assert_eq!(config.max_total, 2);
        assert_eq!(config.concurrency, 1);
        assert_eq!(config.startup_wait, Duration::from_millis(250));
        assert_eq!(config.startup_params, params);
    }
}
