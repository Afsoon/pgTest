use std::collections::VecDeque;

use pgtest_utils::read_string::ReadString;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::worker_engine::{
    database_jobs::{CleanupDatabase, CreateDatabase, DatabaseId},
    errors::PostgresDDLClientError,
};

#[derive(Clone, Debug)]
pub struct Database {
    pub database_id: DatabaseId,
    pub database_name: ReadString,
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone, Debug))]
pub struct DatabaseInventory {
    next_database_id: u64,
    creating: FxHashSet<DatabaseId>,
    ready: VecDeque<Database>,
    retiring: FxHashMap<DatabaseId, ReadString>,
}

impl DatabaseInventory {
    pub fn reserve_creation(&mut self) -> CreateDatabase {
        self.next_database_id =
            self.next_database_id.checked_add(1).expect("database identity exhausted");

        let database_id = DatabaseId(self.next_database_id);
        self.creating.insert(database_id);

        CreateDatabase { database_id }
    }

    /// Release a reservation when its creation request cannot be submitted.
    pub fn cancel_creation(&mut self, database_id: DatabaseId) -> bool {
        self.creating.remove(&database_id)
    }

    /// Settle a reserved creation. Unknown or repeated completions are ignored.
    /// A failed creation frees its reservation without contributing ready
    /// supply.
    pub fn complete_creation(
        &mut self,
        database_id: DatabaseId,
        result: Result<ReadString, PostgresDDLClientError>,
    ) -> Option<Result<(), PostgresDDLClientError>> {
        if !self.creating.remove(&database_id) {
            return None;
        }
        Some(result.map(|database_name| {
            self.ready.push_back(Database { database_id, database_name });
        }))
    }

    pub fn take_ready(&mut self) -> Option<Database> {
        self.ready.pop_front()
    }

    /// Restore a checked-out database to its original place ahead of unused
    /// supply.
    pub fn return_ready(&mut self, database: Database) {
        debug_assert!(!self.contains(database.database_id));
        self.ready.push_front(database);
    }

    /// Record retirement before the caller submits cleanup, so a submission
    /// failure cannot lose the database's identity.
    pub fn retire(&mut self, database: Database) -> CleanupDatabase {
        debug_assert!(!self.contains(database.database_id));
        let Database { database_id, database_name } = database;
        self.retiring.insert(database_id, database_name.clone());
        CleanupDatabase { database_id, database_name }
    }

    /// Forget a retired database only after successful cleanup. Unknown or
    /// repeated completions are ignored, and failures retain the record.
    pub fn complete_cleanup(
        &mut self,
        database_id: DatabaseId,
        result: Result<(), PostgresDDLClientError>,
    ) -> Option<Result<(), PostgresDDLClientError>> {
        if !self.retiring.contains_key(&database_id) {
            return None;
        }
        Some(result.map(|()| {
            self.retiring.remove(&database_id);
        }))
    }

    pub fn creating(&self) -> &FxHashSet<DatabaseId> {
        &self.creating
    }

    pub fn ready(&self) -> &VecDeque<Database> {
        &self.ready
    }

    pub fn retiring(&self) -> &FxHashMap<DatabaseId, ReadString> {
        &self.retiring
    }

    pub fn supply_len(&self) -> usize {
        self.ready.len() + self.creating.len()
    }

    fn contains(&self, database_id: DatabaseId) -> bool {
        self.creating.contains(&database_id)
            || self.ready.iter().any(|database| database.database_id == database_id)
            || self.retiring.contains_key(&database_id)
    }
}
