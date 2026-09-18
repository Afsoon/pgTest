use std::collections::VecDeque;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    utils::ReadString,
    worker_engine::database_jobs::{CreateDatabase, DatabaseId},
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
    pub creating: FxHashSet<DatabaseId>,
    pub ready: VecDeque<Database>,
    pub retiring: FxHashMap<DatabaseId, ReadString>,
}

impl DatabaseInventory {
    pub fn reserve_creation(&mut self) -> CreateDatabase {
        self.next_database_id =
            self.next_database_id.checked_add(1).expect("database identety exhausted");

        let database_id = DatabaseId(self.next_database_id);
        self.creating.insert(database_id);

        CreateDatabase { database_id }
    }

    pub fn supply_len(&self) -> usize {
        self.ready.len() + self.creating.len()
    }
}
