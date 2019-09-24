pub struct DeltaDbManagerLog {
    pub engine: Arc<Mutex<Engine>>,
}

#[allow(unused)]
impl DeltaDbManagerLog {
    pub fn new(engine: Arc<Mutex<Engine>>) -> DeltaDbManagerLog {
        Self {
            engine: engine.clone(),
        }
    }
}

impl DeltaDbManagerTrait for DeltaDbManagerLog {
    type DeltaDb = KvdbLog;

    fn new_empty_delta_db(&self, _delta_db_name: &str) -> Result<KvdbLog> {
        Ok(KvdbLog::new(self.engine.clone()))
    }

    fn get_delta_db(&self, _delta_db_name: &str) -> Result<Option<KvdbLog>> {
        unimplemented!()
    }

    fn destroy_delta_db(&self, _delta_db_name: &str) -> Result<()> { Ok(()) }
}

use super::{
    super::{
        super::storage_db::delta_db_manager::DeltaDbManagerTrait, errors::*,
    },
    kvdb_log::KvdbLog,
};
use lengine::*;
use parking_lot::Mutex;
use std::sync::Arc;
