pub struct KvdbLog {
    pub db: Arc<Mutex<Engine>>,
    committing: Arc<AtomicBool>,
}

impl KvdbLog {
    pub fn new(db: Arc<Mutex<Engine>>) -> KvdbLog {
        Self {
            db,
            committing: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl KeyValueDbTypes for KvdbLog {
    type ValueType = Box<[u8]>;
}

impl<'g> KeyValueDbTypes for MutexGuard<'g, Engine> {
    type ValueType = Box<[u8]>;
}

impl<'g> KeyValueDbTraitOwnedRead for MutexGuard<'g, Engine> {
    fn get_mut(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>> {
        Ok(self.info().get(key).cloned())
    }

    fn get_mut_with_number_key(
        &mut self, key: i64,
    ) -> Result<Option<Box<[u8]>>> {
        Ok(self.get(key as RowId)?)
    }
}

impl KeyValueDbToOwnedReadTrait for KvdbLog {
    fn to_owned_read(
        &self,
    ) -> Result<Box<dyn '_ + KeyValueDbTraitOwnedRead<ValueType = Box<[u8]>>>>
    {
        Ok(Box::new(self.db.lock()))
    }
}

impl KeyValueDbTraitRead for KvdbLog {
    fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>> {
        Ok(self.db.lock().info().get(key).cloned())
    }

    fn get_with_number_key(&self, key: i64) -> Result<Option<Box<[u8]>>> {
        Ok(self.db.lock().get(key as RowId)?)
    }
}

impl KeyValueDbTraitTransactionalDyn for KvdbLog {
    fn start_transaction_dyn(
        &self, immediate_write: bool,
    ) -> Result<Box<dyn KeyValueDbTransactionTrait<ValueType = Box<[u8]>>>>
    {
        assert_eq!(self.committing.load(Ordering::SeqCst), false);
        assert_eq!(immediate_write, true);
        self.committing.store(true, Ordering::SeqCst);
        Ok(Box::new(KvdbLogTxn {
            pending_info: HashMap::new(),
            pending_entries: Vec::new(),
            next_row: self.db.lock().next_row(),
            committing_flag: self.committing.clone(),
        }))
    }
}

impl DeltaDbTrait for KvdbLog {}

struct KvdbLogTxn {
    pending_entries: Vec<Box<[u8]>>,
    pending_info: HashMap<Box<[u8]>, Box<[u8]>>,
    next_row: RowId,
    committing_flag: Arc<AtomicBool>,
}

impl Drop for KvdbLogTxn {
    fn drop(&mut self) { self.committing_flag.store(false, Ordering::SeqCst); }
}

impl KeyValueDbTypes for KvdbLogTxn {
    type ValueType = Box<[u8]>;
}

impl KeyValueDbTraitSingleWriter for KvdbLogTxn {
    fn delete(
        &mut self, key: &[u8],
    ) -> Result<Option<Option<Self::ValueType>>> {
        self.pending_info.remove(key);
        Ok(None)
    }

    fn delete_with_number_key(
        &mut self, _key: i64,
    ) -> Result<Option<Option<Box<[u8]>>>> {
        unreachable!()
    }

    fn put(
        &mut self, key: &[u8], value: &[u8],
    ) -> Result<Option<Option<Self::ValueType>>> {
        self.pending_info.insert(
            Vec::from(key).into_boxed_slice(),
            Vec::from(value).into_boxed_slice(),
        );
        Ok(None)
    }

    fn put_with_number_key(
        &mut self, key: i64, value: &[u8],
    ) -> Result<Option<Option<Self::ValueType>>> {
        assert_eq!(self.next_row, key as RowId);
        self.pending_entries
            .push(value.to_owned().into_boxed_slice());
        self.next_row += 1;
        Ok(None)
    }
}

impl KeyValueDbTraitOwnedRead for KvdbLogTxn {
    fn get_mut(&mut self, _key: &[u8]) -> Result<Option<Box<[u8]>>> {
        // Transaction doesn't implement get method, so the user shouldn't
        // rely on this method.
        unreachable!()
    }
}

impl KeyValueDbTransactionTrait for KvdbLogTxn {
    fn commit(&mut self, db: &dyn Any) -> Result<()> {
        match db.downcast_ref::<KvdbLog>() {
            Some(log) => {
                let mut locked_db = log.db.lock();
                let mut txn = locked_db.transaction()?;
                for entry in self.pending_entries.drain(..) {
                    txn.append(entry.as_ref())?;
                }
                for (k, v) in self.pending_info.drain() {
                    txn.put_info(k.as_ref(), v.as_ref());
                }
                Ok(txn.commit()?)
            }
            None => unreachable!(),
        }
    }

    fn revert(&mut self) {
        self.pending_entries.clear();
        self.pending_info.clear();
    }

    fn restart(
        &mut self, immediate_write: bool, no_revert: bool,
    ) -> Result<()> {
        assert_eq!(immediate_write, true);
        if !no_revert {
            self.revert()
        }
        Ok(())
    }
}

use super::super::{
    super::storage_db::{delta_db_manager::DeltaDbTrait, key_value_db::*},
    errors::*,
};
use lengine::*;
use parking_lot::{Mutex, MutexGuard};
use std::{
    any::Any,
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
