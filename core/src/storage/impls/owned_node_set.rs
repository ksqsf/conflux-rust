#[derive(Default, Debug)]
pub struct OwnedNodeSet {
    dirty: BTreeMap<ActualSlabIndex, Option<DeltaMptDbKey>>,
    committed: BTreeSet<DeltaMptDbKey>,
}

impl OwnedNodeSet {
    pub fn insert(&mut self, val: NodeRefDeltaMpt) -> bool {
        match val {
            NodeRefDeltaMpt::Committed { db_key } => {
                self.committed.insert(db_key)
            }
            NodeRefDeltaMpt::Dirty {
                index,
                original_db_key,
            } => self.dirty.insert(index, original_db_key).is_none(),
        }
    }

    pub fn remove(&mut self, val: &NodeRefDeltaMpt) -> bool {
        match val {
            NodeRefDeltaMpt::Committed { db_key } => {
                self.committed.remove(db_key)
            }
            NodeRefDeltaMpt::Dirty { index, .. } => {
                self.dirty.remove(index).is_some()
            }
        }
    }

    pub fn contains(&self, val: &NodeRefDeltaMpt) -> bool {
        match val {
            NodeRefDeltaMpt::Committed { db_key } => {
                self.committed.contains(db_key)
            }
            NodeRefDeltaMpt::Dirty { index, .. } => {
                self.dirty.contains_key(index)
            }
        }
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter {
            dirty_iter: self.dirty.iter().fuse(),
            committed_iter: self.committed.iter().fuse(),
        }
    }
}

pub struct Iter<'a> {
    committed_iter:
        std::iter::Fuse<std::collections::btree_set::Iter<'a, DeltaMptDbKey>>,
    dirty_iter: std::iter::Fuse<
        std::collections::btree_map::Iter<
            'a,
            ActualSlabIndex,
            Option<DeltaMptDbKey>,
        >,
    >,
}

impl<'a> Iterator for Iter<'a> {
    type Item = NodeRefDeltaMpt;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(dirty) = self.dirty_iter.next() {
            return Some(NodeRefDeltaMpt::Dirty {
                index: *dirty.0,
                original_db_key: *dirty.1,
            });
        }

        if let Some(committed) = self.committed_iter.next() {
            return Some(NodeRefDeltaMpt::Committed { db_key: *committed });
        }

        return None;
    }
}

impl<'a> IntoIterator for &'a OwnedNodeSet {
    type IntoIter = Iter<'a>;
    type Item = NodeRefDeltaMpt;

    fn into_iter(self) -> Iter<'a> { self.iter() }
}

use super::multi_version_merkle_patricia_trie::{
    merkle_patricia_trie::NodeRefDeltaMpt,
    node_memory_manager::ActualSlabIndex, node_ref_map::DeltaMptDbKey,
};
use std::collections::{BTreeMap, BTreeSet};
