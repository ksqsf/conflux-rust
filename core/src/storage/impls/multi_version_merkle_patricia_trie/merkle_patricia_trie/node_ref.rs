// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use super::super::{
    node_memory_manager::ActualSlabIndex, node_ref_map::DeltaMptDbKey,
};
use rlp::*;

/// The MSB is used to indicate if a node is in mem or on disk,
/// the higher 31 bits after the MSB specifies the index of the node in the
/// memory region, while the lower 32 bits indicate the original DB key, if the
/// node is a dirty node in mem.
///
/// Although a NodeRefDeltaMptCompact is 64 bits long, the RLP encoding only
/// considers its high 32 bits, because data on wire will only consist of DB
/// keys.
///
/// It's necessary to use MaybeNodeRef in ChildrenTable because it consumes less
/// space than NodeRef.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NodeRefDeltaMptCompact {
    value: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MaybeNodeRefDeltaMptCompact {
    value: u64,
}

impl Default for MaybeNodeRefDeltaMptCompact {
    fn default() -> Self { Self { value: Self::NULL } }
}

impl NodeRefDeltaMptCompact {
    /// Valid dirty slot ranges from [0..DIRTY_SLOT_LIMIT).
    /// The DIRTY_SLOT_LIMIT is reserved for MaybeNodeRefDeltaMptCompact#NULL.
    pub const DIRTY_SLOT_LIMIT: u64 = 0x7fffffffffffffff;
    const PERSISTENT_KEY_BIT: u64 = 0x8000000000000000;

    pub fn new(value: u64) -> Self { Self { value } }

    pub fn is_dirty(&self) -> bool { !self.is_committed() }

    pub fn is_committed(&self) -> bool {
        (self.value & Self::PERSISTENT_KEY_BIT) != 0
    }
}

impl MaybeNodeRefDeltaMptCompact {
    const NULL: u64 = 0;
    pub const NULL_NODE: MaybeNodeRefDeltaMptCompact =
        MaybeNodeRefDeltaMptCompact { value: Self::NULL };

    pub fn new(value: u64) -> Self { Self { value } }

    pub fn is_none(&self) -> bool { *self == Self::NULL_NODE }

    pub fn is_some(&self) -> bool { !self.is_none() }

    pub fn is_dirty(&self) -> bool { !self.is_committed() }

    pub fn is_committed(&self) -> bool {
        (self.value & NodeRefDeltaMptCompact::PERSISTENT_KEY_BIT) != 0
    }
}

// Manages access to a TrieNode. Converted from MaybeNodeRef. NodeRef is not
// copy because it controls access to TrieNode.
#[derive(Clone, Eq, PartialOrd, PartialEq, Ord)]
pub enum NodeRefDeltaMpt {
    Committed {
        db_key: DeltaMptDbKey,
    },
    Dirty {
        index: ActualSlabIndex,
        original_db_key: Option<DeltaMptDbKey>,
    },
}

impl NodeRefDeltaMpt {
    /// Returns the db key to the original node (old version found in the
    /// database). Returns None if this node ref is clean (Committed).
    pub fn original_db_key(&self) -> Option<DeltaMptDbKey> {
        match self {
            NodeRefDeltaMpt::Committed { .. } => None,
            NodeRefDeltaMpt::Dirty {
                original_db_key, ..
            } => original_db_key.clone(),
        }
    }

    pub fn is_committed(&self) -> bool { !self.is_dirty() }

    pub fn is_dirty(&self) -> bool {
        if let NodeRefDeltaMpt::Dirty { .. } = self {
            true
        } else {
            false
        }
    }
}

impl From<NodeRefDeltaMpt> for NodeRefDeltaMptCompact {
    fn from(node: NodeRefDeltaMpt) -> Self {
        fn from_maybe_u32(x: Option<u32>) -> u32 {
            match x {
                Some(x) => x,
                None => 0xffff_ffff,
            }
        }

        match node {
            NodeRefDeltaMpt::Committed { db_key } => Self {
                value: ((db_key as u64) << 32)
                    ^ NodeRefDeltaMptCompact::PERSISTENT_KEY_BIT,
            },
            NodeRefDeltaMpt::Dirty {
                index,
                original_db_key,
            } => Self {
                value: ((index as u64) << 32
                    | from_maybe_u32(original_db_key) as u64)
                    ^ NodeRefDeltaMptCompact::DIRTY_SLOT_LIMIT,
            },
        }
    }
}

impl From<NodeRefDeltaMptCompact> for NodeRefDeltaMpt {
    fn from(x: NodeRefDeltaMptCompact) -> Self {
        fn to_maybe_u32(x: u32) -> Option<u32> {
            if x == 0xffff_ffff {
                None
            } else {
                Some(x)
            }
        }

        if x.is_dirty() {
            NodeRefDeltaMpt::Dirty {
                index: ((NodeRefDeltaMptCompact::DIRTY_SLOT_LIMIT ^ x.value)
                    >> 32) as u32,
                original_db_key: to_maybe_u32(
                    (NodeRefDeltaMptCompact::DIRTY_SLOT_LIMIT ^ x.value) as u32,
                ),
            }
        } else {
            NodeRefDeltaMpt::Committed {
                db_key: ((NodeRefDeltaMptCompact::PERSISTENT_KEY_BIT ^ x.value)
                    >> 32) as u32,
            }
        }
    }
}

impl From<MaybeNodeRefDeltaMptCompact> for Option<NodeRefDeltaMpt> {
    fn from(x: MaybeNodeRefDeltaMptCompact) -> Self {
        if x.is_none() {
            None
        } else {
            Some(NodeRefDeltaMptCompact::new(x.value).into())
        }
    }
}

impl From<Option<NodeRefDeltaMpt>> for MaybeNodeRefDeltaMptCompact {
    fn from(maybe_node: Option<NodeRefDeltaMpt>) -> Self {
        match maybe_node {
            None => MaybeNodeRefDeltaMptCompact::NULL_NODE,
            Some(node) => MaybeNodeRefDeltaMptCompact::new(
                NodeRefDeltaMptCompact::from(node).value,
            ),
        }
    }
}

/// We only read 32 bits, see Encodable for NodeRefDeltaMptCompact.
impl Decodable for NodeRefDeltaMptCompact {
    fn decode(rlp: &Rlp) -> ::std::result::Result<Self, DecoderError> {
        let val: u32 = rlp.as_val()?;
        // lower bits being zero indicates that the original db key is none.
        // Cf. impl From<NodeRefDeltaMpt> for NodeRefDeltaMptCompact.
        Ok(NodeRefDeltaMptCompact {
            value: (val as u64) << 32,
        })
    }
}

/// We only encode the higher 32 bits, because everything on wire will only be
/// DB keys.
impl Encodable for NodeRefDeltaMptCompact {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.append_internal(&((self.value >> 32) as u32));
    }
}
