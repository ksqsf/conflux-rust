pub trait MerkleDbTrait {
    /// Returns the raw bytes data of children merkles.
    fn get_children_merkles_raw_data(
        &self, key: &[u8],
    ) -> Result<Option<Box<[u8]>>>;

    /// This default implementation expects children merkles to be encoded in
    /// RLP format.
    fn get_children_merkles(
        &self, key: &[u8],
    ) -> Result<Option<[MerkleHash; CHILDREN_COUNT]>> {
        self.get_children_merkles_raw_data(key).map(|maybe_rlp| {
            maybe_rlp.map(|rlp_bytes| {
                let v: Vec<MerkleHash> = rlp::decode_list(&rlp_bytes);
                let mut ret = [Default::default(); CHILDREN_COUNT];
                assert_eq!(v.len(), CHILDREN_COUNT);
                ret.copy_from_slice(&v);
                ret
            })
        })
    }
}

use rlp;
use super::super::impls::errors::*;
pub use super::super::impls::multi_version_merkle_patricia_trie::merkle_patricia_trie::children_table::CHILDREN_COUNT;
pub use primitives::MerkleHash;
