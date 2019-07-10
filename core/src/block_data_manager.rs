// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{
    cache_manager::{CacheId, CacheManager, CacheSize},
    db::{COL_BLOCKS, COL_BLOCK_RECEIPTS, COL_TX_ADDRESS},
    ext_db::SystemDB,
    pow::TargetDifficultyManager,
    storage::{
        state_manager::{SnapshotAndEpochIdRef, StateManagerTrait},
        StorageManager,
    },
    verification::VerificationConfig,
};
use cfx_types::{Bloom, H256};
use heapsize::HeapSizeOf;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use primitives::{
    block::CompactBlock,
    receipt::{Receipt, TRANSACTION_OUTCOME_SUCCESS},
    Block, BlockHeader, SignedTransaction, TransactionAddress,
    TransactionWithSignature,
};
use rlp::{Rlp, RlpStream};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

const BLOCK_STATUS_SUFFIX_BYTE: u8 = 1;
const BLOCK_BODY_SUFFIX_BYTE: u8 = 2;

pub struct BlockDataManager {
    block_headers: RwLock<HashMap<H256, Arc<BlockHeader>>>,
    blocks: RwLock<HashMap<H256, Arc<Block>>>,
    compact_blocks: RwLock<HashMap<H256, CompactBlock>>,
    block_receipts: RwLock<HashMap<H256, BlockReceiptsInfo>>,
    transaction_addresses: RwLock<HashMap<H256, TransactionAddress>>,
    pub transaction_pubkey_cache: RwLock<HashMap<H256, Arc<SignedTransaction>>>,
    block_receipts_root: RwLock<HashMap<H256, H256>>,
    invalid_block_set: RwLock<HashSet<H256>>,
    cur_consensus_era_genesis_hash: RwLock<H256>,

    config: DataManagerConfiguration,

    pub genesis_block: Arc<Block>,
    pub db: Arc<SystemDB>,
    pub storage_manager: Arc<StorageManager>,
    pub cache_man: Arc<Mutex<CacheManager<CacheId>>>,
    pub target_difficulty_manager: TargetDifficultyManager,
}

impl BlockDataManager {
    pub fn new(
        genesis_block: Arc<Block>, db: Arc<SystemDB>,
        storage_manager: Arc<StorageManager>,
        cache_man: Arc<Mutex<CacheManager<CacheId>>>,
        config: DataManagerConfiguration,
    ) -> Self
    {
        let genesis_hash = genesis_block.block_header.hash();
        let data_man = Self {
            block_headers: RwLock::new(HashMap::new()),
            blocks: RwLock::new(HashMap::new()),
            compact_blocks: Default::default(),
            block_receipts: Default::default(),
            transaction_addresses: Default::default(),
            block_receipts_root: Default::default(),
            transaction_pubkey_cache: Default::default(),
            invalid_block_set: Default::default(),
            genesis_block,
            db,
            storage_manager,
            cache_man,
            config,
            target_difficulty_manager: TargetDifficultyManager::new(),
            cur_consensus_era_genesis_hash: RwLock::new(genesis_hash),
        };

        data_man.insert_receipts_root(
            data_man.genesis_block.hash(),
            *data_man.genesis_block.block_header.deferred_receipts_root(),
        );
        data_man.insert_block_header(
            data_man.genesis_block.hash(),
            Arc::new(data_man.genesis_block.block_header.clone()),
        );
        data_man.insert_block_to_kv(data_man.genesis_block(), true);
        data_man
    }

    pub fn genesis_block(&self) -> Arc<Block> { self.genesis_block.clone() }

    pub fn transaction_by_hash(
        &self, hash: &H256,
    ) -> Option<Arc<SignedTransaction>> {
        let address = self.transaction_address_by_hash(hash, false)?;
        let block = self.block_by_hash(&address.block_hash, false)?;
        assert!(address.index < block.transactions.len());
        Some(block.transactions[address.index].clone())
    }

    fn block_header_from_db(&self, hash: &H256) -> Option<BlockHeader> {
        let rlp_bytes = self.db.key_value().get(COL_BLOCKS, hash)
            .expect("Low level database error when fetching block. Some issue with disk?")?;
        let rlp = Rlp::new(&rlp_bytes);
        let mut block_header = rlp.as_val().expect("Wrong block rlp format!");
        VerificationConfig::compute_header_pow_quality(&mut block_header);
        Some(block_header)
    }

    fn insert_block_header_to_db(&self, header: &BlockHeader) {
        let mut dbops = self.db.key_value().transaction();
        dbops.put(COL_BLOCKS, &header.hash(), &rlp::encode(header));
        self.db
            .key_value()
            .write(dbops)
            .expect("crash for db failure");
    }

    fn block_body_key(block_hash: &H256) -> Vec<u8> {
        let mut key = Vec::with_capacity(block_hash.len() + 1);
        key.extend_from_slice(&block_hash);
        key.push(BLOCK_BODY_SUFFIX_BYTE);
        key
    }

    fn block_body_from_db(
        &self, block_hash: &H256,
    ) -> Option<Vec<Arc<SignedTransaction>>> {
        let rlp_bytes = self.db.key_value().get(COL_BLOCKS, &Self::block_body_key(block_hash))
            .expect("Low level database error when fetching block. Some issue with disk?")?;
        let rlp = Rlp::new(&rlp_bytes);
        let block_body = Block::decode_body_with_tx_public(&rlp)
            .expect("Wrong block rlp format!");
        Some(block_body)
    }

    fn insert_block_body_to_db(&self, block: &Block) {
        let mut dbops = self.db.key_value().transaction();
        dbops.put(
            COL_BLOCKS,
            &Self::block_body_key(&block.hash()),
            &block.encode_body_with_tx_public(),
        );
        self.db
            .key_value()
            .write(dbops)
            .expect("crash for db failure");
    }

    pub fn block_by_hash(
        &self, hash: &H256, update_cache: bool,
    ) -> Option<Arc<Block>> {
        // Check cache first
        {
            let read = self.blocks.read();
            if let Some(v) = read.get(hash) {
                return Some(v.clone());
            }
        }

        let header = self.block_header_by_hash(hash)?;
        let block = Arc::new(Block {
            block_header: header.as_ref().clone(),
            transactions: self.block_body_from_db(hash)?,
            approximated_rlp_size: 0,
            approximated_rlp_size_with_public: 0,
        });

        if update_cache {
            let mut write = self.blocks.write();
            write.insert(*hash, block.clone());
            self.cache_man.lock().note_used(CacheId::Block(*hash));
        }
        Some(block)
    }

    pub fn block_from_db(&self, block_hash: &H256) -> Option<Block> {
        Some(Block::new(
            self.block_header_from_db(block_hash)?,
            self.block_body_from_db(block_hash)?,
        ))
    }

    pub fn blocks_by_hash_list(
        &self, hashes: &Vec<H256>, update_cache: bool,
    ) -> Option<Vec<Arc<Block>>> {
        let mut blocks = Vec::new();
        for h in hashes {
            blocks.push(self.block_by_hash(h, update_cache)?);
        }
        Some(blocks)
    }

    pub fn insert_block_to_kv(&self, block: Arc<Block>, persistent: bool) {
        let hash = block.hash();
        if persistent {
            self.insert_block_header_to_db(&block.block_header);
            self.insert_block_body_to_db(&block);
        }
        self.blocks.write().insert(hash, block);
        self.cache_man.lock().note_used(CacheId::Block(hash));
    }

    fn block_status_key(block_hash: &H256) -> Vec<u8> {
        let mut key = Vec::with_capacity(block_hash.len() + 1);
        key.extend_from_slice(block_hash);
        key.push(BLOCK_STATUS_SUFFIX_BYTE);
        key
    }

    /// Store block status to db. Now the status means if the block is partial
    /// invalid.
    /// The db key is the block hash plus one extra byte, so we can get better
    /// data locality if we get both a block and its status from db.
    /// The status is not a part of the block because the block is inserted
    /// before we know its status, and we do not want to insert a large chunk
    /// again. TODO Maybe we can use in-place modification (operator `merge`
    /// in rocksdb) to keep the status together with the block.
    pub fn insert_block_status_to_db(
        &self, block_hash: &H256, status: BlockStatus,
    ) {
        let mut dbops = self.db.key_value().transaction();
        dbops.put(
            COL_BLOCKS,
            &Self::block_status_key(block_hash),
            &[status.to_db_status()],
        );
        self.db
            .key_value()
            .write(dbops)
            .expect("crash for db failure");
    }

    /// Get block status from db. Now the status means if the block is partial
    /// invalid
    pub fn block_status_from_db(
        &self, block_hash: &H256,
    ) -> Option<BlockStatus> {
        self.db
            .key_value()
            .get(COL_BLOCKS, &Self::block_status_key(block_hash))
            .expect("crash for db failure")
            .map(|encoded| BlockStatus::from_db_status(encoded[0]))
    }

    pub fn remove_block_from_kv(&self, hash: &H256) {
        self.blocks.write().remove(hash);
        let mut dbops = self.db.key_value().transaction();
        dbops.delete(COL_BLOCKS, hash);
        self.db
            .key_value()
            .write(dbops)
            .expect("crash for db failure");
    }

    pub fn block_header_by_hash(
        &self, hash: &H256,
    ) -> Option<Arc<BlockHeader>> {
        let block_headers = self.block_headers.upgradable_read();
        if let Some(header) = block_headers.get(hash) {
            return Some(header.clone());
        } else if !self.config.persist_header {
            return None;
        } else {
            let maybe_header = self.block_header_from_db(hash);
            maybe_header.map(|header| {
                let header_arc = Arc::new(header);
                RwLockUpgradableReadGuard::upgrade(block_headers)
                    .insert(header_arc.hash(), header_arc.clone());
                self.cache_man
                    .lock()
                    .note_used(CacheId::BlockHeader(header_arc.hash()));
                header_arc
            })
        }
    }

    pub fn insert_block_header(&self, hash: H256, header: Arc<BlockHeader>) {
        if self.config.persist_header {
            self.insert_block_header_to_db(&header);
            self.cache_man.lock().note_used(CacheId::BlockHeader(hash));
        }
        self.block_headers.write().insert(hash, header);
    }

    pub fn remove_block_header(&self, hash: &H256) -> Option<Arc<BlockHeader>> {
        self.block_headers.write().remove(hash)
    }

    pub fn block_height_by_hash(&self, hash: &H256) -> Option<u64> {
        let result = self.block_by_hash(hash, false)?;
        Some(result.block_header.height())
    }

    pub fn compact_block_by_hash(&self, hash: &H256) -> Option<CompactBlock> {
        self.compact_blocks.read().get(hash).map(|b| {
            self.cache_man
                .lock()
                .note_used(CacheId::CompactBlock(b.hash()));
            b.clone()
        })
    }

    pub fn insert_compact_block(&self, cb: CompactBlock) {
        let hash = cb.hash();
        self.compact_blocks.write().insert(hash, cb);
        self.cache_man.lock().note_used(CacheId::CompactBlock(hash));
    }

    pub fn contains_compact_block(&self, hash: &H256) -> bool {
        self.compact_blocks.read().contains_key(hash)
    }

    pub fn block_results_by_hash_from_db(
        &self, hash: &H256,
    ) -> Option<(H256, BlockExecutedResult)> {
        trace!("Read receipts from db {}", hash);
        let block_receipts = self.db.key_value().get(COL_BLOCK_RECEIPTS, hash)
            .expect("Low level database error when fetching block receipts. Some issue with disk?")?;
        let rlp = Rlp::new(&block_receipts);
        let epoch: H256 = rlp.val_at(0).expect("encoded");
        let receipts: Vec<Receipt> = rlp.list_at(1).expect("encoded");
        let bloom: Bloom = rlp.val_at(2).expect("encoded");
        Some((
            epoch,
            BlockExecutedResult {
                receipts: Arc::new(receipts),
                bloom,
            },
        ))
    }

    /// Return None if receipts for corresponding epoch is not computed before
    /// or has been overwritten by another new pivot chain in db
    ///
    /// This function will require lock of block_receipts
    pub fn block_results_by_hash_with_epoch(
        &self, hash: &H256, assumed_epoch: &H256, update_cache: bool,
    ) -> Option<BlockExecutedResult> {
        let maybe_receipts =
            self.block_receipts
                .read()
                .get(hash)
                .and_then(|receipt_info| {
                    receipt_info.get_receipts_at_epoch(assumed_epoch)
                });
        if maybe_receipts.is_some() {
            return maybe_receipts;
        }
        let (epoch, receipts) = self.block_results_by_hash_from_db(hash)?;
        if epoch != *assumed_epoch {
            debug!(
                "epoch from db {} does not match assumed {}",
                epoch, assumed_epoch
            );
            return None;
        }
        if update_cache {
            self.block_receipts
                .write()
                .entry(*hash)
                .or_insert(BlockReceiptsInfo::default())
                .insert_receipts_at_epoch(assumed_epoch, receipts.clone());
            self.cache_man
                .lock()
                .note_used(CacheId::BlockReceipts(*hash));
        }
        Some(receipts)
    }

    pub fn insert_block_results_to_kv(
        &self, hash: H256, epoch: H256, receipts: Arc<Vec<Receipt>>,
        persistent: bool,
    )
    {
        let bloom = receipts.iter().fold(Bloom::zero(), |mut b, r| {
            b.accrue_bloom(&r.log_bloom);
            b
        });

        if persistent {
            let mut dbops = self.db.key_value().transaction();
            let mut rlp_stream = RlpStream::new_list(3);
            rlp_stream.append(&epoch);
            rlp_stream.append_list(&receipts);
            rlp_stream.append(&bloom);
            dbops.put(COL_BLOCK_RECEIPTS, &hash, &rlp_stream.drain());
            self.db
                .key_value()
                .write(dbops)
                .expect("crash for db failure");
        }

        let mut block_receipts = self.block_receipts.write();
        let receipt_info = block_receipts
            .entry(hash)
            .or_insert(BlockReceiptsInfo::default());
        receipt_info.insert_receipts_at_epoch(
            &epoch,
            BlockExecutedResult { receipts, bloom },
        );

        self.cache_man
            .lock()
            .note_used(CacheId::BlockReceipts(hash));
    }

    pub fn transaction_address_by_hash_from_db(
        &self, hash: &H256,
    ) -> Option<TransactionAddress> {
        let tx_index_encoded = self.db.key_value().get(COL_TX_ADDRESS, hash).expect("Low level database error when fetching transaction index. Some issue with disk?")?;
        let rlp = Rlp::new(&tx_index_encoded);
        let tx_index: TransactionAddress =
            rlp.as_val().expect("Wrong tx index rlp format!");
        Some(tx_index)
    }

    pub fn transaction_address_by_hash(
        &self, hash: &H256, update_cache: bool,
    ) -> Option<TransactionAddress> {
        let transaction_addresses =
            self.transaction_addresses.upgradable_read();
        if let Some(index) = transaction_addresses.get(hash) {
            return Some(index.clone());
        }
        self.transaction_address_by_hash_from_db(hash)
            .map(|address| {
                if update_cache {
                    RwLockUpgradableReadGuard::upgrade(transaction_addresses)
                        .insert(*hash, address.clone());
                    self.cache_man
                        .lock()
                        .note_used(CacheId::TransactionAddress(*hash));
                }
                address
            })
    }

    pub fn insert_transaction_address_to_kv(
        &self, hash: &H256, tx_address: &TransactionAddress,
    ) {
        if !self.config.record_tx_address {
            return;
        }
        self.transaction_addresses
            .write()
            .entry(*hash)
            .and_modify(|v| {
                *v = tx_address.clone();
                self.cache_man
                    .lock()
                    .note_used(CacheId::TransactionAddress(*hash));
            });
        let mut dbops = self.db.key_value().transaction();
        dbops.put(COL_TX_ADDRESS, hash, &rlp::encode(tx_address));
        self.db
            .key_value()
            .write(dbops)
            .expect("crash for db failure");
    }

    /// Return `false` if there is no executed results for given `block_hash`
    pub fn receipts_retain_epoch(
        &self, block_hash: &H256, epoch: &H256,
    ) -> bool {
        match self.block_receipts.write().get_mut(block_hash) {
            Some(r) => {
                r.retain_epoch(epoch);
                true
            }
            None => false,
        }
    }

    pub fn insert_receipts_root(
        &self, block_hash: H256, receipts_root: H256,
    ) -> Option<H256> {
        self.block_receipts_root
            .write()
            .insert(block_hash, receipts_root)
    }

    pub fn get_receipts_root(&self, block_hash: &H256) -> Option<H256> {
        self.block_receipts_root
            .read()
            .get(block_hash)
            .map(Clone::clone)
    }

    pub fn cache_transaction(
        &self, tx_hash: &H256, tx: Arc<SignedTransaction>,
    ) {
        let mut transactions = self.transaction_pubkey_cache.write();
        let mut cache_man = self.cache_man.lock();
        transactions.insert(*tx_hash, tx);
        cache_man.note_used(CacheId::TransactionPubkey(*tx_hash))
    }

    pub fn get_uncached_transactions(
        &self, transactions: &Vec<TransactionWithSignature>,
    ) -> Vec<TransactionWithSignature> {
        let tx_cache = self.transaction_pubkey_cache.read();
        transactions
            .iter()
            .filter(|tx| {
                let tx_hash = tx.hash();
                let inserted = tx_cache.contains_key(&tx_hash);
                // Sample 1/128 transactions
                if tx_hash[0] & 254 == 0 {
                    debug!("Sampled transaction {:?} in tx pool", tx_hash);
                }

                !inserted
            })
            .map(|tx| tx.clone())
            .collect()
    }

    pub fn epoch_executed(&self, epoch_hash: &H256) -> bool {
        // `block_receipts_root` is not computed when recovering from db with
        // fast_recover == false. And we should force it to recompute
        // without checking receipts when fast_recover == false
        self.get_receipts_root(epoch_hash).is_some()
            && self
                .storage_manager
                .contains_state(SnapshotAndEpochIdRef::new(epoch_hash, None))
                .unwrap()
    }

    /// Check if all executed results of an epoch exist
    pub fn epoch_executed_and_recovered(
        &self, epoch_hash: &H256, epoch_block_hashes: &Vec<H256>,
        on_local_pivot: bool,
    ) -> bool
    {
        if !self.epoch_executed(epoch_hash) {
            return false;
        }

        if self.config.record_tx_address && on_local_pivot {
            // Check if all blocks receipts are from this epoch
            let mut epoch_receipts = Vec::new();
            for h in epoch_block_hashes {
                if let Some(r) =
                    self.block_results_by_hash_with_epoch(h, epoch_hash, true)
                {
                    epoch_receipts.push(r.receipts);
                } else {
                    return false;
                }
            }
            // Recover tx address if we will skip pivot chain execution
            for (block_idx, block_hash) in epoch_block_hashes.iter().enumerate()
            {
                let block =
                    self.block_by_hash(block_hash, true).expect("block exists");
                for (tx_idx, tx) in block.transactions.iter().enumerate() {
                    if epoch_receipts[block_idx]
                        .get(tx_idx)
                        .unwrap()
                        .outcome_status
                        == TRANSACTION_OUTCOME_SUCCESS
                    {
                        self.insert_transaction_address_to_kv(
                            &tx.hash,
                            &TransactionAddress {
                                block_hash: *block_hash,
                                index: tx_idx,
                            },
                        )
                    }
                }
            }
        }
        true
    }

    pub fn invalidate_block(&self, block_hash: H256) {
        self.insert_block_status_to_db(&block_hash, BlockStatus::Invalid);
        self.invalid_block_set.write().insert(block_hash);
    }

    /// Check if a block is already marked as invalid.
    pub fn verified_invalid(&self, block_hash: &H256) -> bool {
        let invalid_block_set = self.invalid_block_set.upgradable_read();
        if invalid_block_set.contains(block_hash) {
            return true;
        } else {
            if let Some(status) = self.block_status_from_db(block_hash) {
                match status {
                    BlockStatus::Invalid => {
                        RwLockUpgradableReadGuard::upgrade(invalid_block_set)
                            .insert(*block_hash);
                        return true;
                    }
                    _ => return false,
                }
            } else {
                // No status on disk, so the block is not marked invalid before
                return false;
            }
        }
    }

    pub fn cached_block_count(&self) -> usize { self.blocks.read().len() }

    /// Get current cache size.
    pub fn cache_size(&self) -> CacheSize {
        let block_headers = self.block_headers.read().heap_size_of_children();
        let blocks = self.blocks.read().heap_size_of_children();
        let compact_blocks = self.compact_blocks.read().heap_size_of_children();
        let block_receipts = self.block_receipts.read().heap_size_of_children();
        let transaction_addresses =
            self.transaction_addresses.read().heap_size_of_children();
        let transaction_pubkey = SignedTransaction::heap_size_of_iter(
            self.transaction_pubkey_cache.read().values(),
        );
        CacheSize {
            block_headers,
            blocks,
            block_receipts,
            transaction_addresses,
            compact_blocks,
            transaction_pubkey,
        }
    }

    pub fn block_cache_gc(&self) {
        let current_size = self.cache_size().total();
        let mut block_headers = self.block_headers.write();
        let mut blocks = self.blocks.write();
        let mut compact_blocks = self.compact_blocks.write();
        let mut executed_results = self.block_receipts.write();
        let mut transaction_pubkey_cache =
            self.transaction_pubkey_cache.write();
        let mut tx_address = self.transaction_addresses.write();
        let mut cache_man = self.cache_man.lock();
        info!(
            "Before gc cache_size={} {} {} {} {} {}",
            current_size,
            blocks.len(),
            compact_blocks.len(),
            executed_results.len(),
            tx_address.len(),
            transaction_pubkey_cache.len(),
        );

        cache_man.collect_garbage(current_size, |ids| {
            for id in &ids {
                match *id {
                    CacheId::Block(ref h) => {
                        blocks.remove(h);
                    }
                    CacheId::BlockReceipts(ref h) => {
                        executed_results.remove(h);
                    }
                    CacheId::TransactionAddress(ref h) => {
                        tx_address.remove(h);
                    }
                    CacheId::CompactBlock(ref h) => {
                        compact_blocks.remove(h);
                    }
                    CacheId::TransactionPubkey(ref h) => {
                        transaction_pubkey_cache.remove(h);
                    }
                    CacheId::BlockHeader(ref h) => {
                        block_headers.remove(h);
                    }
                }
            }

            block_headers.shrink_to_fit();
            blocks.shrink_to_fit();
            executed_results.shrink_to_fit();
            tx_address.shrink_to_fit();
            transaction_pubkey_cache.shrink_to_fit();
            compact_blocks.shrink_to_fit();

            block_headers.heap_size_of_children()
                + blocks.heap_size_of_children()
                + executed_results.heap_size_of_children()
                + tx_address.heap_size_of_children()
                + transaction_pubkey_cache.heap_size_of_children()
                + compact_blocks.heap_size_of_children()
        });
    }

    pub fn set_cur_consensus_era_genesis_hash(&self, hash: &H256) {
        let mut cur_era_hash = self.cur_consensus_era_genesis_hash.write();
        *cur_era_hash = hash.clone();
    }

    pub fn get_cur_consensus_era_genesis_hash(&self) -> H256 {
        self.cur_consensus_era_genesis_hash.read().clone()
    }
}

#[derive(Clone, Debug)]
pub struct BlockExecutedResult {
    pub receipts: Arc<Vec<Receipt>>,
    pub bloom: Bloom,
}
impl HeapSizeOf for BlockExecutedResult {
    fn heap_size_of_children(&self) -> usize {
        self.receipts.heap_size_of_children()
    }
}
type EpochIndex = H256;

#[derive(Default, Debug)]
pub struct BlockReceiptsInfo {
    info_with_epoch: Vec<(EpochIndex, BlockExecutedResult)>,
}

impl HeapSizeOf for BlockReceiptsInfo {
    fn heap_size_of_children(&self) -> usize {
        self.info_with_epoch.heap_size_of_children()
    }
}

impl BlockReceiptsInfo {
    /// `epoch` is the index of the epoch id in consensus arena
    pub fn get_receipts_at_epoch(
        &self, epoch: &EpochIndex,
    ) -> Option<BlockExecutedResult> {
        for (e_id, receipts) in &self.info_with_epoch {
            if *e_id == *epoch {
                return Some(receipts.clone());
            }
        }
        None
    }

    /// Insert the tx fee when the block is included in epoch `epoch`
    pub fn insert_receipts_at_epoch(
        &mut self, epoch: &EpochIndex, receipts: BlockExecutedResult,
    ) {
        // If it's inserted before, the fee must be the same, so we do not add
        // duplicate entry
        if self.get_receipts_at_epoch(epoch).is_none() {
            self.info_with_epoch.push((*epoch, receipts));
        }
    }

    /// Only keep the tx fee in the given `epoch`
    /// Called after we process rewards, and other fees will not be used w.h.p.
    pub fn retain_epoch(&mut self, epoch: &EpochIndex) {
        self.info_with_epoch.retain(|(e_id, _)| *e_id == *epoch);
    }
}

#[derive(Copy, Clone)]
pub enum BlockStatus {
    Valid = 0,
    Invalid = 1,
    PartialInvalid = 2,
    Pending = 3,
}

impl BlockStatus {
    fn from_db_status(db_status: u8) -> Self {
        match db_status {
            0 => BlockStatus::Valid,
            1 => BlockStatus::Invalid,
            2 => BlockStatus::PartialInvalid,
            3 => BlockStatus::Pending,
            _ => panic!("Read unknown block status from db"),
        }
    }

    fn to_db_status(&self) -> u8 { *self as u8 }
}

pub struct DataManagerConfiguration {
    record_tx_address: bool,
    persist_header: bool,
}

impl DataManagerConfiguration {
    pub fn new(record_tx_address: bool, persist_header: bool) -> Self {
        Self {
            record_tx_address,
            persist_header,
        }
    }
}
