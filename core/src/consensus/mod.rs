// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

mod anticone_cache;
pub mod consensus_inner;
mod debug;
mod pastset_cache;

use super::consensus::consensus_inner::{
    confirmation_meter::ConfirmationMeter,
    consensus_executor::ConsensusExecutor,
    consensus_new_block_handler::ConsensusNewBlockHandler,
};
pub use crate::consensus::consensus_inner::{
    ConsensusGraphInner, ConsensusInnerConfig,
};
use crate::{
    block_data_manager::BlockDataManager,
    bytes::Bytes,
    parameters::{block::REFEREE_BOUND, consensus::*, consensus_internal::*},
    pow::ProofOfWorkConfig,
    state::State,
    state_exposer::SharedStateExposer,
    statedb::StateDb,
    statistics::SharedStatistics,
    storage::{state_manager::StateManagerTrait, SnapshotAndEpochIdRef},
    transaction_pool::SharedTransactionPool,
    vm_factory::VmFactory,
};
use cfx_types::{Bloom, H160, H256, U256};
use metrics::{register_meter_with_group, Meter, MeterTimer};
use parking_lot::{Mutex, RwLock};
use primitives::{
    filter::{Filter, FilterError},
    log_entry::{LocalizedLogEntry, LogEntry},
    receipt::Receipt,
    EpochNumber, SignedTransaction, StateRootWithAuxInfo, TransactionAddress,
};
use rayon::prelude::*;
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    sync::Arc,
    thread::sleep,
    time::Duration,
};

lazy_static! {
    static ref CONSENSIS_ON_NEW_BLOCK_TIMER: Arc<dyn Meter> =
        register_meter_with_group("timer", "consensus_on_new_block_timer");
}

#[derive(Clone)]
pub struct ConsensusConfig {
    // If we hit invalid state root, we will dump the information into a
    // directory specified here. This is useful for testing.
    pub debug_dump_dir_invalid_state_root: String,
    // When bench_mode is true, the PoW solution verification will be skipped.
    // The transaction execution will also be skipped and only return the
    // pair of (KECCAK_NULL_RLP, KECCAK_EMPTY_LIST_RLP) This is for testing
    // only
    pub bench_mode: bool,
    // The configuration used by inner data
    pub inner_conf: ConsensusInnerConfig,
}

#[derive(Debug)]
pub struct ConsensusGraphStatistics {
    pub inserted_block_count: usize,
    pub processed_block_count: usize,
}

impl ConsensusGraphStatistics {
    pub fn new() -> ConsensusGraphStatistics {
        ConsensusGraphStatistics {
            inserted_block_count: 0,
            processed_block_count: 0,
        }
    }

    pub fn clear(&mut self) {
        self.inserted_block_count = 0;
        self.processed_block_count = 0;
    }
}

#[derive(Default)]
pub struct BestInformation {
    pub best_block_hash: H256,
    pub best_epoch_number: u64,
    pub current_difficulty: U256,
    // terminal_block_hashes will be None if it is same as the
    // bounded_terminal_block_hashes. This is just to save some space.
    pub terminal_block_hashes: Option<Vec<H256>>,
    pub bounded_terminal_block_hashes: Vec<H256>,
}

/// ConsensusGraph is a layer on top of SynchronizationGraph. A SyncGraph
/// collect all blocks that the client has received so far, but a block can only
/// be delivered to the ConsensusGraph if 1) the whole block content is
/// available and 2) all of its past blocks are also in the ConsensusGraph.
///
/// ConsensusGraph maintains the TreeGraph structure of the client and
/// implements *GHAST*/*Conflux* algorithm to determine the block total order.
/// It dispatches transactions in epochs to ConsensusExecutor to process. To
/// avoid executing too many execution reroll caused by transaction order
/// oscillation. It defers the transaction execution for a few epochs.
pub struct ConsensusGraph {
    pub inner: Arc<RwLock<ConsensusGraphInner>>,
    pub txpool: SharedTransactionPool,
    pub data_man: Arc<BlockDataManager>,
    executor: Arc<ConsensusExecutor>,
    pub statistics: SharedStatistics,
    pub new_block_handler: ConsensusNewBlockHandler,
    pub confirmation_meter: ConfirmationMeter,
    /// Make sure that it is only modified when holding inner lock to prevent
    /// any inconsistency
    best_info: RwLock<Arc<BestInformation>>,
    /// This is the hash of latest block inserted into consensus graph.
    /// Since the critical section is very short, a `Mutex` is enough.
    pub latest_inserted_block: Mutex<H256>,
    /// This HashMap stores whether the state in header is correct or not for
    /// pivot blocks from current era genesis to first trusted blame block
    /// after current era stable genesis.
    /// We use `Mutex` here because other thread will only modify it once and
    /// after that only current thread will operate this map.
    pub pivot_block_state_valid_map: Mutex<HashMap<H256, bool>>,
    state_exposer: SharedStateExposer,
}

pub type SharedConsensusGraph = Arc<ConsensusGraph>;

impl ConsensusGraph {
    /// Build the ConsensusGraph with a specific era genesis block and various
    /// other components. The execution will be skipped if bench_mode sets
    /// to true. The height of
    pub fn with_era_genesis_block(
        conf: ConsensusConfig, vm: VmFactory, txpool: SharedTransactionPool,
        statistics: SharedStatistics, data_man: Arc<BlockDataManager>,
        pow_config: ProofOfWorkConfig, era_genesis_block_hash: &H256,
        state_exposer: SharedStateExposer,
    ) -> Self
    {
        let inner =
            Arc::new(RwLock::new(ConsensusGraphInner::with_era_genesis_block(
                pow_config,
                data_man.clone(),
                conf.inner_conf.clone(),
                era_genesis_block_hash,
                None,
            )));
        let executor = ConsensusExecutor::start(
            txpool.clone(),
            data_man.clone(),
            vm,
            inner.clone(),
            conf.bench_mode,
        );
        let confirmation_meter = ConfirmationMeter::new();

        let graph = ConsensusGraph {
            inner,
            txpool: txpool.clone(),
            data_man: data_man.clone(),
            executor: executor.clone(),
            statistics: statistics.clone(),
            new_block_handler: ConsensusNewBlockHandler::new(
                conf, txpool, data_man, executor, statistics,
            ),
            confirmation_meter,
            best_info: RwLock::new(Arc::new(Default::default())),
            latest_inserted_block: Mutex::new(*era_genesis_block_hash),
            pivot_block_state_valid_map: Mutex::new(Default::default()),
            state_exposer,
        };
        graph.update_best_info(&*graph.inner.read());
        graph
            .txpool
            .notify_new_best_info(graph.best_info.read_recursive().clone());
        graph
    }

    /// Build the ConsensusGraph with the initial (checkpointed) genesis block
    /// in the data manager and various other components. The execution will
    /// be skipped if bench_mode sets to true.
    pub fn new(
        conf: ConsensusConfig, vm: VmFactory, txpool: SharedTransactionPool,
        statistics: SharedStatistics, data_man: Arc<BlockDataManager>,
        pow_config: ProofOfWorkConfig, state_exposer: SharedStateExposer,
    ) -> Self
    {
        let genesis_hash = data_man.get_cur_consensus_era_genesis_hash();
        ConsensusGraph::with_era_genesis_block(
            conf,
            vm,
            txpool,
            statistics,
            data_man,
            pow_config,
            &genesis_hash,
            state_exposer,
        )
    }

    /// Compute the expected difficulty of a new block given its parent
    pub fn expected_difficulty(&self, parent_hash: &H256) -> U256 {
        let inner = self.inner.read();
        inner.expected_difficulty(parent_hash)
    }

    pub fn update_total_weight_in_past(&self) {
        self.confirmation_meter.update_total_weight_in_past();
    }

    /// Wait for the generation and the execution completion of a block in the
    /// consensus graph. This API is used mainly for testing purpose
    pub fn wait_for_generation(&self, hash: &H256) {
        while !self
            .inner
            .read_recursive()
            .hash_to_arena_indices
            .contains_key(hash)
        {
            sleep(Duration::from_millis(1));
        }
        let best_state_block =
            self.inner.read_recursive().best_state_block_hash();
        self.executor.wait_for_result(best_state_block);
    }

    /// Determine whether the next mined block should have adaptive weight or
    /// not
    pub fn check_mining_adaptive_block(
        &self, inner: &mut ConsensusGraphInner, parent_hash: &H256,
        difficulty: &U256,
    ) -> bool
    {
        let parent_index =
            *inner.hash_to_arena_indices.get(parent_hash).unwrap();
        inner.check_mining_adaptive_block(parent_index, *difficulty)
    }

    /// Convert EpochNumber to height based on the current ConsensusGraph
    pub fn get_height_from_epoch_number(
        &self, epoch_number: EpochNumber,
    ) -> Result<u64, String> {
        Ok(match epoch_number {
            EpochNumber::Earliest => 0,
            EpochNumber::LatestMined => self.best_epoch_number(),
            EpochNumber::LatestState => self.executed_best_state_epoch_number(),
            EpochNumber::Number(num) => {
                let epoch_num = num;
                if epoch_num > self.best_epoch_number() {
                    return Err("Invalid params: expected a numbers with less than largest epoch number.".to_owned());
                }
                epoch_num
            }
        })
    }

    pub fn best_epoch_number(&self) -> u64 {
        self.best_info.read_recursive().best_epoch_number
    }

    pub fn get_block_epoch_number(&self, hash: &H256) -> Option<u64> {
        self.inner.read_recursive().get_block_epoch_number(hash)
    }

    pub fn get_block_hashes_by_epoch(
        &self, epoch_number: EpochNumber,
    ) -> Result<Vec<H256>, String> {
        self.get_height_from_epoch_number(epoch_number)
            .and_then(|height| {
                self.inner.read_recursive().block_hashes_by_epoch(height)
            })
    }

    /// Get the average gas price of the last GAS_PRICE_TRANSACTION_SAMPLE_SIZE
    /// blocks
    pub fn gas_price(&self) -> Option<U256> {
        let inner = self.inner.read();
        let mut last_epoch_number = inner.best_epoch_number();
        let mut number_of_blocks_to_sample = GAS_PRICE_BLOCK_SAMPLE_SIZE;
        let mut tx_hashes = HashSet::new();
        let mut prices = Vec::new();

        loop {
            if number_of_blocks_to_sample == 0 || last_epoch_number == 0 {
                break;
            }
            if prices.len() == GAS_PRICE_TRANSACTION_SAMPLE_SIZE {
                break;
            }
            let mut hashes = inner
                .block_hashes_by_epoch(last_epoch_number.into())
                .unwrap();
            hashes.reverse();
            last_epoch_number -= 1;

            for hash in hashes {
                let block = self
                    .data_man
                    .block_by_hash(&hash, false /* update_cache */)
                    .unwrap();
                for tx in block.transactions.iter() {
                    if tx_hashes.insert(tx.hash()) {
                        prices.push(tx.gas_price().clone());
                        if prices.len() == GAS_PRICE_TRANSACTION_SAMPLE_SIZE {
                            break;
                        }
                    }
                }
                number_of_blocks_to_sample -= 1;
                if number_of_blocks_to_sample == 0 {
                    break;
                }
            }
        }

        prices.sort();
        if prices.is_empty() {
            None
        } else {
            Some(prices[prices.len() / 2])
        }
    }

    fn validate_stated_epoch(
        &self, epoch_number: &EpochNumber,
    ) -> Result<(), String> {
        match epoch_number {
            EpochNumber::LatestMined => {
                return Err("Latest mined epoch is not executed".into());
            }
            EpochNumber::Number(num) => {
                let latest_state_epoch =
                    self.executed_best_state_epoch_number();
                if *num > latest_state_epoch {
                    return Err(format!("Specified epoch {} is not executed, the latest state epoch is {}", num, latest_state_epoch));
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn get_state_db_by_epoch_number(
        &self, epoch_number: EpochNumber,
    ) -> Result<StateDb, String> {
        self.validate_stated_epoch(&epoch_number)?;
        let epoch_number = self.get_height_from_epoch_number(epoch_number)?;
        let hash =
            self.inner.read().get_hash_from_epoch_number(epoch_number)?;
        let maybe_state = self
            .data_man
            .storage_manager
            .get_state_no_commit(SnapshotAndEpochIdRef::new(&hash, None))
            .map_err(|e| format!("Error to get state, err={:?}", e))?;

        let state = match maybe_state {
            Some(state) => state,
            None => {
                return Err(format!(
                    "State for epoch (number={:?} hash={:?}) does not exist",
                    epoch_number, hash
                )
                .into())
            }
        };

        Ok(StateDb::new(state))
    }

    /// Get the code of an address
    pub fn get_code(
        &self, address: H160, epoch_number: EpochNumber,
    ) -> Result<Bytes, String> {
        let state_db =
            self.get_state_db_by_epoch_number(epoch_number.clone())?;
        let acc = match state_db.get_account(&address) {
            Ok(Some(acc)) => acc,
            _ => {
                return Err(format!(
                    "Account {:?} epoch_number={:?} does not exist",
                    address, epoch_number,
                )
                .into())
            }
        };

        match state_db.get_code(&address, &acc.code_hash) {
            Some(code) => Ok(code),
            None => Ok(vec![]),
        }
    }

    /// Get the current balance of an address
    pub fn get_balance(
        &self, address: H160, epoch_number: EpochNumber,
    ) -> Result<U256, String> {
        let state_db = self.get_state_db_by_epoch_number(epoch_number)?;
        Ok(if let Ok(maybe_acc) = state_db.get_account(&address) {
            maybe_acc.map_or(U256::zero(), |acc| acc.balance).into()
        } else {
            0.into()
        })
    }

    /// Force the engine to recompute the deferred state root for a particular
    /// block given a delay.
    pub fn force_compute_blame_and_deferred_state_for_generation(
        &self, parent_block_hash: &H256,
    ) -> Result<(u32, StateRootWithAuxInfo, H256, H256, H256), String> {
        {
            let inner = &mut *self.inner.write();
            let hash = inner
                .get_state_block_with_delay(
                    parent_block_hash,
                    DEFERRED_STATE_EPOCH_COUNT as usize - 1,
                )?
                .clone();
            self.executor.compute_state_for_block(&hash, inner)?;
        }
        self.executor.get_blame_and_deferred_state_for_generation(
            parent_block_hash,
            &self.inner,
        )
    }

    pub fn get_blame_and_deferred_state_for_generation(
        &self, parent_block_hash: &H256,
    ) -> Result<(u32, StateRootWithAuxInfo, H256, H256, H256), String> {
        self.executor.get_blame_and_deferred_state_for_generation(
            parent_block_hash,
            &self.inner,
        )
    }

    /// This function is called after a new block appended to the
    /// ConsensusGraph. Because BestInformation is often queried outside. We
    /// store a version of best_info outside the inner to prevent keep
    /// getting inner locks.
    pub fn update_best_info(&self, inner: &ConsensusGraphInner) {
        let mut best_info = self.best_info.write();
        self.state_exposer.write().consensus_graph.best_block_hash =
            inner.best_block_hash();

        let terminal_hashes = inner.terminal_hashes();
        let (terminal_block_hashes, bounded_terminal_block_hashes) =
            if terminal_hashes.len() > REFEREE_BOUND {
                let mut tmp = Vec::new();
                let best_idx = inner.pivot_chain.last().unwrap();
                for hash in terminal_hashes.iter() {
                    let a_idx = inner.hash_to_arena_indices.get(hash).unwrap();
                    let a_lca = inner.lca(*a_idx, *best_idx);
                    tmp.push((inner.arena[a_lca].height, hash));
                }
                tmp.sort_by(|a, b| Reverse(a.0).cmp(&Reverse(b.0)));
                tmp.split_off(REFEREE_BOUND);
                let bounded_hashes =
                    tmp.iter().map(|(_, b)| (*b).clone()).collect();
                (Some(terminal_hashes), bounded_hashes)
            } else {
                (None, terminal_hashes)
            };

        *best_info = Arc::new(BestInformation {
            best_block_hash: inner.best_block_hash(),
            best_epoch_number: inner.best_epoch_number(),
            current_difficulty: inner.current_difficulty,
            terminal_block_hashes,
            bounded_terminal_block_hashes,
        });
    }

    /// This is the main function that SynchronizationGraph calls to deliver a
    /// new block to the consensus graph.
    pub fn on_new_block(&self, hash: &H256, ignore_body: bool) {
        let _timer =
            MeterTimer::time_func(CONSENSIS_ON_NEW_BLOCK_TIMER.as_ref());
        self.statistics.inc_consensus_graph_processed_block_count();

        let block_opt = if ignore_body {
            None
        } else {
            self.data_man.block_by_hash(hash, true /* update_cache */)
        };

        let header_opt = if ignore_body {
            self.data_man.block_header_by_hash(hash)
        } else {
            None
        };

        {
            let inner = &mut *self.inner.write();
            if !ignore_body {
                let block = block_opt.unwrap();
                debug!(
                    "insert new block into consensus: block_header={:?} tx_count={}, block_size={}",
                    block.block_header,
                    block.transactions.len(),
                    block.size(),
                );
                self.new_block_handler.on_new_block(
                    inner,
                    &self.confirmation_meter,
                    hash,
                    &block.block_header,
                    Some(&block.transactions),
                );
            } else {
                // This `ignore_body` case will only be used when
                // 1. archive node is in `CatchUpRecoverBlockFromDB` phase
                // 2. full node is in `CatchUpRecoverBlockHeaderFromDB`,
                // `CatchUpSyncBlockHeader` or `CatchUpRecoverBlockFromDB` phase
                let header = header_opt.unwrap();
                debug!(
                    "insert new block_header into consensus: block_header={:?}",
                    header
                );
                self.new_block_handler.on_new_block(
                    inner,
                    &self.confirmation_meter,
                    hash,
                    header.as_ref(),
                    None,
                );
            }

            // for full node, we should recover state_valid for pivot block
            let mut pivot_block_state_valid_map =
                self.pivot_block_state_valid_map.lock();
            if !pivot_block_state_valid_map.is_empty()
                && pivot_block_state_valid_map.contains_key(&hash)
            {
                let arena_index =
                    *inner.hash_to_arena_indices.get(&hash).unwrap();
                inner.arena[arena_index].data.state_valid =
                    pivot_block_state_valid_map.remove(&hash).unwrap();
            }

            // we should recover exec_info from db
            if let Some(arena_index) = inner.hash_to_arena_indices.get(hash) {
                if let Some(exe_info) =
                    self.data_man.consensus_graph_execution_info_from_db(hash)
                {
                    inner.execution_info_cache.insert(*arena_index, exe_info);
                }
            }

            self.update_best_info(inner);
            if *hash == self.data_man.get_cur_consensus_era_stable_hash() {
                inner.set_pivot_to_stable(hash);
            }
        }
        self.txpool
            .notify_new_best_info(self.best_info.read().clone());
        *self.latest_inserted_block.lock() = *hash;
    }

    pub fn best_block_hash(&self) -> H256 {
        self.best_info.read_recursive().best_block_hash
    }

    /// Returns the latest epoch with executed state.
    pub fn executed_best_state_epoch_number(&self) -> u64 {
        self.inner
            .read_recursive()
            .executed_best_state_epoch_number()
    }

    /// Returns the latest epoch whose state execution has been enqueued.
    /// And this state should be the `deferred_state` of the block being mined.
    ///
    /// Note that the state may not exist, and the caller should wait for the
    /// result if the state is going to be used.
    pub fn best_state_epoch_number(&self) -> u64 {
        self.inner.read_recursive().best_state_epoch_number()
    }

    pub fn get_hash_from_epoch_number(
        &self, epoch_number: EpochNumber,
    ) -> Result<H256, String> {
        self.get_height_from_epoch_number(epoch_number)
            .and_then(|height| {
                self.inner.read().get_hash_from_epoch_number(height)
            })
    }

    pub fn get_transaction_info_by_hash(
        &self, hash: &H256,
    ) -> Option<(SignedTransaction, Receipt, TransactionAddress)> {
        // We need to hold the inner lock to ensure that tx_address and receipts
        // are consistent
        let inner = self.inner.read();
        if let Some((receipt, address)) =
            inner.get_transaction_receipt_with_address(hash)
        {
            let block = self.data_man.block_by_hash(
                &address.block_hash,
                false, /* update_cache */
            )?;
            let transaction = (*block.transactions[address.index]).clone();
            Some((transaction, receipt, address))
        } else {
            None
        }
    }

    pub fn get_state_root_by_pivot_height(
        &self, pivot_height: u64,
    ) -> Option<H256> {
        let inner = self.inner.read();
        let height = pivot_height + DEFERRED_STATE_EPOCH_COUNT as u64;
        let pivot_index = match height {
            h if h < inner.get_cur_era_genesis_height() => return None,
            h => inner.height_to_pivot_index(h),
        };
        if pivot_index < inner.pivot_chain.len() {
            let pivot_hash = &inner.arena[inner.pivot_chain[pivot_index]].hash;
            return match self
                .data_man
                .consensus_graph_execution_info_from_db(pivot_hash)
            {
                Some(info) => Some(info.original_deferred_state_root),
                None => None,
            };
        }
        None
    }

    pub fn transaction_count(
        &self, address: H160, epoch_number: EpochNumber,
    ) -> Result<U256, String> {
        let state_db = self.get_state_db_by_epoch_number(epoch_number)?;
        let state = State::new(state_db, 0.into(), Default::default());
        state
            .nonce(&address)
            .map_err(|err| format!("Get transaction count error: {:?}", err))
    }

    /// Wait until the best state has been executed, and return the state
    pub fn get_best_state(&self) -> State {
        let best_state_hash = self.inner.read().best_state_block_hash();
        self.executor.wait_for_result(best_state_hash);
        if let Ok(state) = self.data_man.storage_manager.get_state_no_commit(
            SnapshotAndEpochIdRef::new(&best_state_hash, None),
        ) {
            state
                .map(|db| {
                    State::new(StateDb::new(db), 0.into(), Default::default())
                })
                .expect("Best state has been executed")
        } else {
            panic!("get_best_state: Error for hash {}", best_state_hash);
        }
    }

    /// Returns the total number of blocks processed in consensus graph.
    ///
    /// This function should only be used in tests.
    /// If the process crashes and recovered, the blocks in the anticone of the
    /// current checkpoint may not be counted since they will not be
    /// inserted into consensus in the recover process.
    pub fn block_count(&self) -> u64 {
        self.inner.read_recursive().total_processed_block_count()
    }

    /// Estimate the gas of a transaction
    pub fn estimate_gas(&self, tx: &SignedTransaction) -> Result<U256, String> {
        self.call_virtual(tx, EpochNumber::LatestState)
            .map(|(_, gas_used)| gas_used)
    }

    pub fn logs(
        &self, filter: Filter,
    ) -> Result<Vec<LocalizedLogEntry>, FilterError> {
        let block_hashes = if filter.block_hashes.is_none() {
            // at most best_epoch
            let from_epoch = match self
                .get_height_from_epoch_number(filter.from_epoch.clone())
            {
                Ok(num) => num,
                Err(_) => return Ok(vec![]),
            };

            // at most best_epoch
            let to_epoch = self
                .get_height_from_epoch_number(filter.to_epoch.clone())
                .unwrap_or(self.best_epoch_number());

            if from_epoch > to_epoch {
                return Err(FilterError::InvalidEpochNumber {
                    from_epoch,
                    to_epoch,
                });
            }

            let blooms = filter.bloom_possibilities();
            let bloom_match = |block_log_bloom: &Bloom| {
                blooms
                    .iter()
                    .any(|bloom| block_log_bloom.contains_bloom(bloom))
            };

            let inner = self.inner.read();

            let mut blocks = vec![];
            for epoch_number in from_epoch..(to_epoch + 1) {
                let epoch_hash = inner.arena
                    [inner.get_pivot_block_arena_index(epoch_number)]
                .hash;
                for index in &inner.arena
                    [inner.get_pivot_block_arena_index(epoch_number)]
                .data
                .ordered_executable_epoch_blocks
                {
                    let hash = inner.arena[*index].hash;
                    if let Some(block_log_bloom) = self
                        .data_man
                        .block_execution_result_by_hash_with_epoch(
                            &hash,
                            &epoch_hash,
                            false, /* update_cache */
                        )
                        .map(|r| r.bloom)
                    {
                        if !bloom_match(&block_log_bloom) {
                            continue;
                        }
                    }
                    blocks.push(hash);
                }
            }

            blocks
        } else {
            filter.block_hashes.as_ref().unwrap().clone()
        };

        Ok(self.logs_from_blocks(
            block_hashes,
            |entry| filter.matches(entry),
            filter.limit,
        ))
    }

    /// Returns logs matching given filter. The order of logs returned will be
    /// the same as the order of the blocks provided. And it's the callers
    /// responsibility to sort blocks provided in advance.
    pub fn logs_from_blocks<F>(
        &self, mut blocks: Vec<H256>, matches: F, limit: Option<usize>,
    ) -> Vec<LocalizedLogEntry>
    where
        F: Fn(&LogEntry) -> bool + Send + Sync,
        Self: Sized,
    {
        // sort in reverse order
        blocks.reverse();

        let mut logs = blocks
            .chunks(128)
            .flat_map(move |blocks_chunk| {
                blocks_chunk.into_par_iter()
                    .filter_map(|hash|
                        self.inner.read().block_receipts_by_hash(&hash, false /* update_cache */).map(|r| (hash, (*r).clone()))
                    )
                    .filter_map(|(hash, receipts)| self.data_man.block_by_hash(&hash, false /* update_cache */).map(|b| (hash, receipts, b.transaction_hashes())))
                    .flat_map(|(hash, mut receipts, mut hashes)| {
                        if receipts.len() != hashes.len() {
                            warn!("Block ({}) has different number of receipts ({}) to transactions ({}). Database corrupt?", hash, receipts.len(), hashes.len());
                            assert!(false);
                        }
                        let mut log_index = receipts.iter().fold(0, |sum, receipt| sum + receipt.logs.len());

                        let receipts_len = receipts.len();
                        hashes.reverse();
                        receipts.reverse();
                        receipts.into_iter()
                            .map(|receipt| receipt.logs)
                            .zip(hashes)
                            .enumerate()
                            .flat_map(move |(index, (mut logs, tx_hash))| {
                                let current_log_index = log_index;
                                let no_of_logs = logs.len();
                                log_index -= no_of_logs;

                                logs.reverse();
                                logs.into_iter()
                                    .enumerate()
                                    .map(move |(i, log)| LocalizedLogEntry {
                                        entry: log,
                                        block_hash: *hash,
                                        // TODO
                                        block_number: 0,
                                        transaction_hash: tx_hash,
                                        // iterating in reverse order
                                        transaction_index: receipts_len - index - 1,
                                        transaction_log_index: no_of_logs - i - 1,
                                        log_index: current_log_index - i - 1,
                                    })
                            })
                            .filter(|log_entry| matches(&log_entry.entry))
                            .take(limit.unwrap_or(::std::usize::MAX))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .take(limit.unwrap_or(::std::usize::MAX))
            .collect::<Vec<LocalizedLogEntry>>();
        logs.reverse();
        logs
    }

    pub fn call_virtual(
        &self, tx: &SignedTransaction, epoch: EpochNumber,
    ) -> Result<(Vec<u8>, U256), String> {
        // only allow to call against stated epoch
        self.validate_stated_epoch(&epoch)?;
        let epoch_id = self.get_hash_from_epoch_number(epoch)?;
        self.executor.call_virtual(tx, &epoch_id)
    }

    // FIXME store this in BlockDataManager
    /// Return the sequence number of the current era genesis hash.
    pub fn current_era_genesis_seq_num(&self) -> u64 {
        let inner = self.inner.read_recursive();
        inner.arena[inner.cur_era_genesis_block_arena_index]
            .data
            .sequence_number
    }

    /// Get the number of processed blocks (i.e., the number of calls to
    /// on_new_block()
    pub fn get_processed_block_count(&self) -> usize {
        self.statistics.get_consensus_graph_processed_block_count()
    }

    /// This function is called when preparing a new block for generation. It
    /// propagate the ReadGuard up to make the read-lock live longer so that
    /// the whole block packing process can be atomic.
    pub fn get_best_info(&self) -> Arc<BestInformation> {
        self.best_info.read_recursive().clone()
    }

    /// Get the set of block hashes inside an epoch
    pub fn block_hashes_by_epoch(
        &self, epoch_number: EpochNumber,
    ) -> Result<Vec<H256>, String> {
        self.get_height_from_epoch_number(epoch_number)
            .and_then(|height| {
                self.inner.read_recursive().block_hashes_by_epoch(height)
            })
    }

    /// This function returns the set of blocks that are two eras farther from
    /// current era. They can be safely garbage collected.
    pub fn retrieve_old_era_blocks(&self) -> Option<H256> {
        self.inner.read().old_era_block_set.lock().pop_front()
    }

    /// Find a trusted blame block for checkpoint
    pub fn get_trusted_blame_block(&self, stable_hash: &H256) -> Option<H256> {
        let inner = self.inner.read();
        inner.get_trusted_blame_block(stable_hash)
    }

    pub fn first_trusted_header_starting_from(
        &self, height: u64, blame_bound: Option<u32>,
    ) -> Option<u64> {
        // TODO(thegaram): change logic to work with arbitrary height, not just
        // the ones from the current era (i.e. use epoch instead of pivot index)
        let inner = self.inner.read();

        // for now, make sure to avoid underflow
        let pivot_index = match height {
            h if h < inner.get_cur_era_genesis_height() => return None,
            h => inner.height_to_pivot_index(h),
        };

        let trusted =
            inner.find_first_trusted_starting_from(pivot_index, blame_bound);
        trusted.map(|index| inner.pivot_index_to_height(index))
    }

    /// construct_pivot_state() rebuild pivot chain state info from db
    /// avoiding intermediate redundant computation triggered by
    /// on_new_block().
    pub fn construct_pivot_state(&self) {
        let inner = &mut *self.inner.write();
        self.new_block_handler.construct_pivot_state(inner);
    }

    pub fn best_info(&self) -> Arc<BestInformation> {
        self.best_info.read().clone()
    }
}

impl Drop for ConsensusGraph {
    fn drop(&mut self) { self.executor.stop(); }
}
