// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

extern crate futures;
extern crate lru_time_cache;

use futures::Future;
use lru_time_cache::LruCache;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::{
    light_protocol::{
        common::{FullPeerState, Peers, UniqueId},
        message::{GetReceipts, ReceiptsWithEpoch},
        Error, ErrorKind,
    },
    message::Message,
    network::{NetworkContext, PeerId},
    parameters::light::{
        CACHE_TIMEOUT, MAX_RECEIPTS_IN_FLIGHT, RECEIPT_REQUEST_BATCH_SIZE,
        RECEIPT_REQUEST_TIMEOUT,
    },
    primitives::{BlockHeaderBuilder, Receipt},
};

use super::{
    common::{FutureItem, KeyOrdered, SyncManager},
    witnesses::Witnesses,
};

#[derive(Debug)]
struct Statistics {
    cached: usize,
    in_flight: usize,
    waiting: usize,
}

// prioritize higher epochs
type MissingReceipts = KeyOrdered<u64>;

pub struct Receipts {
    // series of unique request ids
    request_id_allocator: Arc<UniqueId>,

    // sync and request manager
    sync_manager: SyncManager<u64, MissingReceipts>,

    // epoch receipts received from full node
    verified: Arc<RwLock<LruCache<u64, Vec<Vec<Receipt>>>>>,

    // witness sync manager
    witnesses: Arc<Witnesses>,
}

impl Receipts {
    pub fn new(
        peers: Arc<Peers<FullPeerState>>, request_id_allocator: Arc<UniqueId>,
        witnesses: Arc<Witnesses>,
    ) -> Self
    {
        let sync_manager = SyncManager::new(peers.clone());

        let cache = LruCache::with_expiry_duration(*CACHE_TIMEOUT);
        let verified = Arc::new(RwLock::new(cache));

        Receipts {
            request_id_allocator,
            sync_manager,
            verified,
            witnesses,
        }
    }

    #[inline]
    fn get_statistics(&self) -> Statistics {
        Statistics {
            cached: self.verified.read().len(),
            in_flight: self.sync_manager.num_in_flight(),
            waiting: self.sync_manager.num_waiting(),
        }
    }

    #[inline]
    pub fn request(
        &self, epoch: u64,
    ) -> impl Future<Item = Vec<Vec<Receipt>>, Error = Error> {
        if epoch == 0 {
            self.verified.write().insert(0, vec![]);
        }

        if !self.verified.read().contains_key(&epoch) {
            let missing = MissingReceipts::new(epoch);
            self.sync_manager.insert_waiting(std::iter::once(missing));
        }

        FutureItem::new(epoch, self.verified.clone())
    }

    #[inline]
    pub fn receive(
        &self, receipts: impl Iterator<Item = ReceiptsWithEpoch>,
    ) -> Result<(), Error> {
        for ReceiptsWithEpoch { epoch, receipts } in receipts {
            info!("Validating receipts {:?} with epoch {}", receipts, epoch);
            self.validate_receipts(epoch, &receipts)?;

            self.verified.write().insert(epoch, receipts);
            self.sync_manager.remove_in_flight(&epoch);
        }

        Ok(())
    }

    #[inline]
    pub fn receive_single(
        &self, epoch: u64, receipts: Vec<Vec<Receipt>>,
    ) -> Result<(), Error> {
        let item = ReceiptsWithEpoch { epoch, receipts };
        self.receive(std::iter::once(item))
    }

    #[inline]
    pub fn clean_up(&self) {
        // remove timeout in-flight requests
        let timeout = *RECEIPT_REQUEST_TIMEOUT;
        let receiptss = self.sync_manager.remove_timeout_requests(timeout);
        self.sync_manager.insert_waiting(receiptss.into_iter());

        // trigger cache cleanup
        self.verified.write().get(&Default::default());
    }

    #[inline]
    fn send_request(
        &self, io: &dyn NetworkContext, peer: PeerId, epochs: Vec<u64>,
    ) -> Result<(), Error> {
        info!("send_request peer={:?} epochs={:?}", peer, epochs);

        if epochs.is_empty() {
            return Ok(());
        }

        let msg: Box<dyn Message> = Box::new(GetReceipts {
            request_id: self.request_id_allocator.next(),
            epochs,
        });

        msg.send(io, peer)?;
        Ok(())
    }

    #[inline]
    pub fn sync(&self, io: &dyn NetworkContext) {
        info!("receipt sync statistics: {:?}", self.get_statistics());

        self.sync_manager.sync(
            MAX_RECEIPTS_IN_FLIGHT,
            RECEIPT_REQUEST_BATCH_SIZE,
            |peer, epochs| self.send_request(io, peer, epochs),
        );
    }

    #[inline]
    fn validate_receipts(
        &self, epoch: u64, receipts: &Vec<Vec<Receipt>>,
    ) -> Result<(), Error> {
        // calculate received receipts root
        // convert Vec<Vec<Receipt>> -> Vec<Arc<Vec<Receipt>>>
        // for API compatibility
        let rs = receipts
            .clone()
            .into_iter()
            .map(|rs| Arc::new(rs))
            .collect();

        let received = BlockHeaderBuilder::compute_block_receipts_root(&rs);

        // retrieve local receipts root
        let local = match self.witnesses.root_hashes_of(epoch) {
            Some((_, receipts_root, _)) => receipts_root,
            None => {
                warn!(
                    "Receipt root not found, epoch={}, receipts={:?}",
                    epoch, receipts
                );
                return Err(ErrorKind::InternalError.into());
            }
        };

        // check
        if received != local {
            warn!(
                "Receipt validation failed, received={:?}, local={:?}",
                received, local
            );
            return Err(ErrorKind::InvalidBloom.into());
        }

        Ok(())
    }
}
