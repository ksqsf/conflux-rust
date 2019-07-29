// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

/// CowNodeRef facilities access and modification to trie nodes in multi-version
/// MPT. It offers read-only access to the original trie node, and creates an
/// unique owned trie node once there is any modification. The ownership is
/// maintained centralized in owned_node_set which is passed into many methods
/// as argument. When CowNodeRef is created from an owned node, the ownership is
/// transferred into the CowNodeRef object. The visitor of MPT makes sure that
/// ownership of any trie node is not transferred more than once at the same
/// time.
pub struct CowNodeRef {
    // If a CowNodeRef is owned, the trie node must be in memory (allocator),
    // and the node ref must be in an owned node set.
    owned: bool,
    pub node_ref: NodeRefDeltaMpt,
}

pub struct MaybeOwnedTrieNode<'a> {
    trie_node: &'a TrieNodeDeltaMptCell,
}

type GuardedMaybeOwnedTrieNodeAsCowCallParam<'c> = GuardedValue<
    Option<MutexGuard<'c, CacheManagerDeltaMpt>>,
    MaybeOwnedTrieNodeAsCowCallParam,
>;

/// This class can only be meaningfully used internally by CowNodeRef.
pub struct MaybeOwnedTrieNodeAsCowCallParam {
    trie_node: *mut TrieNodeDeltaMpt,
}

impl MaybeOwnedTrieNodeAsCowCallParam {
    // Returns a mutable reference to trie node when the trie_node is owned,
    // however the precondition is unchecked.
    unsafe fn owned_as_mut_unchecked<'a>(
        &mut self,
    ) -> &'a mut TrieNodeDeltaMpt {
        &mut *self.trie_node
    }

    /// Do not implement in a trait to keep the call private.
    fn as_ref<'a>(&self) -> &'a TrieNodeDeltaMpt { unsafe { &*self.trie_node } }
}

impl<'a, GuardType> GuardedValue<GuardType, MaybeOwnedTrieNode<'a>> {
    pub fn take(
        x: Self,
    ) -> GuardedValue<GuardType, MaybeOwnedTrieNodeAsCowCallParam> {
        let (guard, value) = x.into();
        GuardedValue::new(
            guard,
            MaybeOwnedTrieNodeAsCowCallParam {
                trie_node: value.trie_node.get(),
            },
        )
    }
}

impl<'a, GuardType> GuardedValue<GuardType, &'a TrieNodeDeltaMptCell> {
    pub fn into_wrapped(
        x: Self,
    ) -> GuardedValue<GuardType, MaybeOwnedTrieNode<'a>> {
        let (guard, value) = x.into();
        GuardedValue::new(guard, MaybeOwnedTrieNode { trie_node: value })
    }
}

impl<'a> MaybeOwnedTrieNode<'a> {
    pub fn take(x: Self) -> MaybeOwnedTrieNodeAsCowCallParam {
        MaybeOwnedTrieNodeAsCowCallParam {
            trie_node: x.trie_node.get(),
        }
    }
}

impl<'a> Deref for MaybeOwnedTrieNode<'a> {
    type Target = TrieNodeDeltaMpt;

    fn deref(&self) -> &Self::Target { unsafe { &*self.trie_node.get() } }
}

impl<'a> MaybeOwnedTrieNode<'a> {
    pub unsafe fn owned_as_mut_unchecked(
        &mut self,
    ) -> &'a mut TrieNodeDeltaMpt {
        self.trie_node.get_ref_mut()
    }
}

impl CowNodeRef {
    pub fn new_uninitialized_node<'a>(
        allocator: AllocatorRefRefDeltaMpt<'a>,
        owned_node_set: &mut OwnedNodeSet,
    ) -> Result<(Self, SlabVacantEntryDeltaMpt<'a>)>
    {
        let (node_ref, new_entry) =
            NodeMemoryManagerDeltaMpt::new_node(allocator, None)?;
        owned_node_set.insert(node_ref.clone());

        Ok((
            Self {
                owned: true,
                node_ref,
            },
            new_entry,
        ))
    }

    pub fn new(
        node_ref: NodeRefDeltaMpt, owned_node_set: &OwnedNodeSet,
    ) -> Self {
        Self {
            owned: owned_node_set.contains(&node_ref),
            node_ref,
        }
    }

    /// Take the value out of Self. Self is safe to drop.
    pub fn take(&mut self) -> Self {
        let ret = Self {
            owned: self.owned,
            node_ref: self.node_ref.clone(),
        };

        self.owned = false;
        ret
    }
}

impl Drop for CowNodeRef {
    /// Assert that the CowNodeRef doesn't own something.
    fn drop(&mut self) {
        assert_eq!(false, self.owned);
    }
}

impl CowNodeRef {
    pub fn is_owned(&self) -> bool { self.owned }

    // FIXME: refactor node_memory_manager?
    fn convert_to_owned<'a>(
        &mut self, _node_memory_manager: &'a NodeMemoryManagerDeltaMpt,
        allocator: AllocatorRefRefDeltaMpt<'a>,
        owned_node_set: &mut OwnedNodeSet,
    ) -> Result<Option<SlabVacantEntryDeltaMpt<'a>>>
    {
        if self.owned {
            Ok(None)
        } else {
            let original_db_key = match self.node_ref {
                NodeRefDeltaMpt::Committed { db_key } => Some(db_key),
                NodeRefDeltaMpt::Dirty { .. } => {
                    self.node_ref.original_db_key()
                }
            };
            // Similar to Self::new_uninitialized_node().
            let (node_ref, new_entry) = NodeMemoryManagerDeltaMpt::new_node(
                &allocator,
                original_db_key,
            )?;
            owned_node_set.insert(node_ref.clone());
            self.node_ref = node_ref;
            self.owned = true;

            Ok(Some(new_entry))
        }
    }

    /// The returned MaybeOwnedTrieNode is considered a borrow of CowNodeRef
    /// because when it's owned user may use it as mutable borrow of
    /// TrieNode. The lifetime is bounded by allocator for slab and by
    /// node_memory_manager for cache.
    ///
    /// Lifetime of cache is separated because holding the lock itself shouldn't
    /// prevent any further calls on self.
    pub fn get_trie_node<'a, 'c: 'a>(
        &'a mut self, node_memory_manager: &'c NodeMemoryManagerDeltaMpt,
        allocator: AllocatorRefRefDeltaMpt<'a>,
    ) -> Result<
        GuardedValue<
            Option<MutexGuard<'c, CacheManagerDeltaMpt>>,
            MaybeOwnedTrieNode<'a>,
        >,
    >
    {
        Ok(GuardedValue::into_wrapped(
            node_memory_manager.node_cell_with_cache_manager(
                &allocator,
                self.node_ref.clone(),
                node_memory_manager.get_cache_manager(),
                &mut false,
            )?,
        ))
    }

    /// The trie node obtained from CowNodeRef is invalidated at the same time
    /// of delete_node and into_child. when the trie node obtained from
    /// CowNodeRef is through get_trie_node, because the lifetime
    /// is shorter.
    // FIXME: the comment above seems broken.
    pub fn delete_node(
        mut self, node_memory_manager: &NodeMemoryManagerDeltaMpt,
        owned_node_set: &mut OwnedNodeSet,
    )
    {
        if self.owned {
            node_memory_manager.free_owned_node(&mut self.node_ref);
            owned_node_set.remove(&self.node_ref);
            self.owned = false;
        }
    }

    // FIXME: maybe forbid calling for un-owned node? Check
    // SubTrieVisitor#delete, #delete_all, etc.
    pub fn into_child(mut self) -> Option<NodeRefDeltaMptCompact> {
        if self.owned {
            self.owned = false;
        }
        Some(self.node_ref.clone().into())
    }

    /// The deletion is always successful. When return value is Error, the
    /// failing part is iteration.
    pub fn delete_subtree(
        mut self, trie: &DeltaMpt, owned_node_set: &OwnedNodeSet,
        guarded_trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
        key_prefix: CompressedPathRaw, values: &mut Vec<(Vec<u8>, Box<[u8]>)>,
    ) -> Result<()>
    {
        if self.owned {
            if guarded_trie_node.as_ref().as_ref().has_value() {
                assert_eq!(key_prefix.end_mask(), 0);
                values.push((
                    key_prefix.path_slice().to_vec(),
                    guarded_trie_node.as_ref().as_ref().value_clone().unwrap(),
                ));
            }

            let children_table =
                guarded_trie_node.as_ref().as_ref().children_table.clone();
            // Free the lock for trie_node.
            // FIXME: try to share the lock.
            drop(guarded_trie_node);

            let node_memory_manager = trie.get_node_memory_manager();
            let allocator = node_memory_manager.get_allocator();
            for (i, node_ref) in children_table.iter() {
                let mut cow_child_node =
                    Self::new((*node_ref).into(), owned_node_set);
                let child_node = cow_child_node
                    .get_trie_node(node_memory_manager, &allocator)?;
                let key_prefix = CompressedPathRaw::concat(
                    &key_prefix,
                    i,
                    &child_node.compressed_path_ref(),
                );
                let child_node = GuardedValue::take(child_node);
                cow_child_node.delete_subtree(
                    trie,
                    owned_node_set,
                    child_node,
                    key_prefix,
                    values,
                )?;
            }

            node_memory_manager.free_owned_node(&mut self.node_ref);
            self.owned = false;
            Ok(())
        } else {
            self.iterate_internal(
                owned_node_set,
                trie,
                guarded_trie_node,
                key_prefix,
                values,
            )
        }
    }

    // FIXME: Revisit methods including a trie parameter are subject
    // FIXME: to refactor because we are going to separate node
    // FIXME: memory manager from mpt, and we are probably going
    // FIXME: to have a new trait for a MPT.
    fn commit_dirty_recurse_into_children(
        &mut self, trie: &DeltaMpt, owned_node_set: &mut OwnedNodeSet,
        children_merkle_map: &mut ChildrenMerkleMap,
        trie_node: &mut TrieNodeDeltaMpt,
        commit_transaction: &mut AtomicCommitTransaction,
        cache_manager: &mut CacheManagerDeltaMpt,
        allocator_ref: AllocatorRefRefDeltaMpt,
    ) -> Result<()>
    {
        for (_i, node_ref_mut) in trie_node.children_table.iter_mut() {
            let node_ref = node_ref_mut.clone();
            let mut cow_child_node = Self::new(node_ref.into(), owned_node_set);
            if cow_child_node.is_owned() {
                let trie_node = unsafe {
                    trie.get_node_memory_manager().dirty_node_as_mut_unchecked(
                        allocator_ref,
                        &mut cow_child_node.node_ref,
                    )
                };
                let commit_result = cow_child_node.commit_dirty_recursively(
                    trie,
                    owned_node_set,
                    children_merkle_map,
                    trie_node,
                    commit_transaction,
                    cache_manager,
                    allocator_ref,
                );

                if commit_result.is_ok() {
                    // An owned child TrieNode now have a new NodeRef.
                    *node_ref_mut = cow_child_node.into_child().unwrap();
                } else {
                    cow_child_node.into_child();

                    commit_result?;
                }
            }
        }
        Ok(())
    }

    fn set_merkle(
        &mut self, children_merkles: MaybeMerkleTableRef,
        trie_node: &mut TrieNodeDeltaMpt,
    ) -> MerkleHash
    {
        let path_merkle = compute_merkle(
            trie_node.compressed_path_ref(),
            children_merkles,
            trie_node.value_as_slice().into_option(),
        );
        trie_node.merkle_hash = path_merkle;

        path_merkle
    }

    /// Get if unowned, compute if owned.
    pub fn get_or_compute_merkle(
        &mut self, trie: &DeltaMpt, owned_node_set: &mut OwnedNodeSet,
        children_merkle_map: &mut ChildrenMerkleMap,
        allocator_ref: AllocatorRefRefDeltaMpt,
    ) -> Result<MerkleHash>
    {
        if self.owned {
            let trie_node = unsafe {
                trie.get_node_memory_manager().dirty_node_as_mut_unchecked(
                    allocator_ref,
                    &mut self.node_ref,
                )
            };

            let children_merkles = self.get_or_compute_children_merkles(
                trie,
                trie_node,
                owned_node_set,
                children_merkle_map,
                allocator_ref,
            )?;

            let merkle = self.set_merkle(children_merkles.as_ref(), trie_node);

            Ok(merkle)
        } else {
            let mut load_from_db = false;
            let trie_node = trie
                .get_node_memory_manager()
                .node_as_ref_with_cache_manager(
                    allocator_ref,
                    self.node_ref.clone(),
                    trie.get_node_memory_manager().get_cache_manager(),
                    &mut load_from_db,
                )?;
            if load_from_db {
                trie.get_node_memory_manager()
                    .compute_merkle_db_loads
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(trie_node.merkle_hash)
        }
    }

    fn compute_children_merkles(
        &mut self, trie: &DeltaMpt, trie_node: &mut TrieNodeDeltaMpt,
        owned_node_set: &mut OwnedNodeSet,
        children_merkle_map: &mut ChildrenMerkleMap,
        allocator_ref: AllocatorRefRefDeltaMpt,
    ) -> Result<MaybeMerkleTable>
    {
        let mut merkles = ChildrenMerkleTable::default();
        for (i, maybe_node_ref) in trie_node.children_table.iter_non_skip() {
            match maybe_node_ref {
                None => merkles[i as usize] = MERKLE_NULL_NODE,
                Some(node_ref) => {
                    let mut cow_child_node =
                        Self::new((*node_ref).into(), owned_node_set);
                    let result = cow_child_node.get_or_compute_merkle(
                        trie,
                        owned_node_set,
                        children_merkle_map,
                        allocator_ref,
                    );
                    // There is no change to the child reference so the
                    // return value is dropped.
                    cow_child_node.into_child();

                    merkles[i as usize] = result?;
                }
            }
        }

        // TODO(mk) avoid memcpy when original_db_key is some.
        match &self.node_ref {
            NodeRefDeltaMpt::Dirty {
                original_db_key: Some(key),
                ..
            } => {
                children_merkle_map.insert(*key, merkles.clone());
            }
            _ => {}
        }

        Ok(Some(merkles))
    }

    fn get_or_compute_children_merkles(
        &mut self, trie: &DeltaMpt, trie_node: &mut TrieNodeDeltaMpt,
        owned_node_set: &mut OwnedNodeSet,
        children_merkle_map: &mut ChildrenMerkleMap,
        allocator_ref: AllocatorRefRefDeltaMpt,
    ) -> Result<MaybeMerkleTable>
    {
        match (
            trie_node.children_table.get_children_count(),
            self.node_ref.original_db_key(),
        ) {
            (0, _) => Ok(None),
            (_, None) => {
                return self.compute_children_merkles(
                    trie,
                    trie_node,
                    owned_node_set,
                    children_merkle_map,
                    allocator_ref,
                );
            }
            (_, Some(db_key)) => {
                let merkles = trie
                    .get_node_memory_manager()
                    .load_children_merkles(db_key, children_merkle_map)
                    .map(|x| x.cloned());

                match merkles {
                    Err(_) | Ok(None) => {
                        // merkles not found in db or errors occurred in merkle
                        // db, fallback
                        self.compute_children_merkles(
                            trie,
                            trie_node,
                            owned_node_set,
                            children_merkle_map,
                            allocator_ref,
                        )
                    }
                    Ok(Some(mut merkles)) => {
                        for (i, maybe_node_ref) in
                            trie_node.children_table.iter_non_skip()
                        {
                            match maybe_node_ref {
                                None => {
                                    // child might be deleted, setting to null
                                    // anyway
                                    merkles[i as usize] = MERKLE_NULL_NODE;
                                }
                                Some(compact_node_ref)
                                    if compact_node_ref.is_dirty() =>
                                {
                                    let mut cow_child_node = Self::new(
                                        (*compact_node_ref).into(),
                                        owned_node_set,
                                    );
                                    let result = cow_child_node
                                        .get_or_compute_merkle(
                                            trie,
                                            owned_node_set,
                                            children_merkle_map,
                                            allocator_ref,
                                        );
                                    cow_child_node.into_child().unwrap();
                                    merkles[i as usize] = result?;
                                }
                                _ => {
                                    // node_ref is Committed.
                                    // In this case, the merkles loaded from db
                                    // are correct.  Do nothing here.
                                }
                            }
                        }
                        Ok(Some(merkles))
                    }
                }
            }
        }
    }

    // FIXME: unit test.
    // FIXME: It's unnecessary to use owned_node_set for read-only access.
    // FIXME: Where to put which method? CowNodeRef, MVMPT / MPT,
    // FIXME: SubTrieVisitor?
    pub fn iterate_internal(
        &self, owned_node_set: &OwnedNodeSet, trie: &DeltaMpt,
        guarded_trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
        key_prefix: CompressedPathRaw, values: &mut Vec<(Vec<u8>, Box<[u8]>)>,
    ) -> Result<()>
    {
        if guarded_trie_node.as_ref().as_ref().has_value() {
            assert_eq!(key_prefix.end_mask(), 0);
            values.push((
                key_prefix.path_slice().to_vec(),
                guarded_trie_node.as_ref().as_ref().value_clone().unwrap(),
            ));
        }

        let children_table =
            guarded_trie_node.as_ref().as_ref().children_table.clone();
        // Free the lock for trie_node.
        // FIXME: try to share the lock.
        drop(guarded_trie_node);

        let node_memory_manager = trie.get_node_memory_manager();
        let allocator = node_memory_manager.get_allocator();
        for (i, node_ref) in children_table.iter() {
            let mut cow_child_node =
                Self::new((*node_ref).into(), owned_node_set);
            let child_node = cow_child_node
                .get_trie_node(node_memory_manager, &allocator)?;
            let key_prefix = CompressedPathRaw::concat(
                &key_prefix,
                i,
                &child_node.compressed_path_ref(),
            );
            let child_node = GuardedValue::take(child_node);
            cow_child_node.iterate_internal(
                owned_node_set,
                trie,
                child_node,
                key_prefix,
                values,
            )?;
        }

        Ok(())
    }

    unsafe fn get_precomputed_children_merkles_unchecked<'a: 'b, 'b>(
        &'b self, children_merkle_map: &'a ChildrenMerkleMap,
    ) -> Option<&'a ChildrenMerkleTable> {
        match &self.node_ref {
            NodeRefDeltaMpt::Committed { .. } => unreachable_unchecked(),
            NodeRefDeltaMpt::Dirty {
                original_db_key: Some(key),
                ..
            } => children_merkle_map.get(key),
            _ => None,
        }
    }

    /// Recursively commit dirty nodes.
    pub fn commit_dirty_recursively(
        &mut self, trie: &DeltaMpt, owned_node_set: &mut OwnedNodeSet,
        children_merkle_map: &mut ChildrenMerkleMap,
        trie_node: &mut TrieNodeDeltaMpt,
        commit_transaction: &mut AtomicCommitTransaction,
        cache_manager: &mut CacheManagerDeltaMpt,
        allocator_ref: AllocatorRefRefDeltaMpt,
    ) -> Result<bool>
    {
        if self.owned {
            self.commit_dirty_recurse_into_children(
                trie,
                owned_node_set,
                children_merkle_map,
                trie_node,
                commit_transaction,
                cache_manager,
                allocator_ref,
            )?;

            let db_key = commit_transaction.info.row_number.value;
            commit_transaction.transaction.put(
                COL_DELTA_TRIE,
                commit_transaction.info.row_number.to_string().as_bytes(),
                trie_node.rlp_bytes().as_slice(),
            );

            // Commit children merkles, using the current DB key as the key for
            // future lookups. Cached entries are evicted because
            // they may interfere with db keys. (Note we used original_db_key as
            // key in the children merkle map.)
            if let Some(merkles) = unsafe {
                self.get_precomputed_children_merkles_unchecked(
                    children_merkle_map,
                )
            } {
                commit_transaction.transaction.put(
                    COL_CHILDREN_MERKLES,
                    commit_transaction.info.row_number.to_string().as_bytes(),
                    &rlp::encode_list(merkles).into_boxed_slice(),
                );
            }
            if let NodeRefDeltaMpt::Dirty {
                original_db_key: Some(key),
                ..
            } = &self.node_ref
            {
                children_merkle_map.remove(key);
            }

            commit_transaction.info.row_number =
                commit_transaction.info.row_number.get_next()?;

            let slot = match &self.node_ref {
                NodeRefDeltaMpt::Dirty { index, .. } => *index,
                _ => unsafe { unreachable_unchecked() },
            };
            let committed_node_ref = NodeRefDeltaMpt::Committed { db_key };
            owned_node_set.insert(committed_node_ref.clone());
            // We insert the new node_ref into owned_node_set first because in
            // general inserting to a set may fail, even though it
            // doesn't fail for the current implementation.
            //
            // When it fails to insert into cache, it's fine to have an extra
            // entry in owned_node_set because there is no-op in reverting in
            // this case.
            cache_manager.insert_to_node_ref_map_and_call_cache_access(
                db_key,
                slot,
                trie.get_node_memory_manager(),
            )?;
            owned_node_set.remove(&self.node_ref);
            self.node_ref = committed_node_ref;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn cow_merge_path(
        self, trie: &DeltaMpt, owned_node_set: &mut OwnedNodeSet,
        trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
        child_node_ref: NodeRefDeltaMpt, child_index: u8,
    ) -> Result<CowNodeRef>
    {
        let node_memory_manager = trie.get_node_memory_manager();
        let allocator = node_memory_manager.get_allocator();

        let mut child_node_cow =
            CowNodeRef::new(child_node_ref, owned_node_set);
        let compressed_path_ref =
            trie_node.as_ref().as_ref().compressed_path_ref();
        let path_prefix = CompressedPathRaw::new(
            compressed_path_ref.path_slice(),
            compressed_path_ref.end_mask(),
        );
        // FIXME: Here we may hold the lock and get the trie node for the child
        // FIXME: node. think about it.
        drop(trie_node);
        // COW modify child,
        // FIXME: error processing. Error happens when child node isn't dirty.
        // FIXME: State can be easily reverted if the trie node containing the
        // FIXME: value or itself isn't dirty as well. However if a
        // FIXME: dirty child node was removed, recovering the state
        // FIXME: becomes difficult.
        let child_trie_node =
            child_node_cow.get_trie_node(node_memory_manager, &allocator)?;
        let new_path = child_trie_node.path_prepended(path_prefix, child_index);

        // FIXME: if child_trie_node isn't owned, but node_cow is owned, modify
        // FIXME: node_cow.
        let child_trie_node = GuardedValue::take(child_trie_node);
        child_node_cow.cow_set_compressed_path(
            &node_memory_manager,
            owned_node_set,
            new_path,
            child_trie_node,
        )?;
        self.delete_node(node_memory_manager, owned_node_set);

        Ok(child_node_cow)
    }

    /// When the node is unowned, it doesn't make sense to do copy-on-write
    /// creation because the new node will be deleted immediately.
    pub unsafe fn delete_value_unchecked_followed_by_node_deletion(
        &mut self, mut trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
    ) -> Box<[u8]> {
        if self.owned {
            trie_node
                .as_mut()
                .owned_as_mut_unchecked()
                .delete_value_unchecked()
        } else {
            trie_node.as_ref().as_ref().value_clone().unwrap()
        }
    }

    pub fn cow_set_compressed_path(
        &mut self, node_memory_manager: &NodeMemoryManagerDeltaMpt,
        owned_node_set: &mut OwnedNodeSet, path: CompressedPathRaw,
        trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
    ) -> Result<()>
    {
        let path_to_take = Cell::new(Some(path));

        self.cow_modify_with_operation(
            node_memory_manager,
            &node_memory_manager.get_allocator(),
            owned_node_set,
            trie_node,
            |owned_trie_node| {
                owned_trie_node
                    .set_compressed_path(path_to_take.replace(None).unwrap())
            },
            |read_only_trie_node| {
                (
                    unsafe {
                        read_only_trie_node.copy_and_replace_fields(
                            None,
                            path_to_take.replace(None),
                            None,
                        )
                    },
                    (),
                )
            },
        )
    }

    pub unsafe fn cow_delete_value_unchecked(
        &mut self, node_memory_manager: &NodeMemoryManagerDeltaMpt,
        owned_node_set: &mut OwnedNodeSet,
        trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
    ) -> Result<Box<[u8]>>
    {
        self.cow_modify_with_operation(
            node_memory_manager,
            &node_memory_manager.get_allocator(),
            owned_node_set,
            trie_node,
            |owned_trie_node| owned_trie_node.delete_value_unchecked(),
            |read_only_trie_node| {
                (
                    read_only_trie_node.copy_and_replace_fields(
                        Some(None),
                        None,
                        None,
                    ),
                    read_only_trie_node.value_clone().unwrap(),
                )
            },
        )
    }

    pub fn cow_replace_value_valid(
        &mut self, node_memory_manager: &NodeMemoryManagerDeltaMpt,
        owned_node_set: &mut OwnedNodeSet,
        trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam, value: &[u8],
    ) -> Result<MptValue<Box<[u8]>>>
    {
        self.cow_modify_with_operation(
            node_memory_manager,
            &node_memory_manager.get_allocator(),
            owned_node_set,
            trie_node,
            |owned_trie_node| owned_trie_node.replace_value_valid(value),
            |read_only_trie_node| {
                (
                    unsafe {
                        read_only_trie_node.copy_and_replace_fields(
                            Some(Some(value)),
                            None,
                            None,
                        )
                    },
                    read_only_trie_node.value_clone(),
                )
            },
        )
    }

    /// If owned, run f_owned on trie node; otherwise run f_ref on the read-only
    /// trie node to create the equivalent trie node and return value as the
    /// final state of f_owned.
    pub fn cow_modify_with_operation<
        'a,
        OutputType,
        FOwned: FnOnce(&'a mut TrieNodeDeltaMpt) -> OutputType,
        FRef: FnOnce(&'a TrieNodeDeltaMpt) -> (TrieNodeDeltaMpt, OutputType),
    >(
        &mut self, node_memory_manager: &'a NodeMemoryManagerDeltaMpt,
        allocator: AllocatorRefRefDeltaMpt<'a>,
        owned_node_set: &mut OwnedNodeSet,
        mut trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
        f_owned: FOwned, f_ref: FRef,
    ) -> Result<OutputType>
    {
        let copied = self.convert_to_owned(
            node_memory_manager,
            allocator,
            owned_node_set,
        )?;
        match copied {
            None => unsafe {
                let trie_node_mut = trie_node.as_mut().owned_as_mut_unchecked();
                Ok(f_owned(trie_node_mut))
            },
            Some(new_entry) => {
                let (new_trie_node, output) =
                    f_ref(trie_node.as_ref().as_ref());
                new_entry.insert(new_trie_node);
                Ok(output)
            }
        }
    }

    pub fn cow_modify<'a>(
        &mut self, node_memory_manager: &'a NodeMemoryManagerDeltaMpt,
        allocator: AllocatorRefRefDeltaMpt<'a>,
        owned_node_set: &mut OwnedNodeSet,
        mut trie_node: GuardedMaybeOwnedTrieNodeAsCowCallParam,
    ) -> Result<&'a mut TrieNodeDeltaMpt>
    {
        let copied = self.convert_to_owned(
            node_memory_manager,
            allocator,
            owned_node_set,
        )?;
        match copied {
            None => unsafe { Ok(trie_node.as_mut().owned_as_mut_unchecked()) },
            Some(new_entry) => unsafe {
                let new_trie_node = trie_node
                    .as_ref()
                    .as_ref()
                    .copy_and_replace_fields(None, None, None);
                let key = new_entry.key();
                new_entry.insert(new_trie_node);
                Ok(NodeMemoryManagerDeltaMpt::get_in_memory_node_mut(
                    allocator, key,
                ))
            },
        }
    }
}

use super::{
    super::{
        super::{
            errors::*,
            owned_node_set::OwnedNodeSet,
            state_manager::{
                AtomicCommitTransaction, COL_CHILDREN_MERKLES, COL_DELTA_TRIE,
            },
        },
        guarded_value::GuardedValue,
        node_memory_manager::*,
        DeltaMpt, TrieNodeCellTrait,
    },
    merkle::*,
    mpt_value::MptValue,
    *,
};
use parking_lot::MutexGuard;
use primitives::{MerkleHash, MERKLE_NULL_NODE};
use rlp::*;
use std::{
    cell::Cell, hint::unreachable_unchecked, ops::Deref, sync::atomic::Ordering,
};
