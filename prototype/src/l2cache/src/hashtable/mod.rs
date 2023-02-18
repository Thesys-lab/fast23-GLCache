
// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A hashtable is used to lookup items and store per-item metadata.
//!
//! The [`HashTable`] design uses bulk chaining to reduce the per item overheads,
//! share metadata where possible, and provide better data locality.
//!
//! For a more detailed description of the implementation, please see:
//! <https://twitter.github.io/pelikan/2021/segcache.html>
//!
//! Our [`HashTable`] is composed of a base unit called a [`HashBucket`]. Each
//! bucket is a contiguous allocation that is sized to fit in a single
//! cacheline. This gives us room for a total of 8 64bit slots within the
//! bucket. The first slot of a bucket is used for per bucket metadata, leaving
//! us with up to 7 slots for items in the bucket:
//!
//! ```text
//!    ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
//!    │Bucket│ Item │ Item │ Item │ Item │ Item │ Item │ Item │
//!    │ Info │ Info │ Info │ Info │ Info │ Info │ Info │ Info │
//!    │      │      │      │      │      │      │      │      │
//!    │64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│
//!    │      │      │      │      │      │      │      │      │
//!    └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘
//! ```
//!
//! When a bucket is full, we may be able to chain another bucket from the
//! overflow area onto the primary bucket. To store a pointer to the next bucket
//! in the chain, we reduce the item capacity of the bucket and store the
//! pointer in the last slot. This can be repeated to chain additional buckets:
//!
//! ```text
//!    ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
//!    │Bucket│ Item │ Item │ Item │ Item │ Item │ Item │ Next │
//!    │ Info │ Info │ Info │ Info │ Info │ Info │ Info │Bucket│
//!    │      │      │      │      │      │      │      │      │──┐
//!    │64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│  │
//!    │      │      │      │      │      │      │      │      │  │
//!    └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘  │
//!                                                               │
//! ┌─────────────────────────────────────────────────────────────┘
//! │
//! │  ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
//! │  │ Item │ Item │ Item │ Item │ Item │ Item │ Item │ Next │
//! │  │ Info │ Info │ Info │ Info │ Info │ Info │ Info │Bucket│
//! └─▶│      │      │      │      │      │      │      │      │──┐
//!    │64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│  │
//!    │      │      │      │      │      │      │      │      │  │
//!    └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘  │
//!                                                               │
//! ┌─────────────────────────────────────────────────────────────┘
//! │
//! │  ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
//! │  │ Item │ Item │ Item │ Item │ Item │ Item │ Item │ Item │
//! │  │ Info │ Info │ Info │ Info │ Info │ Info │ Info │ Info │
//! └─▶│      │      │      │      │      │      │      │      │
//!    │64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│64 bit│
//!    │      │      │      │      │      │      │      │      │
//!    └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘
//! ```
//!
//! This works out so that we have capacity to store 7 items for every bucket
//! allocated to a chain.
//!

// hashtable

/// The number of slots within each bucket
const N_BUCKET_SLOT: usize = 8;

/// Maximum number of buckets in a chain. Must be <= 255.
const MAX_CHAIN_LEN: u64 = 16;

use crate::*;
use ahash::RandomState;
use core::num::NonZeroU32;

use rustcommon_time::CoarseInstant as Instant;

mod hash_bucket;

pub(crate) use hash_bucket::*;


/// Main structure for performing item lookup. Contains a contiguous allocation
/// of [`HashBucket`]s which are used to store item info and metadata.
#[repr(C)]
pub(crate) struct HashTable {
    hash_builder: Box<RandomState>,
    power: u64,
    mask: u64,
    data: Box<[HashBucket]>,
    rng: Box<Random>,
    started: CoarseInstant,
    next_to_chain: u64,
}

impl HashTable {
    /// Creates a new hashtable with a specified power and overflow factor. The
    /// hashtable will have the capacity to store up to
    /// `7 * 2^(power - 3) * (1 + overflow_factor)` items.
    pub fn new(power: u8, overflow_factor: f64) -> HashTable {
        if overflow_factor < 0.0 {
            fatal!("hashtable overflow factor must be >= 0.0");
        }

        // overflow factor is effectively bounded by the max chain length
        if overflow_factor > MAX_CHAIN_LEN as f64 {
            fatal!("hashtable overflow factor must be <= {}", MAX_CHAIN_LEN);
        }

        let slots = 1_u64 << power;
        let buckets = slots / 8;
        let mask = buckets - 1;

        let total_buckets = (buckets as f64 * (1.0 + overflow_factor)).ceil() as usize;

        let mut data = Vec::with_capacity(0);
        data.reserve_exact(total_buckets as usize);
        data.resize(total_buckets as usize, HashBucket::new());
        debug!(
            "hashtable has: {} primary slots across {} primary buckets and {} total buckets",
            slots, buckets, total_buckets,
        );

        let hash_builder = RandomState::with_seeds(
            0xbb8c484891ec6c86,
            0x0522a25ae9c769f9,
            0xeed2797b9571bc75,
            0x4feb29c1fbbd59d0,
        );

        Self {
            hash_builder: Box::new(hash_builder),
            power: power.into(),
            mask,
            data: data.into_boxed_slice(),
            rng: Box::new(rng()),
            started: Instant::recent(),
            next_to_chain: buckets as u64,
        }
    }

    /// Lookup an item by key and return it
    pub fn get(&mut self, key: &[u8], segments: &mut Segments) -> Option<(Item, u32)> {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;

        let mut bucket = &mut self.data[bucket_id as usize];
        let chain_len = chain_len(bucket.data[0]);
        let mut chain_idx = 0;

        trace!("hash: {} mask: {} bucket: {}", hash, self.mask, bucket_id);
        
        loop {
            let n_item_slot = if chain_idx == chain_len {
                N_BUCKET_SLOT
            } else {
                N_BUCKET_SLOT - 1
            };

            for i in 0..n_item_slot {
                if chain_idx == 0 && i == 0 {
                    continue;
                }

                let current_info = bucket.data[i];

                if get_tag(current_info) == tag {
                    let current_item = segments.get_item(current_info);
                    if current_item.key() != key {
                    } else {
                        let item = Item::new(
                            current_item,
                            get_cas(self.data[(hash & self.mask) as usize].data[0]),
                        );
                        item.check_magic();
                        
                        let seg_idx = get_seg_id(current_info).unwrap().get() - 1;
                        return Some((item, seg_idx));
                    }        
                }
            }

            if chain_idx == chain_len {
                break;
            }
            bucket = &mut self.data[bucket.data[N_BUCKET_SLOT - 1] as usize];
            chain_idx += 1;
        }

        None
    }

    // pub fn transform_to_ghost_entry(
    //     &mut self,
    //     key: &[u8],
    //     segments: &mut Segments,
    //     old_seg: NonZeroU32,
    //     old_offset: u64,
    //     train_data_idx: u64, 
    //     snapshot_time: u64, 
    // ) {
    //     let hash = self.hash(key);
    //     let tag = tag_from_hash(hash);
    //     let bucket_id = hash & self.mask;

    //     let mut bucket = &mut self.data[bucket_id as usize];
    //     let chain_len = chain_len(bucket.data[0]);
    //     let mut chain_idx = 0;

    //     loop {
    //         let n_item_slot = if chain_idx == chain_len {
    //             N_BUCKET_SLOT
    //         } else {
    //             N_BUCKET_SLOT - 1
    //         };

    //         for i in 0..n_item_slot {
    //             if chain_idx == 0 && i == 0 {
    //                 continue;
    //             }
    //             let current_info = bucket.data[i];

    //             if get_tag(current_info) == tag {
    //                 if get_seg_id(current_info) == Some(old_seg)
    //                     && get_offset(current_info) == old_offset
    //                 {
    //                     let item = segments.get_item(current_info);

    //                     // calculate ghost entry
    //                     let approx_snapshot_time = snapshot_time >> 3;
    //                     let approx_size = (item.size() as u64) >> 4; 

    //                     assert!(train_data_idx < 1<<15 - 1, "{}", train_data_idx); 
    //                     assert!(approx_snapshot_time < 1<<21 - 1, "{}", approx_snapshot_time);
    //                     assert!(approx_size < 1<<22 - 1, "{}", approx_size);

    //                     let ghost_entry = build_ghost_entry(
    //                         tag, train_data_idx, approx_snapshot_time, approx_size);
    //                     bucket.data[i] = ghost_entry;

    //                     assert!(is_ghost(bucket.data[i]));
    //                     assert_eq!(get_tag(bucket.data[i]), tag);
    //                     assert_eq!(get_train_data_idx(bucket.data[i]), train_data_idx);
    //                     assert_eq!(get_snapshot_time(bucket.data[i]), approx_snapshot_time);
    //                     assert_eq!(get_size(bucket.data[i]), approx_size);

    //                 } else {
    //                     
    //                 }
    //             }
    //         }

    //         if chain_idx == chain_len {
    //             break;
    //         }
    //         bucket = &mut self.data[bucket.data[N_BUCKET_SLOT - 1] as usize];
    //         chain_idx += 1;
    //     }
    // }

    #[allow(dead_code)]
    pub fn check_might_exist(&mut self, key: &[u8]) -> bool {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;

        let mut bucket = &mut self.data[bucket_id as usize];
        let chain_len = chain_len(bucket.data[0]);
        let mut chain_idx = 0;

        trace!("hash: {} mask: {} bucket: {}", hash, self.mask, bucket_id);

        loop {
            let n_item_slot = if chain_idx == chain_len {
                N_BUCKET_SLOT
            } else {
                N_BUCKET_SLOT - 1
            };

            for i in 0..n_item_slot {
                if chain_idx == 0 && i == 0 {
                    continue;
                }

                let current_info = bucket.data[i];

                if get_tag(current_info) == tag {
                    return true;
                }
            }

            if chain_idx == chain_len {
                break;
            }
            bucket = &mut self.data[bucket.data[N_BUCKET_SLOT - 1] as usize];
            chain_idx += 1;
        }

        false
    }



    /// Relinks the item to a new location
    #[allow(clippy::result_unit_err)]
    pub fn relink_item(
        &mut self,
        key: &[u8],
        old_seg: NonZeroU32,
        new_seg: NonZeroU32,
        old_offset: u64,
        new_offset: u64,
    ) -> Result<(), ()> {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;

        let mut bucket = &mut self.data[bucket_id as usize];
        let chain_len = chain_len(bucket.data[0]);
        let mut chain_idx = 0;

        let mut updated = false;

        loop {
            let n_item_slot = if chain_idx == chain_len {
                N_BUCKET_SLOT
            } else {
                N_BUCKET_SLOT - 1
            };

            for i in 0..n_item_slot {
                if chain_idx == 0 && i == 0 {
                    continue;
                }
                let current_info = bucket.data[i];

                if get_tag(current_info) == tag {
                    if get_seg_id(current_info) == Some(old_seg)
                        && get_offset(current_info) == old_offset
                    {
                        if !updated {
                            let new_item_info = build_item_info(tag, new_seg, new_offset);
                            bucket.data[i] = new_item_info;
                            updated = true;
                        } else {
                            bucket.data[i] = 0;
                        }
                    } else {
                    }
                }
            }

            if chain_idx == chain_len {
                break;
            }
            bucket = &mut self.data[bucket.data[N_BUCKET_SLOT - 1] as usize];
            chain_idx += 1;
        }

        if updated {
            Ok(())
        } else {
            Err(())
        }
    }

    /// Inserts a new item into the hashtable. This may fail if the hashtable is
    /// full.
    #[allow(clippy::result_unit_err)]
    pub fn insert(
        &mut self,
        item: RawItem,
        seg: NonZeroU32,
        offset: u64,
        ttl_buckets: &mut TtlBuckets,
        segments: &mut Segments,
        curr_vtime: u64,
    ) -> Result<bool, ()> {

        let hash = self.hash(item.key());
        let tag = tag_from_hash(hash);
        let mut bucket_id = (hash & self.mask) as usize;
        let chain_len = chain_len(self.data[bucket_id].data[0]);
        let mut chain_idx = 0;

        // check the item magic
        item.check_magic();

        let mut insert_item_info = build_item_info(tag, seg, offset);
        let mut is_update = false; 

        loop {
            let n_item_slot = if chain_idx == chain_len {
                N_BUCKET_SLOT
            } else {
                N_BUCKET_SLOT - 1
            };

            for i in 0..n_item_slot {
                if chain_idx == 0 && i == 0 {
                    continue;
                }
                let current_item_info = self.data[bucket_id].data[i];
                if get_tag(current_item_info) != tag {
                    if insert_item_info != 0 && current_item_info == 0 {
                        // found a blank slot
                        self.data[bucket_id].data[i] = insert_item_info;
                        insert_item_info = 0;
                    }
                    continue;
                }
                let found_item = segments.get_item(current_item_info);
                if found_item.key() != item.key() {
                    
                } else {
                    is_update = true;

                    // NOTE: this could cause later remove from hash table fail
                    // update existing key
                    self.data[bucket_id].data[i] = insert_item_info;
                    let _ = segments.remove_item(
                        current_item_info,
                        true,
                        ttl_buckets,
                        self,
                        curr_vtime, 
                    );
                    insert_item_info = 0;
                }
            }

            if chain_idx == chain_len {
                break;
            }
            bucket_id = self.data[bucket_id].data[N_BUCKET_SLOT - 1] as usize;
            chain_idx += 1;
        }

        if insert_item_info != 0
            && chain_len < MAX_CHAIN_LEN
            && (self.next_to_chain as usize) < self.data.len()
        {
            let next_id = self.next_to_chain as usize;
            self.next_to_chain += 1;

            self.data[next_id].data[0] = self.data[bucket_id].data[N_BUCKET_SLOT - 1];
            self.data[next_id].data[1] = insert_item_info;
            insert_item_info = 0;
            self.data[bucket_id].data[N_BUCKET_SLOT - 1] = next_id as u64;

            self.data[(hash & self.mask) as usize].data[0] += 0x0001_0000_0000_0000;
        }

        if insert_item_info == 0 {
            self.data[(hash & self.mask) as usize].data[0] += 1;
            Ok(is_update)
        } else {
            Err(())
        }
    }

    /// Used to implement higher-level CAS operations. This function looks up an
    /// item by key and checks if the CAS value matches the provided value.
    ///
    /// A success indicates that the item was found with the CAS value provided
    /// and that the CAS value has now been updated to a new value.
    ///
    /// A failure indicates that the CAS value did not match or there was no
    /// matching item for that key.
    pub fn try_update_cas<'a>(
        &mut self,
        key: &'a [u8],
        cas: u32,
        segments: &mut Segments,
    ) -> Result<(), SegError<'a>> {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;

        let mut bucket = &mut self.data[bucket_id as usize];
        let chain_len = chain_len(bucket.data[0]);
        let mut chain_idx = 0;

        trace!("hash: {} mask: {} bucket: {}", hash, self.mask, bucket_id);

        loop {
            let n_item_slot = if chain_idx == chain_len {
                N_BUCKET_SLOT
            } else {
                N_BUCKET_SLOT - 1
            };
            for i in 0..n_item_slot {
                if chain_idx == 0 && i == 0 {
                    continue;
                }
                let current_info = bucket.data[i];

                if get_tag(current_info) == tag {
                    let current_item = segments.get_item(current_info);
                    if current_item.key() != key {
                        
                    } else {
                        // TODO handle expired item
                        if cas == get_cas(bucket.data[0]) {
                            // TODO(bmartin): what is expected on overflow of the cas bits?
                            self.data[(hash & self.mask) as usize].data[0] += 1;
                            return Ok(());
                        } else {
                            return Err(SegError::Exists);
                        }
                    }
                }
            }

            if chain_idx == chain_len {
                break;
            }
            bucket = &mut self.data[bucket.data[N_BUCKET_SLOT - 1] as usize];
            chain_idx += 1;
        }

        Err(SegError::NotFound)
    }

    /// Removes the item with the given key
    pub fn delete(
        &mut self,
        key: &[u8],
        ttl_buckets: &mut TtlBuckets,
        segments: &mut Segments,
        curr_vtime: u64,
    ) -> bool {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let mut bucket_id = (hash & self.mask) as usize;
        let chain_len = chain_len(self.data[bucket_id].data[0]);
        let mut chain_idx = 0;

        let mut deleted = false;

        loop {
            let n_item_slot = if chain_idx == chain_len {
                N_BUCKET_SLOT
            } else {
                N_BUCKET_SLOT - 1
            };
            for i in 0..n_item_slot {
                if chain_idx == 0 && i == 0 {
                    continue;
                }
                let current_item_info = self.data[bucket_id].data[i];

                if get_tag(current_item_info) == tag {
                    let current_item = segments.get_item(current_item_info);
                    if current_item.key() != key {
                        continue;
                    } else {
                        let _ = segments.remove_item(
                            current_item_info,
                            !deleted,
                            ttl_buckets,
                            self,
                            curr_vtime, 
                        );
                        self.data[bucket_id].data[i] = 0;
                        deleted = true;
                    }
                }
            }

            if chain_idx == chain_len {
                break;
            }
            bucket_id = self.data[bucket_id].data[N_BUCKET_SLOT - 1] as usize;
            chain_idx += 1;
        }

        deleted
    }

    /// Evict a single item from the cache
    pub fn evict(
        &mut self,
        key: &[u8],
        offset: i32,
        segment: &mut Segment,
    ) -> bool {
        let result = self.remove_from(key, offset, segment);
        result
    }

    // /// Expire a single item from the cache
    // pub fn expire(
    //     &mut self,
    //     key: &[u8],
    //     offset: i32,
    //     segment: &mut Segment,
    // ) -> bool {
    //     let result = self.remove_from(key, offset, segment);
    //     if result {
    //         ITEM_EXPIRE.increment();
    //     }
    //     result
    // }

    /// Internal function that removes an item from a segment
    fn remove_from(
        &mut self,
        key: &[u8],
        offset: i32,
        segment: &mut Segment,
    ) -> bool {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;

        let mut bucket = &mut self.data[bucket_id as usize];
        let chain_len = chain_len(bucket.data[0]);
        let mut chain_idx = 0;

        let evict_item_info = build_item_info(tag, segment.id(), offset as u64);

        let mut evicted = false;
        let mut outdated = true;
        let mut first_match = true;

        loop {
            let n_item_slot = if chain_idx == chain_len {
                N_BUCKET_SLOT
            } else {
                N_BUCKET_SLOT - 1
            };
            for i in 0..n_item_slot {
                if chain_idx == 0 && i == 0 {
                    continue;
                }
                // let current_item_info = clear_freq(bucket.data[i]);
                let current_item_info = bucket.data[i];
                if get_tag(current_item_info) != tag {
                    continue;
                }
                if get_seg_id(current_item_info) != Some(segment.id())
                    || get_offset(current_item_info) != offset as u64
                {
                    
                    continue;
                }

                if first_match {
                    if evict_item_info == current_item_info {
                        segment.remove_item(
                            current_item_info,
                            false,
                        );
                        bucket.data[i] = 0;
                        outdated = false;
                        evicted = true;
                    }
                    first_match = false;
                    continue;
                } else {
                    if !evicted && current_item_info == evict_item_info {
                        evicted = true;
                    }
                    segment.remove_item(
                        bucket.data[i],
                        !outdated,
                    );
                    bucket.data[i] = 0;
                }
            }
            if chain_idx == chain_len {
                break;
            }
            bucket = &mut self.data[bucket.data[N_BUCKET_SLOT - 1] as usize];
            chain_idx += 1;
        }

        evicted
    }

    /// Internal function used to calculate a hash value for a key
    fn hash(&self, key: &[u8]) -> u64 {
        let mut hasher = self.hash_builder.build_hasher();
        hasher.write(key);
        hasher.finish()
    }
}
