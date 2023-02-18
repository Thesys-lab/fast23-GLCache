// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Core datastructure

use crate::*;
use std::collections::HashMap;
#[cfg(not(feature="offline_segment_utility"))]
use byteorder::{ByteOrder, LittleEndian};

const RESERVE_RETRIES: usize = 3;
const CACHE_STATE_UPDATE_INTERVAL_SEC: u32 = 10;
const CACHE_STATE_UPDATE_INTERVAL_REQ: u64 = 1000;

/// A pre-allocated key-value store with eager expiration. It uses a
/// segment-structured design that stores data in fixed-size segments, grouping
/// objects with nearby expiration time into the same segment, and lifting most
/// per-object metadata into the shared segment header.
pub struct L2Cache {
    pub(crate) hashtable: HashTable,
    pub(crate) segments: Segments,
    pub(crate) ttl_buckets: TtlBuckets,

    pub(crate) ghost_map: HashMap<u64, u64>, // key: ghost_id, value: train_data_idx:20, approx_snapshot_time: 24, approx_size: 20 

    pub(crate) curr_vtime: u64,

    pub(crate) n_get_req: u64,
    pub(crate) n_miss: u64,
    pub(crate) last_n_get_req: u64,
    pub(crate) last_n_miss: u64,
    pub(crate) last_record_time: u32,

    pub(crate) miss_ratio: f32,
    pub(crate) write_rate: f32,
    pub(crate) req_rate: f32,

    // pub(crate) last_report_time: u32,
}

impl L2Cache {
    /// Returns a new `Builder` which is used to configure and construct a
    /// `Seg` instance.
    ///
    /// ```
    /// use l2Cache::{Policy, L2Cache};
    ///
    /// const MB: usize = 1024 * 1024;
    ///
    /// // create a heap using 1MB segments
    /// let cache = L2Cache::builder()
    ///     .heap_size(64 * MB)
    ///     .segment_size(1 * MB as i32)
    ///     .hash_power(16)
    ///     .eviction(Policy::Random).build();
    /// ```
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Gets a count of items in the `Seg` instance. This is an expensive
    /// operation and is only enabled for tests and builds with the `debug`
    /// feature enabled.
    ///
    /// ```
    /// use l2Cache::{Policy, L2Cache};
    ///
    /// let mut cache = L2Cache::builder().build();
    /// assert_eq!(cache.items(), 0);
    /// ```
    #[cfg(any(test, feature = "debug"))]
    pub fn items(&mut self) -> usize {
        trace!("getting segment item counts");
        self.segments.items()
    }

    /// Get the item in the `Seg` with the provided key
    ///
    /// ```
    /// use l2Cache::{CoarseDuration, Policy, L2Cache};
    ///
    /// let mut cache = L2Cache::builder().build();
    /// assert!(cache.get(b"coffee").is_none());
    ///
    /// cache.insert(b"coffee", b"strong", None, CoarseDuration::ZERO);
    /// let item = cache.get(b"coffee").expect("didn't get item back");
    /// assert_eq!(item.value(), b"strong");
    /// ```
    // pub fn get(&mut self, key: &[u8]) -> Option<Item> {
    //     return self.hashtable.get(key, &mut self.segments);
    // }

    fn update_cache_state(&mut self) {
        self.n_get_req += 1;

        if self.n_get_req - self.last_n_get_req >= CACHE_STATE_UPDATE_INTERVAL_REQ
            && CoarseInstant::recent().as_secs() - self.last_record_time
                >= CACHE_STATE_UPDATE_INTERVAL_SEC {

            let n_req = (self.n_get_req - self.last_n_get_req) as f32;
            let n_miss = (self.n_miss - self.last_n_miss) as f32;
            let n_sec = (CoarseInstant::recent().as_secs() - self.last_record_time) as f32;

            self.miss_ratio = n_miss / n_req;
            self.write_rate = n_miss / n_sec;
            self.req_rate = n_req / n_sec;

            self.last_n_get_req = self.n_get_req;
            self.last_n_miss = self.n_miss;
            self.last_record_time = CoarseInstant::recent().as_secs();
        }
    }

    #[cfg(not(feature = "offline_segment_utility"))]
    fn check_ghost(&mut self, key: &[u8]) {
        if let Policy::LearnedMerge {learner, ..} = self.segments.evict.policy_mut() {
            let key_u64 = LittleEndian::read_u64(key); 
            match self.ghost_map.get(&key_u64) {
                Some(ghost_info) => {
                    // train_data_idx:32, size: 32
                    let train_data_idx = ghost_info >> 32;
                    let size = ghost_info & 0x0000_0000_FFFF_FFFF;
                    learner.accu_train_segment_utility(train_data_idx as i32, size as u32, self.curr_vtime);

                    self.ghost_map.remove(&key_u64);
                }, 
                None => {}
            }
        }
    }

    pub fn get(&mut self, key: &[u8], _next_access_vtime: i64) -> Option<Item> {
        self.update_cache_state();

        // move from evict to here because if cache size is very large, 
        // we may not have training data and model when we need to evict
        if let Policy::LearnedMerge{train_interval_sec, time_before_first_train_data, learner, ..} = self.segments.evict.policy_mut() {
            if !learner.has_training_data && CoarseInstant::recent().as_secs() >= *time_before_first_train_data {
                // generate the first batch of training data 
                learner.gen_training_data(&mut self.segments.headers, self.curr_vtime);
                learner.next_train_time = *time_before_first_train_data + *train_interval_sec;
                
                #[cfg(feature="offline_segment_utility")]
                self.segments.cal_offline_segment_utility(self.curr_vtime); 
            }
        }

        // if the object is on a snaphotted segment and evicted 
        #[cfg(not(feature = "offline_segment_utility"))]
        self.check_ghost(key);
        
        let r = self.hashtable.get(key, &mut self.segments);

        match r {
            Some((mut item, seg_idx)) => {
                #[cfg(feature = "oracle_reuse")]
                item.set_future_reuse_vtime(_next_access_vtime);

                let mut seg_header = &mut self.segments.headers[seg_idx as usize]; 
                
                #[cfg(not(feature = "offline_segment_utility"))]
                if let Policy::LearnedMerge {learner, ..} = self.segments.evict.policy_mut() {
                    // if the object is on a snaphotted segment and not evicted 
                    if seg_header.snapshot_time > 0 && !item.raw.has_accessed_since_snapshot() {
                        // update the training utility
                        learner.accu_train_segment_utility(seg_header.train_data_idx, item.raw.size() as u32, self.curr_vtime);

                        item.raw.set_accessed_since_snapshot(true); 

                        #[cfg(feature = "last_access_time")]
                        item.raw.set_last_access_time(CoarseInstant::recent()); 
                    }    
                }

                seg_header.n_req += 1;
                if ! item.raw.has_accessed_since_write() {
                    seg_header.n_active += 1;
                    assert!(seg_header.n_active <= seg_header.live_items() as u32);
                    // self.segments.print_objects(seg_idx as usize); 
                }

                item.update_access_metadata(CoarseInstant::recent()); 

                return Some(item);
            }

            None => {
                self.n_miss += 1;
                return None;
            }
        }
    }

    // pub fn incr_vtime(&mut self) {
    //     self.segments.curr_vtime += 1;
    // }

    /// Get the item in the `Seg` with the provided key without
    /// increasing the item frequency - useful for combined operations that
    /// check for presence - eg replace is a get + set
    /// ```
    /// use l2Cache::{CoarseDuration, Policy, L2Cache};
    ///
    /// let mut cache = L2Cache::builder().build();
    /// assert!(cache.get_no_freq_incr(b"coffee").is_none());
    /// ```
    // pub fn get_no_freq_incr(&mut self, key: &[u8]) -> Option<Item> {
    //     self.hashtable.get_no_freq_incr(key, &mut self.segments)
    // }

    fn reserve<'a>(
        &mut self,
        key: &'a [u8],
        size: usize,
        ttl: CoarseDuration,
    ) -> Result<ReservedItem, SegError<'a>> {
        // try to get a `ReservedItem`
        let mut retries = RESERVE_RETRIES;

        loop {
            match self
                .ttl_buckets
                .get_mut_bucket(ttl)
                .reserve(size, &mut self.segments)
            {
                Ok((reserved_item, update_bucket_linking)) => {
                    if update_bucket_linking {
                        self.ttl_buckets.fix_bucket_linking();
                    }

                    if reserved_item.offset() == 0 || reserved_item.offset() == std::mem::size_of_val(&SEG_MAGIC) {
                        let mut seg = &mut self.segments.headers[reserved_item.seg().get() as usize - 1]; 
                        seg.write_rate = self.write_rate;
                        seg.req_rate = self.req_rate;
                        seg.miss_ratio = self.miss_ratio;
                    }

                    {
                        // // for learned-reinsertion 
                        // let mut seg = &mut self.segments.headers[reserved_item.seg().get() as usize - 1]; 
                        // if seg.reset_header_cache_stat {
                        //     // println!("reset {:?}", seg);
                        //     seg.reset_header_cache_stat = false; 
                        //     seg.write_rate = self.write_rate;
                        //     seg.req_rate = self.req_rate;
                        //     seg.miss_ratio = self.miss_ratio;
                        // }
                    }

                    return Ok(reserved_item);
                }
                Err(TtlBucketsError::ItemOversized { size }) => {
                    return Err(SegError::ItemOversized { size, key });
                }
                Err(TtlBucketsError::NoFreeSegments) => {
                    if let Err(err) = self
                        .segments
                        .evict(
                            &mut self.ttl_buckets, 
                            &mut self.hashtable, 
                            self.curr_vtime, 
                            &mut self.ghost_map,
                        )
                    {
                        println!("evict error {:?}", err);
                    } else {
                        continue;
                    }
                }
            }
            if retries == 0 {
                return Err(SegError::NoFreeSegments);
            }
            retries -= 1;
        }
    }

    /// Insert a new item into the cache. May return an error indicating that
    /// the insert was not successful.
    /// ```
    /// use l2Cache::{CoarseDuration, Policy, L2Cache};
    ///
    /// let mut cache = L2Cache::builder().build();
    /// assert!(cache.get(b"drink").is_none());
    ///
    /// cache.insert(b"drink", b"coffee", None, CoarseDuration::ZERO);
    /// let item = cache.get(b"drink").expect("didn't get item back");
    /// assert_eq!(item.value(), b"coffee");
    ///
    /// cache.insert(b"drink", b"whisky", None, CoarseDuration::ZERO);
    /// let item = cache.get(b"drink").expect("didn't get item back");
    /// assert_eq!(item.value(), b"whisky");
    /// ```
    pub fn insert<'a>(
        &mut self,
        key: &'a [u8],
        value: &[u8],
        optional: Option<&[u8]>,
        ttl: CoarseDuration,
        _next_access_vtime: i64,
    ) -> Result<(), SegError<'a>> {
        let optional = optional.unwrap_or(&[]);
        // calculate size for item
        let size = align_size(ITEM_HDR_SIZE + key.len() + value.len() + optional.len());

        let reserved = self.reserve(key, size, ttl);
        match reserved {
            Ok(mut reserved_item) => {
                reserved_item.define(key, value, optional);

                #[cfg(feature = "oracle_reuse")]
                reserved_item.item().set_future_reuse_time(_next_access_vtime);

                match self.link_hashtable(&reserved_item) {
                    Err(_) => {
                        return Err(SegError::HashTableInsertEx);
                    }
                    Ok(_) => {
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    fn link_hashtable(&mut self, reserved: &ReservedItem) -> Result<bool, SegError> {
        // insert into the hashtable, or roll-back by removing the item
        // TODO(bmartin): we can probably roll-back the offset and re-use the
        // space in the segment, currently we consume the space even if the
        // hashtable is overfull
        // return whether it is an update or new item

        match self.hashtable.insert(
            reserved.item(),
            reserved.seg(),
            reserved.offset() as u64,
            &mut self.ttl_buckets,
            &mut self.segments,
            self.curr_vtime,
        ) {
            Ok(is_update) => {
                return Ok(is_update);
            }
            Err(_) => {
                let _ = self.segments.remove_at(
                    reserved.seg(),
                    reserved.offset(),
                    false,
                    &mut self.ttl_buckets,
                    &mut self.hashtable,
                    self.curr_vtime, 
                );
                return Err(SegError::HashTableInsertEx);
            }
        }
    }

    /// Performs a CAS operation, inserting the item only if the CAS value
    /// matches the current value for that item.
    ///
    /// ```
    /// use l2Cache::{CoarseDuration, Policy, L2Cache, SegError};
    ///
    /// let mut cache = L2Cache::builder().build();
    ///
    /// // If the item is not in the cache, CAS will fail as 'NotFound'
    /// assert_eq!(
    ///     cache.cas(b"drink", b"coffee", None, CoarseDuration::ZERO, 0),
    ///     Err(SegError::NotFound)
    /// );
    ///
    /// // If a stale CAS value is provided, CAS will fail as 'Exists'
    /// cache.insert(b"drink", b"coffee", None, CoarseDuration::ZERO);
    /// assert_eq!(
    ///     cache.cas(b"drink", b"coffee", None, CoarseDuration::ZERO, 0),
    ///     Err(SegError::Exists)
    /// );
    ///
    /// // Getting the CAS value and then performing the operation ensures
    /// // success in absence of a race with another client
    /// let current = cache.get(b"drink").expect("not found");
    /// assert!(cache.cas(b"drink", b"whisky", None, CoarseDuration::ZERO, current.cas()).is_ok());
    /// let item = cache.get(b"drink").expect("not found");
    /// assert_eq!(item.value(), b"whisky"); // item is updated
    /// ```
    pub fn cas<'a>(
        &mut self,
        key: &'a [u8],
        value: &[u8],
        optional: Option<&[u8]>,
        ttl: CoarseDuration,
        cas: u32,
        next_access_vtime: i64, 
    ) -> Result<(), SegError<'a>> {
        match self.hashtable.try_update_cas(key, cas, &mut self.segments) {
            Ok(()) => self.insert(key, value, optional, ttl, next_access_vtime),
            Err(e) => Err(e),
        }
    }

    /// Remove the item with the given key, returns a bool indicating if it was
    /// removed.
    /// ```
    /// use l2Cache::{CoarseDuration, Policy, L2Cache, SegError};
    ///
    /// let mut cache = L2Cache::builder().build();
    ///
    /// // If the item is not in the cache, delete will return false
    /// assert_eq!(cache.delete(b"coffee"), false);
    ///
    /// // And will return true on success
    /// cache.insert(b"coffee", b"strong", None, CoarseDuration::ZERO);
    /// assert!(cache.get(b"coffee").is_some());
    /// assert_eq!(cache.delete(b"coffee"), true);
    /// assert!(cache.get(b"coffee").is_none());
    /// ```
    // TODO(bmartin): a result would be better here
    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.hashtable
            .delete(key, 
                &mut self.ttl_buckets, 
                &mut self.segments, 
                self.curr_vtime)
    }

    /// Loops through the TTL Buckets to handle eager expiration, returns the
    /// number of segments expired
    /// ```
    /// use l2Cache::{CoarseDuration, Policy, L2Cache, SegError};
    /// use rustcommon_time::{CoarseInstant, refresh_with_sec_timestamp};
    ///
    /// let mut cache = L2Cache::builder().build();
    ///
    /// // Insert an item with a short ttl
    /// cache.insert(b"coffee", b"strong", None, CoarseDuration::from_secs(5));
    ///
    /// // The item is still in the cache
    /// assert!(cache.get(b"coffee").is_some());
    ///
    /// // Delay and then trigger expiration
    /// // std::thread::sleep(std::time::Duration::from_secs(6));
    /// refresh_with_sec_timestamp(CoarseInstant::now().secs + 6);
    /// cache.expire();
    ///
    /// // And the expired item is not in the cache
    /// assert!(cache.get(b"coffee").is_none());
    /// ```
    pub fn expire(&mut self) -> usize {
        // rustcommon_time::refresh_clock();

        self.ttl_buckets
            .expire(&mut self.hashtable, &mut self.segments)
    }

    /// Checks the integrity of all segments
    /// *NOTE*: this operation is relatively expensive
    #[cfg(feature = "debug")]
    pub fn check_integrity(&mut self) -> Result<(), SegError> {
        if self.segments.check_integrity() {
            Ok(())
        } else {
            Err(SegError::DataCorrupted)
        }
    }
}
