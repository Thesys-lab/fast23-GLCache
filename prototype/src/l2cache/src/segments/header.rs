// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! The `SegmentHeader` contains metadata about the segment. It is intended to
//! be stored in DRAM as the fields are frequently accessed and changed.
//!
//! The header is padded out to occupy a full cacheline
//! ```text
//! ┌──────────────┬──────────────┬──────────────┬──────────────┐
//! │      ID      │ WRITE OFFSET │  LIVE BYTES  │  LIVE ITEMS  │
//! │              │              │              │              │
//! │    32 bit    │    32 bit    │    32 bit    │    32 bit    │
//! ├──────────────┼──────────────┼──────────────┼──────────────┤
//! │   PREV SEG   │   NEXT SEG   │  CREATE AT   │   MERGE AT   │
//! │              │              │              │              │
//! │    32 bit    │    32 bit    │    32 bit    │    32 bit    │
//! ├──────────────┼──┬──┬────────┴──────────────┴──────────────┤
//! │     TTL      │  │  │               PADDING                │   Accessible
//! │              │  │◀─┼──────────────────────────────────────┼──    8 bit
//! │    32 bit    │8b│8b│                80 bit                │
//! ├──────────────┴──┴──┴──────────────────────────────────────┤    Evictable
//! │                          PADDING                          │      8 bit
//! │                                                           │
//! │                          128 bit                          │
//! └───────────────────────────────────────────────────────────┘
//! ```

use super::SEG_MAGIC;
use core::num::NonZeroU32;
use rustcommon_time::*;
use super::error::NotEvictableReason; 

// use rustcommon_time::CoarseDuration as Duration;
use rustcommon_time::CoarseInstant as Instant;

// the minimum age of a segment before it is eligible for eviction
// TODO(bmartin): this should be parameterized.
// const SEG_MATURE_TIME: Duration = Duration::from_secs(20);

#[derive(Debug)]
#[repr(C)]
pub struct SegmentHeader {
    /// The id for this segment
    id: NonZeroU32,
    /// Current write position
    pub(crate) write_offset: i32,
    /// The number of live bytes in the segment
    pub(crate) live_bytes: i32,
    /// The number of live items in the segment
    pub(crate) live_items: i32,
    /// The previous segment in the TtlBucket or on the free queue
    pub(crate) prev_seg: Option<NonZeroU32>,
    /// The next segment in the TtlBucket or on the free queue
    pub(crate) next_seg: Option<NonZeroU32>,
    /// The time the segment was last "created" (taken from free queue)
    pub(crate) create_at: Instant,
    /// The time the segment was last merged
    pub(crate) merge_at: Instant,
    /// The TTL of the segment in seconds
    pub(crate) ttl: u32,

    // feature related 
    pub(crate) snapshot_time: i32,
    pub(crate) train_data_idx: i32,

    pub(crate) req_rate: f32, 
    pub(crate) write_rate: f32, 
    pub(crate) miss_ratio: f32,

    pub(crate) n_req: u32, 
    pub(crate) n_active: u32,
    pub(crate) n_merge: u16,
    // used for learned eviction with reinsertion 
    pub(crate) reset_header_cache_stat: bool, 

    pub(crate) pred_utility: f32,

    /// Is the segment accessible?
    pub(crate) accessible: bool,
    /// Is the segment evictable?
    pub(crate) evictable: bool,
    // used by algorithm that periodically re-ranks
    pub(crate) can_evict_this_round: bool, 
    /// Is the segment one of the free segments 
    pub(crate) free: bool, 
    // _pad: [u8; 24],
}

impl SegmentHeader {
    pub fn new(id: NonZeroU32) -> Self {
        Self {
            id,
            write_offset: 0,
            live_bytes: 0,
            live_items: 0,
            prev_seg: None,
            next_seg: None,
            create_at: Instant::recent(),
            merge_at: Instant::recent(),
            ttl: 0,

            snapshot_time: -1,
            train_data_idx: -1,
            pred_utility: 0.0,

            req_rate: 0.0,
            write_rate: 0.0,
            miss_ratio: 0.0,
            n_req: 0,
            n_active: 0,
            n_merge: 0,
            reset_header_cache_stat: false,

            accessible: false,
            evictable: false,
            can_evict_this_round: true, 
            free: true,
            // _pad: [0; 24],
        }
    }

    pub fn init(&mut self) {
        // TODO(bmartin): should these be `debug_assert` or are we enforcing
        // invariants? Eitherway, keeping them before changing values in the
        // header is probably wise?
        // assert!(!self.accessible());
        // assert!(!self.evictable());

        self.free = true;
        self.prev_seg = None;
        self.next_seg = None;
        self.live_items = 0;
        self.create_at = Instant::recent();
        self.merge_at = CoarseInstant::from_secs(0);

        self.snapshot_time = -1;
        self.train_data_idx = -1;
        self.pred_utility = 0.0;
        self.req_rate = 0.0;
        self.write_rate = 0.0;
        self.miss_ratio = 0.0;
        self.n_req = 0;
        self.n_active = 0;
        self.n_merge = 0;
        
        self.reset_header_cache_stat = false;
        
        let offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC) as i32
        } else {
            0
        };

        self.write_offset = offset;
        self.live_bytes = offset;

        self.accessible = true;
    }

    #[inline]
    pub fn id(&self) -> NonZeroU32 {
        self.id
    }

    #[inline]
    /// Returns the offset in the segment to begin writing the next item.
    pub fn write_offset(&self) -> i32 {
        self.write_offset
    }

    #[inline]
    /// Sets the write offset to the provided value. Typically used when
    /// resetting the segment.
    pub fn set_write_offset(&mut self, bytes: i32) {
        self.write_offset = bytes;
    }

    #[inline]
    /// Moves the write offset forward by some number of bytes and returns the
    /// previous value. This is used as part of writing a new item to reserve
    /// some number of bytes and return the position to begin writing.
    pub fn incr_write_offset(&mut self, bytes: i32) -> i32 {
        let prev = self.write_offset;
        self.write_offset += bytes;
        prev
    }

    #[inline]
    /// Is the segment accessible?
    pub fn accessible(&self) -> bool {
        self.accessible
    }

    #[inline]
    /// Set whether the segment is accessible.
    pub fn set_accessible(&mut self, accessible: bool) {
        self.accessible = accessible;
    }

    #[inline]
    /// Is the segment evictable?
    pub fn evictable(&self) -> bool {
        self.evictable
    }

    #[inline]
    /// Set whether the segment is evictable.
    pub fn set_evictable(&mut self, evictable: bool) {
        self.evictable = evictable;
    }

    #[inline]
    pub fn is_free(&self) -> bool {
        self.free
    }

    #[inline]
    pub fn set_free(&mut self) {
        assert_eq!(self.free, false);
        self.free = true
    }

    #[inline]
    pub fn set_not_free(&mut self) {
        assert_eq!(self.free, true);
        self.free = false
    }

    #[inline]
    /// The number of live items within the segment.
    pub fn live_items(&self) -> i32 {
        self.live_items
    }

    #[inline]
    /// Increment the number of live items.
    pub fn incr_live_items(&mut self) {
        self.live_items += 1;
    }

    #[inline]
    /// Decrement the number of live items.
    pub fn decr_live_items(&mut self) {
        self.live_items -= 1;
    }

    #[inline]
    /// Returns the TTL for the segment.
    pub fn ttl(&self) -> CoarseDuration {
        CoarseDuration::from_secs(self.ttl)
    }

    #[inline]
    /// Sets the TTL for the segment.
    pub fn set_ttl(&mut self, ttl: CoarseDuration) {
        self.ttl = ttl.as_secs();
    }

    #[inline]
    /// The number of bytes used in the segment.
    pub fn live_bytes(&self) -> i32 {
        self.live_bytes
    }

    #[inline]
    /// Increment the number of bytes used in the segment.
    pub fn incr_live_bytes(&mut self, bytes: i32) -> i32 {
        let prev = self.live_bytes;
        self.live_bytes += bytes;
        prev
    }

    #[inline]
    /// Decrement the number of bytes used in the segment.
    pub fn decr_live_bytes(&mut self, bytes: i32) -> i32 {
        let prev = self.live_bytes;
        self.live_bytes -= bytes;
        prev
    }

    #[inline]
    /// Returns an option containing the previous segment id if there is one.
    pub fn prev_seg(&self) -> Option<NonZeroU32> {
        self.prev_seg
    }

    #[inline]
    /// Set the previous segment to some id. Passing a negative id results in
    /// clearing the previous segment pointer.
    pub fn set_prev_seg(&mut self, id: Option<NonZeroU32>) {
        self.prev_seg = id;
    }

    #[inline]
    /// Returns an option containing the next segment id if there is one.
    pub fn next_seg(&self) -> Option<NonZeroU32> {
        self.next_seg
    }

    #[inline]
    /// Set the next segment to some id. Passing a negative id results in
    /// clearing the next segment pointer.
    pub fn set_next_seg(&mut self, id: Option<NonZeroU32>) {
        self.next_seg = id;
    }

    #[inline]
    /// Returns the instant at which the segment was created
    pub fn create_at(&self) -> CoarseInstant {
        self.create_at
    }

    #[inline]
    /// Update the created time
    pub fn mark_created(&mut self) {
        self.create_at = CoarseInstant::recent();
    }

    #[inline]
    /// Returns the instant at which the segment was merged
    pub fn merge_at(&self) -> CoarseInstant {
        self.merge_at
    }

    #[inline]
    /// Update the created time
    pub fn mark_merged(&mut self) {
        self.merge_at = CoarseInstant::recent();
        if self.n_merge < u16::MAX {
            self.n_merge += 1;
        }
    }

    #[inline]
    pub fn reset_merge_at(&mut self) {
        self.merge_at = CoarseInstant::from_secs(0);
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_age(&self) -> u32 {
        (CoarseInstant::now() - self.create_at).as_secs()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_create_time(&self) -> CoarseInstant {
        self.create_at
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        self.ttl > 0 && self.create_at() + self.ttl() < Instant::recent()
    }

    #[inline]
    // clippy throws a false positive for suspicious_operation_groupings lint
    // for the instant + duration portion. We set the allow pragma to silence
    // the false positive.
    #[allow(clippy::suspicious_operation_groupings)]
    /// Can the segment be evicted?
    pub fn can_evict(&self) -> bool {
        assert!(self.evictable);
        self.can_evict_this_round && self.next_seg().is_some() && !self.is_expired()
    }

    #[inline]
    pub fn not_evictable_reason(&self) -> NotEvictableReason {
        if !self.evictable() {
            return NotEvictableReason::NotEvictableFlag;
        }

        if !self.can_evict_this_round {
            return NotEvictableReason::NotEvictableThisRound;
        }

        if self.next_seg().is_none() {
            return NotEvictableReason::NoNextSeg;
        }

        if self.is_expired() {
            return NotEvictableReason::CloseToExpire;
        }

        return NotEvictableReason::CanEvict;
        // panic!("unreachable");
    }


    #[cfg(feature="seg_free_chunk")]
    pub fn get_free_chunk_list_len(&self) -> u8 {
        self.free_chunk_offset.len() as u8
    }

    #[allow(dead_code)]
    #[cfg(feature="seg_free_chunk")]
    pub fn get_free_chunk_offsets(&self) -> &[i32] {
        return &self.free_chunk_offset
    }

    #[cfg(feature="seg_free_chunk")]
    pub fn get_free_chunk_offset(&self, idx: u8) -> (i32, i32) {
        debug_assert!(idx < self.free_chunk_offset.len() as u8);
        (self.free_chunk_offset[idx as usize], 
            self.free_chunk_size[idx as usize])
    }
    
    #[cfg(feature="seg_free_chunk")]
    pub fn set_free_chunk_offset(&mut self, idx: u8, offset: i32, size: i32) {
        debug_assert!(idx < self.free_chunk_offset.len() as u8);
        self.free_chunk_offset[idx as usize] = offset;
        self.free_chunk_size[idx as usize] = size;

        // let size_reduced = size / MIN_FREE_CHUNK_SIZE as i32; 
        // assert!(size_reduced < u16::MAX as i32);
        // self.free_chunk_size[idx as usize] = size_reduced as u16;
    }

    #[cfg(feature="seg_free_chunk")]
    pub fn reset_free_chunk_offset(&mut self) {
        for i in 0..self.free_chunk_offset.len() {
            self.free_chunk_offset[i] = -1;
            self.free_chunk_size[i] = 0;
        }
    }
}

