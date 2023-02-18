// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! TTL bucket containing a segment chain which stores items with a similar TTL
//! in an ordered fashion.
//!
//! TTL Bucket:
//! ```text
//! ┌──────────────┬──────────────┬─────────────┬──────────────┐
//! │   HEAD SEG   │   TAIL SEG   │     TTL     │     NSEG     │
//! │              │              │             │              │
//! │    32 bit    │    32 bit    │    32 bit   │    32 bit    │
//! ├──────────────┼──────────────┴─────────────┴──────────────┤
//! │  NEXT MERGE  │                  PADDING                  │
//! │              │                                           │
//! │    32 bit    │                  96 bit                   │
//! ├──────────────┴───────────────────────────────────────────┤
//! │                         PADDING                          │
//! │                                                          │
//! │                         128 bit                          │
//! ├──────────────────────────────────────────────────────────┤
//! │                         PADDING                          │
//! │                                                          │
//! │                         128 bit                          │
//! └──────────────────────────────────────────────────────────┘
//! ```

use crate::*;
use core::num::NonZeroU32;

// use std::sync::atomic::{AtomicI64, Ordering};
// static NUM_SEARCHES: AtomicI64 = AtomicI64::new(0);
// static NUM_SEARCHES_FAILED: AtomicI64 = AtomicI64::new(0);
// static NUM_SEARCHED_SEGS: AtomicI64 = AtomicI64::new(0);

// use std::cell::RefCell;
// thread_local!(static GLOBAL_DATA: RefCell<u64> = RefCell::new(0));



// TODO whydo we pack to 64B not 32B?
/// Each ttl bucket contains a segment chain to store items with a similar TTL
/// in an ordered fashion. The first segment to expire will be the head of the
/// segment chain. This allows us to efficiently scan across the [`TtlBuckets`]
/// and expire segments in an eager fashion.
///
/// we will still have one chain, but we will have two heads,
/// one point to the real head, one points to the merged head, notice that
/// after the first expire_merge, we will have one merged segment, over time there may be more,
/// when merged segments are merged_expire again, we just do re-admission to the merged_tail
#[derive(Debug)]
pub struct TtlBucket {
    pub(crate) head: Option<NonZeroU32>,
    pub(crate) tail: Option<NonZeroU32>,
    pub(crate) seg_before_tail: Option<NonZeroU32>,

    // the next bucket that have segments
    pub(crate) next_in_use_bucket_idx: Option<u32>, 
    pub(crate) ttl: i32,
    pub(crate) nseg: i32,
    pub(crate) next_to_merge: Option<NonZeroU32>,

    /// time of last write, 
    pub(crate) last_write_at: CoarseInstant,
    // _pad: [u8; 28],
}

impl TtlBucket {
    pub(super) fn new(ttl: i32) -> Self {
        Self {
            head: None,
            tail: None,
            seg_before_tail: None,
            next_in_use_bucket_idx: None,
            ttl,
            nseg: 0,
            next_to_merge: None,
            last_write_at: CoarseInstant::now(),
            // _pad: [0; 28],
        }
    }

    #[inline]
    pub fn head(&self) -> Option<NonZeroU32> {
        self.head
    }

    #[inline]
    pub fn set_head(&mut self, id: Option<NonZeroU32>) {
        self.head = id;
    }

    #[inline]
    pub fn tail(&self) -> Option<NonZeroU32> {
        self.tail
    }

    #[inline]
    pub fn set_tail(&mut self, id: Option<NonZeroU32>) {
        self.tail = id;
    }
    #[inline]
    pub fn next_to_merge(&self) -> Option<NonZeroU32> {
        self.next_to_merge
    }

    #[inline]
    pub fn set_next_to_merge(&mut self, next: Option<NonZeroU32>) {
        self.next_to_merge = next;
    }

    #[inline]
    #[allow(dead_code)]
    pub fn ttl(&self) -> i32 {
        self.ttl
    }

    #[inline]
    #[allow(dead_code)]
    pub fn nseg(&self) -> i32 {
        self.nseg
    }

    #[inline]
    pub fn reduce_nseg(&mut self, n: i32) {
        self.nseg -= n;
    }

    #[inline]
    pub fn next_in_use_bucket_idx(&self) -> Option<u32> {
        self.next_in_use_bucket_idx
    }

    #[inline]
    pub fn set_next_in_use_bucket_idx(&mut self, idx: Option<u32>) {
        self.next_in_use_bucket_idx = idx;
    }

    #[inline]
    #[allow(dead_code)]
    pub fn update_last_write_at(&mut self) {
        self.last_write_at = CoarseInstant::recent();
    }

    // expire segments from this TtlBucket, returns the number of segments expired
    #[inline]
    pub(super) fn expire(
        &mut self,
        hashtable: &mut HashTable,
        segments: &mut Segments,
    ) -> usize {
        if self.head.is_none() {
            return 0;
        }
        let mut expired = 0;
        let flush_at = segments.flush_at();
        // self.verify_ttl_bucket(segments);

        loop {
            let seg_id = self.head;
            if let Some(seg_id) = seg_id {
                let mut segment = segments.get_mut(seg_id).unwrap();
                assert_eq!(segment.is_free(), false);
                if segment.is_expired() || segment.create_at() < flush_at {
                    let _ = segment.clear(hashtable, true);
                    segments.push_free(seg_id, Some(self));
                    expired += 1;
                } else {
                    return expired;
                }
            } else {
                return expired;
            }
        }
    }


    #[allow(dead_code)]
    pub(crate) fn print_segment_chain(&self, segments: &Segments) {
        let mut seg_id = self.head;
        while let Some(seg_id_) = seg_id {
            let segment = &segments.headers[seg_id_.get() as usize - 1];
            println!("{:?}", segment);
            seg_id = segment.next_seg();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn verify_ttl_bucket(&self, segments: &Segments, msg: &str) {
        let mut nseg = 0;
        let mut seg_id = self.head;
        if self.head.is_none() {
            assert_eq!(self.tail, self.head);
            assert_eq!(self.nseg, 0);
            return;
        }

        if self.nseg == 1 {
            assert_eq!(self.head, self.tail);
        }

        assert_eq!(
            segments.headers[self.head.unwrap().get() as usize - 1].prev_seg(),
            None,
            "{}",
            msg
        );
        assert_eq!(
            segments.headers[self.tail.unwrap().get() as usize - 1].next_seg(),
            None,
            "{}",
            msg
        );

        let bucket_ttl = self.ttl as u32;
        let mut last_create_at = 0;
        while let Some(seg_id_) = seg_id {
            let segment = &segments.headers[seg_id_.get() as usize - 1];
            // println!("{:?}", segment);
            assert_eq!(segment.ttl().as_secs(), bucket_ttl);
            assert_eq!(segment.is_free(), false);
            assert!(segment.create_at().as_secs() >= last_create_at);
            last_create_at = segment.create_at().as_secs();
            nseg += 1;
            seg_id = segment.next_seg();
        }

        if nseg != self.nseg {
            println!("{:?}", self);
            seg_id = self.head;
            while let Some(seg_id_) = seg_id {
                let segment = &segments.headers[seg_id_.get() as usize - 1];
                println!("{:?}", segment);
                seg_id = segment.next_seg();
            }
        }

        assert_eq!(nseg, self.nseg, "{}", msg);
    }

    fn try_expand(&mut self, segments: &mut Segments) -> Result<(), TtlBucketsError> {
        if self.last_write_at < CoarseInstant::recent() {
            self.last_write_at = CoarseInstant::recent();
        }

        if let Some(id) = segments.pop_free() {
            self.seg_before_tail = self.tail; 
            if let Some(tail_id) = self.tail {
                let mut tail = segments.get_mut(tail_id).unwrap();
                tail.set_next_seg(Some(id));
            }

            let mut segment = segments.get_mut(id).unwrap();
            segment.set_prev_seg(self.tail);
            segment.set_next_seg(None);
            segment.set_ttl(CoarseDuration::from_secs(self.ttl as u32));
            if self.head.is_none() {
                assert!(self.tail.is_none());
                assert!(self.next_to_merge.is_none());
                assert_eq!(self.nseg, 0, "ttl bucket {}", self.ttl);
                self.head = Some(id);
            }
            self.tail = Some(id);
            self.nseg += 1;
            debug_assert!(!segment.evictable(), "segment should not be evictable");
            segment.set_evictable(true);
            segment.set_accessible(true);

            Ok(())
        } else {
            Err(TtlBucketsError::NoFreeSegments)
        }
    }

    // return reserved item, whether TTL buckets linking needs update, is_in_place_write
    pub(crate) fn reserve(
        &mut self,
        size: usize,
        segments: &mut Segments, 
    ) -> Result<(ReservedItem, bool), TtlBucketsError> {
        trace!("reserving: {} bytes for ttl: {}", size, self.ttl);

        let seg_size = segments.segment_size() as usize;

        if size > seg_size {
            return Err(TtlBucketsError::ItemOversized{size});
        }

        let update_bucket_linking = self.head().is_none(); 
        
        // allow write to second last seg to reduce space fragmentation
        if let Some(id) = self.seg_before_tail {
            let mut segment = segments.get_mut(id).unwrap(); 
            debug_assert!(segment.accessible(), "{} segs, {:?}", self.nseg, segment.get_header()); 

            let offset = segment.write_offset() as usize;
            if offset + size <= seg_size {
                let item = segment.alloc_item(size as i32);
                return Ok((ReservedItem::new(item, segment.id(), offset), update_bucket_linking));
            }
        }

        loop {
            if let Some(id) = self.tail {
                let mut segment = segments.get_mut(id).unwrap(); 
                debug_assert!(segment.accessible()); 

                let offset = segment.write_offset() as usize;
                if offset + size <= seg_size {
                    let item = segment.alloc_item(size as i32);
                    return Ok((ReservedItem::new(item, segment.id(), offset), update_bucket_linking));
                }
            }
            self.try_expand(segments)?;
        }
    }
}
