// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::{SegmentHeader, SegmentsError};
use crate::*;
use core::num::NonZeroU32;
use std::collections::HashMap;

pub const SEG_MAGIC: u64 = 0xBADC0FFEEBADCAFE;

/// A `Segment` is a contiguous allocation of bytes and an associated header
/// which contains metadata. This structure allows us to operate on mutable
/// borrows of the header and data sections to perform basic operations.
pub struct Segment<'a> {
    pub header: &'a mut SegmentHeader,
    data: &'a mut [u8],
}

impl<'a> Segment<'a> {
    /// Construct a `Segment` from its raw parts
    pub fn from_raw_parts(
        header: &'a mut segments::header::SegmentHeader,
        data: &'a mut [u8],
    ) -> Self {
        Segment { header, data }
    }

    /// Initialize the segment. Sets the magic bytes in the data segment (if the
    /// feature is enabled) and initializes the header fields.
    pub fn init(&mut self) {
        if cfg!(feature = "magic") {
            for (i, byte) in SEG_MAGIC.to_be_bytes().iter().enumerate() {
                self.data[i] = *byte;
            }
        }
        self.header.init();
    }

    #[cfg(feature = "magic")]
    #[inline]
    /// Reads the magic bytes from the start of the segment data section.
    pub fn magic(&self) -> u64 {
        u64::from_be_bytes([
            self.data[0],
            self.data[1],
            self.data[2],
            self.data[3],
            self.data[4],
            self.data[5],
            self.data[6],
            self.data[7],
        ])
    }

    #[inline]
    /// Checks that the magic bytes match the expected value
    ///
    /// # Panics
    ///
    /// This function will panic if the magic bytes do not match the expected
    /// value. This would indicate data corruption or that the segment was
    /// constructed from invalid data.
    pub fn check_magic(&self) {
        #[cfg(feature = "magic")]
        assert_eq!(self.magic(), SEG_MAGIC)
    }

    /// Convenience function which is used as a stop point for scanning through
    /// the segment. All valid items would exist below this value
    pub fn max_item_offset(&self) -> usize {
        if self.write_offset() >= ITEM_HDR_SIZE as i32 {
            std::cmp::min(self.write_offset() as usize, self.data.len()) - ITEM_HDR_SIZE
        } else if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        }
    }

    /// Check the segment integrity. This is an expensive operation. Will return
    /// a bool with a true result indicating that the segment integrity check
    /// has passed. A false result indicates that there is data corruption.
    ///
    /// # Panics
    ///
    /// This function may panic if the segment is corrupted or has been
    /// constructed from invalid bytes.
    #[cfg(feature = "debug")]
    pub fn check_integrity(&mut self) -> bool {
        self.check_magic();

        let mut integrity = true;

        let max_offset = self.max_item_offset();
        let mut offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        let mut count = 0;

        while offset < max_offset {
            let item = RawItem::from_ptr(unsafe { self.data.as_mut_ptr().add(offset) });
            if item.klen() == 0 {
                break;
            }
            if !item.deleted() {
                count += 1;
            }
            offset += item.size();
        }

        if count != self.live_items() {
            error!(
                "seg: {} has mismatch between counted items: {} and header items: {}",
                self.id(),
                count,
                self.live_items()
            );
            integrity = false;
        }

        integrity
    }

    /// Return the segment's id
    #[inline]
    pub fn id(&self) -> NonZeroU32 {
        self.header.id()
    }

    /// Return the current write offset of the segment. This index is the start
    /// of the next write.
    #[inline]
    pub fn write_offset(&self) -> i32 {
        self.header.write_offset()
    }

    #[inline]
    pub fn get_offset_start(&self) -> usize {
        let offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        offset
    }

    /// Set the write offset to a specific value
    #[inline]
    pub fn set_write_offset(&mut self, bytes: i32) {
        self.header.set_write_offset(bytes)
    }

    /// Return the number of live (active) bytes in the segment. This may be
    /// lower than the write offset due to items being removed/replaced
    #[inline]
    pub fn live_bytes(&self) -> i32 {
        self.header.live_bytes()
    }

    /// Return the number of live items in the segment.
    #[inline]
    pub fn live_items(&self) -> i32 {
        self.header.live_items()
    }

    /// Returns whether the segment is currently accessible from the hashtable.
    #[inline]
    pub fn accessible(&self) -> bool {
        self.header.accessible()
    }

    /// Mark whether or not the segment is accessible from the hashtable.
    #[inline]
    pub fn set_accessible(&mut self, accessible: bool) {
        self.header.set_accessible(accessible)
    }

    /// Indicate if the segment might be evictable, prefer to use `can_evict()`
    /// to check.
    #[inline]
    pub fn evictable(&self) -> bool {
        self.header.evictable()
    }

    /// Set if the segment could be considered evictable.
    #[inline]
    pub fn set_evictable(&mut self, evictable: bool) {
        self.header.set_evictable(evictable)
    }

    /// Performs some checks to determine if the segment can actually be evicted
    #[inline]
    pub fn can_evict(&self) -> bool {
        self.header.can_evict()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn not_evictable_reason(&self) -> NotEvictableReason {
        self.header.not_evictable_reason()
    }

    /// Return the segment's TTL
    #[inline]
    pub fn ttl(&self) -> CoarseDuration {
        self.header.ttl()
    }

    /// Set the segment's TTL, used when linking it into a TtlBucket
    #[inline]
    pub fn set_ttl(&mut self, ttl: CoarseDuration) {
        self.header.set_ttl(ttl)
    }

    /// Returns the time the segment was last initialized
    #[inline]
    pub fn create_at(&self) -> CoarseInstant {
        self.header.create_at()
    }

    /// Mark that the segment has been merged
    #[inline]
    pub fn mark_merged(&mut self) {
        self.header.mark_merged()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn mark_created(&mut self) {
        self.header.mark_created()
    }

    /// Return the previous segment's id. This will be a segment before it in a
    /// TtlBucket or on the free queue. A `None` indicates that this segment is
    /// the head of a bucket or the free queue.
    #[allow(dead_code)]
    #[inline]
    pub fn prev_seg(&self) -> Option<NonZeroU32> {
        self.header.prev_seg()
    }

    /// Set the previous segment id to this value. Negative values will mean
    /// that there is no previous segment, meaning this segment is the head of
    /// a bucket or the free queue
    #[inline]
    pub fn set_prev_seg(&mut self, id: Option<NonZeroU32>) {
        self.header.set_prev_seg(id)
    }

    /// Return the next segment's id. This will be a segment following it in a
    /// TtlBucket or on the free queue. A `None` indicates that this segment is
    /// the tail of a bucket or the free queue.
    #[inline]
    pub fn next_seg(&self) -> Option<NonZeroU32> {
        self.header.next_seg()
    }

    /// Set the next segment id to this value. Negative values will mean that
    /// there is no previous segment, meaning this segment is the head of a
    /// bucket or the free queue
    #[inline]
    pub fn set_next_seg(&mut self, id: Option<NonZeroU32>) {
        self.header.set_next_seg(id)
    }

    /// Decrement the live bytes by `bytes` and the live items by `1`. This
    /// would be used to update the header after an item has been removed or
    /// replaced.
    #[inline]
    pub fn decr_item(&mut self, bytes: i32) {
        self.header.decr_live_bytes(bytes);
        self.header.decr_live_items();
    }

    /// Internal function which increments the live bytes by `bytes` and the
    /// live items by `1`. Used when an item has been allocated
    #[inline]
    pub(crate) fn incr_item(&mut self, bytes: i32) {
        self.header.incr_live_bytes(bytes);
        self.header.incr_live_items();
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        self.header.is_expired()
    }

    #[inline]
    pub fn is_free(&self) -> bool {
        self.header.is_free()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_header(&self) -> &SegmentHeader {
        &self.header
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_next_seg(&self) -> Option<NonZeroU32> {
        self.header.next_seg()
    }

    #[allow(dead_code)]
    pub(crate) fn verify_integrity(&mut self, hashtable: &mut HashTable) {
        // panic!("");

        let max_offset = self.max_item_offset();
        let mut offset = self.get_offset_start();

        let mut n_live_items = 0;
        let mut n_live_bytes = 0;

        while offset <= max_offset {
            let item = self.get_item_at(offset);
            if item.klen() == 0 && self.live_items() == 0 {
                break;
            }

            item.check_magic();

            // println!("{:?} at offset {}", item.header(), offset);
            assert!(
                item.klen() > 0,
                "invalid klen: ({}) item {:?} offset {}",
                item.klen(),
                item.header(),
                offset
            );

            if !(item.is_deleted()) {
                assert!(
                    hashtable.check_might_exist(item.key()),
                    "item {:?} not in hashtable",
                    item.header()
                );

                n_live_items += 1;
                n_live_bytes += item.size();
            }

            offset += item.size();
        }

        assert_eq!(self.live_items(), n_live_items);
        assert_eq!(self.live_bytes() as usize, n_live_bytes);
    }

    /// Allocate a new `RawItem` with the given size
    ///
    /// # Safety
    ///
    /// This function *does not* check that there is enough free space in the
    /// segment. It is up to the caller to ensure that the resulting item fits
    /// in the segment. Data corruption or segfault is likely to occur if this
    /// is not checked.
    // TODO(bmartin): See about returning a Result here instead and avoiding the
    // potential safety issue.
    pub(crate) fn alloc_item(&mut self, size: i32) -> RawItem {
        let offset = self.write_offset() as usize;
        self.incr_item(size);
        let _ = self.header.incr_write_offset(size);

        let ptr = unsafe { self.data.as_mut_ptr().add(offset) };
        RawItem::from_ptr(ptr)
    }

    /// Remove an item based on its item info
    // TODO(bmartin): tombstone is currently always set
    pub(crate) fn remove_item(&mut self, item_info: u64, tombstone: bool) {
        let offset = get_offset(item_info) as usize;
        self.remove_item_at(offset, tombstone);
    }

    /// Remove an item based on its offset into the segment
    // TODO(bmartin): tombstone is currently always set
    pub(crate) fn remove_item_at(&mut self, offset: usize, _tombstone: bool) {
        let mut item = self.get_item_at(offset);

        if item.is_deleted() {
            return;
        }

        let item_size = item.size() as i64;

        self.check_magic();
        self.decr_item(item_size as i32);
        debug_assert!(self.live_items() >= 0);
        assert!(self.live_bytes() >= 0);

        item.tombstone();

        self.check_magic();
    }

    /// Returns the item at the given offset
    // TODO(bmartin): consider changing the return type here and removing asserts?
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn get_item_at(&mut self, offset: usize) -> RawItem {
        assert!(
            offset <= self.max_item_offset(),
            "{} > {} {:?}",
            offset,
            self.max_item_offset(),
            self
        );

        let rawitem = RawItem::from_ptr(unsafe { self.data.as_mut_ptr().add(offset) });

        rawitem
    }

    pub(crate) fn unchecked_get_item_at(&mut self, offset: usize) -> RawItem {
        assert!(
            offset <= self.max_item_offset(),
            "{} > {} {:?}",
            offset,
            self.max_item_offset(),
            self
        );

        RawItem::from_ptr(unsafe { self.data.as_mut_ptr().add(offset) })
    }

    /// This is used as part of segment merging, it moves all occupied space to
    /// the beginning of the segment, leaving the end of the segment free
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn compact(
        &mut self,
        hashtable: &mut HashTable,
        cutoff_freq: f64,
        curr_vtime: u64,
        remove_item_active_flag: bool,
        ghost_map: &mut HashMap<u64, u64>,
    ) -> Result<i32, SegmentsError> {
        let max_offset = self.max_item_offset();
        let mut read_offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        let mut write_offset = read_offset;
        let mut n_evicted_bytes = 0;

        // println!("segment {:?}", self.header);
        while read_offset <= max_offset {
            let mut item = self.get_item_at(read_offset);
            if item.klen() == 0 && self.live_items() == 0 {
                break;
            }
            item.check_magic();

            let item_size = item.size();

            // skip deleted items and ones that won't fit in the target segment
            if item.is_deleted() {
                read_offset += item_size;
                continue;
            }

            let item_score = item.get_score(curr_vtime);

            if self.header.train_data_idx != -1 && !item.has_accessed_since_snapshot() {
                // cconstruct ghost entry
                let key_u64 = item.key_as_u64();
                let ghost_entry = (self.header.train_data_idx as u64) << 32 | (item_size as u64);
                ghost_map.insert(key_u64, ghost_entry);
            }

            if cutoff_freq > 0.0 && item_score <= cutoff_freq {
                n_evicted_bytes += item_size;
                if !hashtable.evict(item.key(), read_offset.try_into().unwrap(), self) {
                    warn!("unlinked item was present in segment");
                    self.remove_item_at(read_offset, true);
                }

                read_offset += item_size;
                continue;
            }

            let freq_to_set = ((item.get_freq() as u32 + 1) / 2) as u8;

            if read_offset != write_offset {
                let src = unsafe { self.data.as_ptr().add(read_offset) };
                let dst = unsafe { self.data.as_mut_ptr().add(write_offset) };

                if hashtable
                    .relink_item(
                        item.key(),
                        self.id(),
                        self.id(),
                        read_offset as u64,
                        write_offset as u64,
                    )
                    .is_ok()
                {
                    // if an item has been accessed, it will be at least one
                    // note that we use a copy that can handle overlap
                    unsafe {
                        // std::ptr::copy(src, dst, item_size);
                        std::ptr::copy(src, dst, ITEM_HDR_SIZE + item.klen() as usize);
                    }

                    let mut new_item = self.get_item_at(write_offset);
                    new_item.set_freq(freq_to_set);
                    if remove_item_active_flag {
                        new_item.set_accessed_since_write(false);
                        new_item.set_accessed_since_snapshot(false);
                    }

                    assert_eq!(new_item.size(), item_size);
                } else {
                    // this shouldn't happen, but if relink does fail we can
                    // only move forward or return an error
                    println!("object {} {:?}", item.key_as_u64(), item.header());
                    panic!("{:?}", self.header);
                    // read_offset += item_size;
                    // write_offset = read_offset;
                    // continue;
                }
            } else {
                item.set_freq(freq_to_set);
            }

            read_offset += item_size;
            write_offset += item_size;
            continue;
        }

        // updates the write offset to the new position
        self.set_write_offset(write_offset as i32);

        Ok(n_evicted_bytes as i32)
    }

    /// This is used to copy data from this segment into the target segment and
    /// relink the items in the hashtable
    ///
    /// # NOTE
    ///
    /// Any items that don't fit in the target will be left in this segment it
    /// is left to the caller to decide how to handle this
    ///
    ///
    pub(crate) fn copy_into(
        &mut self,
        target: &mut Segment,
        hashtable: &mut HashTable,
        cutoff_freq: f64,
        curr_vtime: u64,
        remove_item_active_flag: bool,
        ghost_map: &mut HashMap<u64, u64>,
    ) -> Result<i32, SegmentsError> {
        let mut n_evicted_bytes = 0;
        let max_offset = self.max_item_offset();
        let mut read_offset = self.get_offset_start();

        while read_offset <= max_offset {
            let mut item = self.get_item_at(read_offset);
            if item.klen() == 0 && self.live_items() == 0 {
                break;
            }

            item.check_magic();

            let item_size = item.size();

            let write_offset = target.write_offset() as usize;
            // skip deleted items and ones that won't fit in the target segment
            // we must use <= to avoid copying all items if they have the same size and frequency
            if item.is_deleted() {
                read_offset += item_size;
                // println!("skip deleted {} {}/{}", item.key_as_u64(), item.get_freq(), item.size());
                continue;
            }

            if self.header.train_data_idx != -1 && !item.has_accessed_since_snapshot() {
                // cconstruct ghost entry
                let key_u64 = item.key_as_u64();
                let ghost_entry = (self.header.train_data_idx as u64) << 32 | (item_size as u64);
                ghost_map.insert(key_u64, ghost_entry);
            }

            if write_offset + item_size >= target.data.len() {
                read_offset += item_size;
                continue;
            }

            let item_score = item.get_score(curr_vtime);

            if cutoff_freq > 0.0 && item_score < cutoff_freq {
                read_offset += item_size;
                n_evicted_bytes += item_size;

                if self.header.train_data_idx != -1 {
                    // cconstruct ghost entry
                    let key_u64 = item.key_as_u64();
                    let ghost_entry =
                        (self.header.train_data_idx as u64) << 32 | (item_size as u64);
                    ghost_map.insert(key_u64, ghost_entry);
                }

                continue;
            }

            let src = unsafe { self.data.as_ptr().add(read_offset) };
            let dst = unsafe { target.data.as_mut_ptr().add(write_offset) };

            if hashtable
                .relink_item(
                    item.key(),
                    self.id(),
                    target.id(),
                    read_offset as u64,
                    write_offset as u64,
                )
                .is_ok()
            {
                // println!("copy {} {} {}", item.key_as_u64(), item.get_freq(), item.size());
                // since we're working with two different segments, we can use
                // nonoverlapping copy
                unsafe {
                    assert!(
                        item.used_size() >= ITEM_HDR_SIZE + 8,
                        "{} < {}",
                        item.used_size(),
                        ITEM_HDR_SIZE
                    );
                    // std::ptr::copy_nonoverlapping(src, dst, item.used_size());
                    std::ptr::copy_nonoverlapping(src, dst, ITEM_HDR_SIZE + item.klen() as usize);
                }
                // because we move freq to object metadata, this is not needed
                // if an item has been accessed, it will be at least one
                let freq_to_set = ((item.get_freq() as u32 + 1) / 2) as u8;

                // do we have to remove item?
                // self.remove_item_at(read_offset, true);
                item.tombstone();
                self.decr_item(item_size as i32);

                target.incr_item(item_size as i32);
                target.set_write_offset(write_offset as i32 + item_size as i32);

                let mut new_item = target.get_item_at(write_offset);
                new_item.set_freq(freq_to_set);
                if remove_item_active_flag {
                    new_item.set_accessed_since_write(false);
                    new_item.set_accessed_since_snapshot(false);
                }
            } else {
                // TODO(bmartin): figure out if this could happen and make the
                // relink function infallible if it can't happen
                panic!(
                    "relink failed item {:?} offset {}",
                    item.header(),
                    read_offset
                );
                // return Err(SegmentsError::RelinkFailure);
            }

            read_offset += item_size;
        }

        Ok(n_evicted_bytes as i32)
    }

    #[allow(dead_code)]
    pub(crate) fn estimate_prune_cutoff(&mut self, target_size: i32, curr_vtime: u64) -> f64 {
        if self.live_items() == 0 {
            return 0.0;
        }

        if self.live_items() == 1 && self.live_bytes() > target_size / 10 {
            return f64::MAX;
        }

        let mut vec = Vec::with_capacity(self.live_items() as usize);
        let max_offset = self.max_item_offset();
        let mut offset = self.get_offset_start();

        while offset <= max_offset {
            let item = self.get_item_at(offset);
            if item.klen() == 0 && self.live_items() == 0 {
                break;
            }

            let item_size = item.size();
            // println!("offset {:6} size {:6} {:?}", offset, item_size, item.header());

            if item.is_deleted() {
                offset += item_size;
                continue;
            }

            let item_score = item.get_score(curr_vtime);

            vec.push((item_score, item_size as i32));

            offset += item_size;
            continue;
        }

        assert_ne!(vec.len(), 0);

        vec.sort_unstable_by(|a, b| b.partial_cmp(a).unwrap());

        let mut curr_size = vec[0].1;
        let mut idx = 0;
        while curr_size <= target_size && idx + 1 < vec.len() {
            idx += 1;
            curr_size += vec[idx].1;
        }

        let cutoff_pos = idx;
        // if there are multiple with the same score, we will retain all
        while idx + 1 < vec.len() && vec[idx + 1].1 == vec[cutoff_pos].1 {
            curr_size += vec[idx].1;
            idx += 1;
        }

        // println!("curr_size {}, target size {}", curr_size, target_size);
        if curr_size > target_size / 2 * 3 {
            // keep too much, drop the last one
            if idx > 0 {
                // curr_size -= vec[idx].1;
                idx -= 1;
            }
        }

        let cutoff = vec[idx].0;
        // if idx + 1 == vec.len() {
        //     // we will keep all yhe objects, to avoid some objects being evicted
        //     // due to the random in weighted_freq, we make the cutoff smaller
        //     // println!("keep all objects");
        //     cutoff /= 8.0;
        // }

        cutoff
    }

    #[allow(dead_code)]
    pub(crate) fn print_weighted_freq(&mut self, _curr_vtime: u64) {
        let max_offset = self.max_item_offset();
        let mut offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        while offset <= max_offset {
            let item = self.get_item_at(offset);
            if item.klen() == 0 && self.live_items() == 0 {
                break;
            }

            let item_size = item.size();
            if item.is_deleted() {
                offset += item_size;
                continue;
            }

            let item_score = item.get_score(_curr_vtime);

            print!("{:.4}({}) ", item_score, item_size);
            offset += item_size;
            continue;
        }
        println!("");
    }

    /// Remove all items from the segment, unlinking them from the hashtable.
    /// If expire is true, this is treated as an expiration option. Otherwise it
    /// is treated as an eviction.
    pub(crate) fn clear(&mut self, hashtable: &mut HashTable, _expire: bool) {
        self.set_accessible(false);
        self.set_evictable(false);

        let max_offset = self.max_item_offset();
        let mut offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        while offset <= max_offset {
            let item = self.get_item_at(offset);
            if item.klen() == 0 && self.live_items() == 0 {
                break;
            }

            item.check_magic();
            debug_assert!(
                item.klen() > 0,
                "invalid klen: ({}) item {:?}",
                item.klen(),
                item.header()
            );

            if !item.is_deleted() {
                trace!("evicting from hashtable");
                let removed = hashtable.evict(item.key(), offset.try_into().unwrap(), self);
                if !removed {
                    // this *shouldn't* happen, but to keep header integrity, we
                    // warn and remove the item even if it wasn't in the
                    // hashtable
                    println!(
                        "seg {:6} ttl {:8} offset {} item key {} {:?}",
                        self.id(),
                        self.ttl().as_secs(),
                        offset,
                        item.key_as_u64(),
                        item.header()
                    );
                    panic!("clear but unlinked item was not present in segment");
                    // self.remove_item_at(offset, true);
                }
            }

            debug_assert!(
                self.live_items() >= 0,
                "cleared segment has invalid number of live items: ({})",
                self.live_items()
            );
            debug_assert!(
                self.live_bytes() >= 0,
                "cleared segment has invalid number of live bytes: ({})",
                self.live_bytes()
            );
            offset += item.size();
        }

        // ITEM_DEAD.sub(items as _);
        // ITEM_DEAD_BYTES.sub(bytes as _);

        // skips over seg_wait_refcount and evict retry, because no threading

        assert_eq!(self.live_items(), 0, "segment not empty after clearing");

        let expected_size = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC) as i32
        } else {
            0
        };
        if self.live_bytes() != expected_size {
            assert_eq!(
                self.live_bytes(),
                expected_size,
                "segment size incorrect after clearing"
            );
        }

        self.set_write_offset(self.live_bytes());
    }
}

#[cfg(feature = "magic")]
impl<'a> std::fmt::Debug for Segment<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Segment")
            .field("header", &self.header)
            .field("magic", &format!("0x{:X}", self.magic()))
            .field("data", &format!("{:02X?}", self.data))
            .finish()
    }
}

#[cfg(not(feature = "magic"))]
impl<'a> std::fmt::Debug for Segment<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Segment")
            .field("header", &self.header)
            // .field("data", &format!("{:X?}", self.data))
            .finish()
    }
}
