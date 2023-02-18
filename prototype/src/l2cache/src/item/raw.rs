// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A raw byte-level representation of an item.
//!
//! Unlike an [`Item`], the [`RawItem`] does not contain any fields which are
//! shared within a hash bucket such as the CAS value.

use crate::*;
use byteorder::{ByteOrder, LittleEndian};

/// The raw byte-level representation of an item
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct RawItem {
    data: *mut u8,
}

impl RawItem {
    /// Get an immutable borrow of the item's header
    pub(crate) fn header(&self) -> &ItemHeader {
        unsafe { &*(self.data as *const ItemHeader) }
    }

    /// Get a mutable borrow of the item's header
    pub(crate) fn header_mut(&mut self) -> *mut ItemHeader {
        self.data as *mut ItemHeader
    }

    /// Create a `RawItem` from a pointer
    ///
    /// # Safety
    ///
    /// Creating a `RawItem` from a pointer that does not point to a valid raw
    /// item or a pointer which is not 64bit aligned will result in undefined
    /// behavior. It is up to the caller to ensure that the item is constructed
    /// from a properly aligned pointer to valid data.
    pub(crate) fn from_ptr(ptr: *mut u8) -> RawItem {
        Self { data: ptr }
    }

    /// Returns the key length
    #[inline]
    pub(crate) fn klen(&self) -> u8 {
        self.header().klen()
    }

    /// Borrow the key
    pub(crate) fn key(&self) -> &[u8] {
        unsafe {
            let ptr = self.data.add(self.key_offset());
            let len = self.klen() as usize;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn key_as_u64(&self) -> u64 {
        LittleEndian::read_u64(self.key())
    }

    #[inline]
    pub(crate) fn has_accessed_since_write(&self) -> bool {
        self.header().has_accessed_since_write()
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn has_accessed_since_snapshot(&self) -> bool {
        self.header().has_accessed_since_snapshot()
    }

    #[inline]
    pub(crate) fn set_accessed_since_write(&mut self, accessed: bool) {
        unsafe {
            (*self.header_mut()).set_accessed_since_write(accessed);
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn set_accessed_since_snapshot(&mut self, accessed: bool) {
        unsafe {
            (*self.header_mut()).set_accessed_since_snapshot(accessed);
        }
    }


    /// Returns the value length
    #[inline]
    pub(crate) fn vlen(&self) -> u32 {
        self.header().vlen()
    }

    /// Borrow the value
    // TODO(bmartin): should probably change this to be Option<>
    pub(crate) fn value(&self) -> &[u8] {
        unsafe {
            let ptr = self.data.add(self.value_offset());
            let len = self.vlen() as usize;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    /// Returns the optional data length
    #[inline]
    pub(crate) fn olen(&self) -> u8 {
        self.header().olen()
    }

    /// Borrow the optional data
    pub(crate) fn optional(&self) -> Option<&[u8]> {
        if self.olen() > 0 {
            unsafe {
                let ptr = self.data.add(self.optional_offset());
                let len = self.olen() as usize;
                Some(std::slice::from_raw_parts(ptr, len))
            }
        } else {
            None
        }
    }

    /// Check the header magic bytes
    #[inline]
    pub(crate) fn check_magic(&self) {
        self.header().check_magic()
    }

    /// Set the header magic bytes
    #[inline]
    pub(crate) fn set_magic(&mut self) {
        #[cfg(feature = "magic")]
        unsafe {
            (*self.header_mut()).set_magic()
        }
    }

    /// Copy data into the item
    pub(crate) fn define(&mut self, key: &[u8], value: &[u8], optional: &[u8]) {
        unsafe {
            self.set_magic();
            (*self.header_mut()).set_deleted(false);
            (*self.header_mut()).set_num(false);
            (*self.header_mut()).set_olen(optional.len() as u8);
            std::ptr::copy_nonoverlapping(
                optional.as_ptr(),
                self.data.add(self.optional_offset()),
                optional.len(),
            );
            (*self.header_mut()).set_klen(key.len() as u8);
            std::ptr::copy_nonoverlapping(
                key.as_ptr(),
                self.data.add(self.key_offset()),
                key.len(),
            );
            (*self.header_mut()).set_vlen(value.len() as u32);
            // std::ptr::copy_nonoverlapping(
            //     value.as_ptr(),
            //     self.data.add(self.value_offset()),
            //     value.len(),
            // );
            (*self.header_mut()).set_freq(0);
            (*self.header_mut()).set_accessed_since_write(false);
            (*self.header_mut()).set_accessed_since_snapshot(false);

            // (*self.header_mut()).set_create_time(CoarseInstant::recent());

            #[cfg(feature = "last_access_time")]
            (*self.header_mut()).set_last_access_time(CoarseInstant::recent());

            // (*self.header_mut()).rand_num = (quickrandom() % u16::MAX as u64) as u16;
        }
    }

    pub fn get_freq(&self) -> u8 {
        (*self.header()).get_freq()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn reset_freq(&mut self) {
        unsafe {
            (*self.header_mut()).set_freq(0);
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn set_freq(&mut self, freq: u8) {
        unsafe {
            (*self.header_mut()).set_freq(freq);
        }
    }

    #[inline]
    #[cfg(any(not(feature = "oracle_reuse"), not(feature="oracle_item_selection")))]
    pub fn get_score(&self, _curr_vtime: u64) -> f64 {
        // add randomness to fix the case when all objects have the same size and frequency
        let eps = (quickrandom() % 10000) as f64 / 1.0e6 + 1.0e-4;
        // let eps = 0.000001;
        // let eps = (*self.header()).rand_num as f64 / 1.0e8;

        // let access_age = self.header().get_last_access_time().elapsed().as_secs() as f64;
        // (access_age + eps) / self.size() as f64
        // 1.0e8 / (access_age + eps) as f64 / self.size() as f64

        let freq = (self.get_freq() as f64 + eps)  * 1.0e6;
        freq / self.size() as f64
    }


    #[inline]
    #[cfg(all(feature = "oracle_reuse", feature="oracle_item_selection"))]
    pub fn get_score(&self, curr_vtime: u64) -> f64 {
        if self.header().get_future_reuse_time() == i64::MAX {
            return 0.0; 
            // return f64::MIN_POSITIVE; 
        }

        let size = self.used_size() as f64;
        let dist = (self.header().get_future_reuse_time() - curr_vtime as i64) as f64;

        assert!(dist >= 0.0,"{} - {} < 0",
            self.header().get_future_reuse_time(), curr_vtime
        );

        let eps = 0 as f64;
        return (1.0e10 + eps) / dist / size;
    }

    #[cfg(feature = "oracle_reuse")]
    #[inline]
    pub fn set_future_reuse_time(&mut self, next_vtime: i64) {
        unsafe {
            (*self.header_mut()).set_future_reuse_time(next_vtime);
        }
    }

    #[inline]
    #[cfg(feature = "last_access_time")]
    pub(crate) fn set_last_access_time(&mut self, last_access_time: CoarseInstant) {
        unsafe {
            (*self.header_mut()).set_last_access_time(last_access_time);
        }
    }

    // #[inline]
    // #[allow(dead_code)]
    // pub(crate) fn get_last_access_age(&self) -> u32 {
    //     (*self.header()).get_last_access_age()
    // }

    // #[inline]
    // #[allow(dead_code)]
    // pub(crate) fn get_create_age(&self) -> u32 {
    //     (*self.header()).get_create_age()
    // }

    // #[inline]
    // #[allow(dead_code)]
    // pub(crate) fn get_create_time(&self) -> CoarseInstant {
    //     (*self.header()).get_create_time()
    // }

    // Gets the offset to the optional data
    #[inline]
    fn optional_offset(&self) -> usize {
        ITEM_HDR_SIZE
    }

    // Gets the offset to the key
    #[inline]
    fn key_offset(&self) -> usize {
        self.optional_offset() + self.olen() as usize
    }

    // Gets the offset to the value
    #[inline]
    fn value_offset(&self) -> usize {
        self.key_offset() + self.klen() as usize
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn used_size(&self) -> usize {
        align_size(
            ITEM_HDR_SIZE + self.olen() as usize + self.klen() as usize + self.vlen() as usize,
        )
    }

    /// Returns item size, rounded up for alignment
    #[inline]
    pub(crate) fn size(&self) -> usize {
        align_size(
            ITEM_HDR_SIZE
                + self.olen() as usize
                + self.klen() as usize
                + self.vlen() as usize as usize,
        )
    }

    /// Sets the tombstone
    #[inline]
    pub(crate) fn tombstone(&mut self) {
        unsafe { (*self.header_mut()).set_deleted(true) }
    }

    /// Checks if the item is deleted
    #[inline]
    pub(crate) fn is_deleted(&self) -> bool {
        self.header().is_deleted()
    }

    // #[inline]
    // pub fn is_expired(&self) -> bool {
    //     self.header().is_expired()
    // }

    // #[inline]
    // pub fn is_deleted_or_empty(&self) -> bool {
    //     self.header().is_deleted_or_empty()
    // }

    // #[inline]
    // #[allow(dead_code)]
    // pub fn deleted_or_expired(&self) -> bool {
    //     self.header().is_deleted_or_expired()
    // }
}

#[inline]
pub(crate) fn align_size(size: usize) -> usize {
    (((size - 1) >> 3) + 1) << 3
}

impl std::fmt::Debug for RawItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("RawItem")
            .field("size", &self.size())
            .field("header", self.header())
            .field(
                "raw",
                &format!("{:02X?}", unsafe {
                    &std::slice::from_raw_parts(self.data, self.size())
                }),
            )
            .finish()
    }
}
