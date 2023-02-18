// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Items are the base unit of data stored within the cache.

mod header;
mod raw;
mod reserved;

#[cfg(any(feature = "magic", feature = "debug"))]
pub(crate) use header::ITEM_MAGIC_SIZE;

pub(crate) use header::{ItemHeader, ITEM_HDR_SIZE};
pub(crate) use raw::{RawItem, align_size};
pub(crate) use reserved::ReservedItem;

use rustcommon_time::{CoarseInstant}; 

/// Items are the base unit of data stored within the cache.
pub struct Item {
    cas: u32,
    pub(crate) raw: RawItem,
}

impl Item {
    /// Creates a new `Item` from its parts
    pub(crate) fn new(raw: RawItem, cas: u32) -> Self {
        Item { cas, raw }
    }

    /// If the `magic` or `debug` features are enabled, this allows for checking
    /// that the magic bytes at the start of an item match the expected value.
    ///
    /// # Panics
    ///
    /// Panics if the magic bytes are incorrect, indicating that the data has
    /// become corrupted or the item was loaded from the wrong offset.
    pub(crate) fn check_magic(&self) {
        self.raw.check_magic()
    }

    /// Borrow the item key
    #[inline]
    pub fn key(&self) -> &[u8] {
        self.raw.key()
    }

    /// Borrow the item value
    #[inline]
    pub fn value(&self) -> &[u8] {
        self.raw.value()
    }

    // #[inline]
    // pub fn set_last_access_time(&mut self, last_access_time: CoarseInstant) {
    //     unsafe {
    //         (*self.raw.header_mut()).set_last_access_time(last_access_time);
    //     }
    // }

    #[inline]
    pub fn update_access_metadata(&mut self, last_access_time: CoarseInstant) {
        unsafe {
            (*self.raw.header_mut()).update_access_metadata(last_access_time);
        }
    }

    #[cfg(feature="oracle_reuse")]
    #[inline]
    pub fn set_future_reuse_vtime(&mut self, future_reuse_time: i64) {
        unsafe {
            (*self.raw.header_mut()).set_future_reuse_time(future_reuse_time);
        }
    }

    /// CAS value for the item
    pub fn cas(&self) -> u32 {
        self.cas
    }

    /// Borrow the optional data
    pub fn optional(&self) -> Option<&[u8]> {
        self.raw.optional()
    }

    // #[inline]
    // pub fn get_raw(&self) -> &RawItem {
    //     &self.raw
    // }

    // #[inline]
    // pub fn get_raw_mut(&mut self) -> &mut RawItem {
    //     &mut self.raw
    // }

    // #[inline]
    // pub fn incr_freq(&mut self) {
    //     unsafe {
    //         (*self.raw.header_mut()).incr_freq();
    //     }
    // }

    #[inline]
    pub fn get_freq(&self) -> u32 {
        (*self.raw.header()).get_freq() as u32
    }
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Item")
            .field("cas", &self.cas())
            .field("raw", &self.raw)
            .finish()
    }
}
