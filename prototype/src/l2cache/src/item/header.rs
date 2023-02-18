// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A header which is stored with the item data which contains information about
//! the item representation within the segment.
//!
//! Item Header:
//! ```text
//! ┌──────────────────────────────┬──────────────────────┬──────┬──────┐
//! │      MAGIC (Optional)        │         VLEN         │ KLEN │FLAGS │
//! │                              │                      │      │      │
//! │            32 bit            │        24 bit        │8 bit │ 8bit │
//! │          0xDECAFBAD          │                      │      │      │
//! │0                           31│32                  55│56  63│64  71│
//! └──────────────────────────────┴──────────────────────┴──────┴──────┘
//! ```
//!
//! Flags:
//! ```text
//! ┌──────────────┬──────────────┬──────────────────────────────┐
//! │   NUMERIC?   │   DELETED?   │             OLEN             │
//! │              │              │                              │
//! │    1 bit     │    1 bit     │            6 bit             │
//! │              │              │                              │
//! │      64      │      65      │  66                      71  │
//! └──────────────┴──────────────┴──────────────────────────────┘
//! ```

// item constants
use rustcommon_time::CoarseInstant;
use crate::*; 

/// The size of the item header in bytes
pub const ITEM_HDR_SIZE: usize = std::mem::size_of::<crate::item::ItemHeader>();
// pub const HEADER_ONLY_OBJ_SIZE: usize = (((ITEM_HDR_SIZE - 1) >> 3) + 1) << 3;
// lazy_static! {
//     pub static ref HEADER_ONLY_OBJ_SIZE: usize = align_size(ITEM_HDR_SIZE);
// }

#[cfg(feature = "magic")]
/// The magic bytes to store at the start of the item
pub const ITEM_MAGIC: u32 = 0xDECAFBAD;
#[cfg(feature = "magic")]
/// The length of the item magic in bytes
pub const ITEM_MAGIC_SIZE: usize = std::mem::size_of::<u32>();
#[cfg(not(feature = "magic"))]
#[allow(dead_code)]
/// The length of the item magic in bytes
pub const ITEM_MAGIC_SIZE: usize = 0;

// masks and shifts
// klen/vlen pack together
/// A mask to get the key length from the item header's length field
const KLEN_MASK: u32 = 0x000000FF;
/// A mask used to get the bits containing the item value length from the item
/// header's length field
const VLEN_MASK: u32 = 0xFFFFFF00;

/// The number of bits to shift the length field masked with the value length
/// mask to get the actual value length
const VLEN_SHIFT: u32 = 8;

// olen/del/num
/// A mask to get the optional data length in bytes from the item header's flags
/// field
const OLEN_MASK: u8 = 0b00011111;
/// A mask to get the bit indicating the item has been deleted from the item
/// header's flags field
const DEL_MASK: u8 = 0b01000000;
/// A mask to get the bit indicating the item value should be treated as a
/// numeric from the item header's flags field
const NUM_MASK: u8 = 0b10000000;

const ACCESSED_SINCE_WRITE_MASK: u8 = 0b00100000;

const ACCESSED_SINCE_SNAPSHOT_MASK: u8 = 0b00010000; 

/// A per-item header which is stored with the item data within a segment. This
/// contains information about the item's raw representation within the segment.
// NOTE: repr(packed) is necessary to get the smallest representation. The
// struct is always taken from an aligned pointer cast. This can potentially
// result in UB when fields are referenced. Fields that require access by
// reference must be strategically placed to ensure alignment and avoid UB.
#[repr(packed)]
pub struct ItemHeader {
    #[cfg(feature = "magic")]
    magic: u32,
    len: u32,  // packs vlen:24 klen:8

    #[cfg(feature = "oracle_reuse")]
    future_reuse_time: i64,

    // TODO: can compress to 12 bit each using the TTL bucket idx 
    // create_time: CoarseInstant,

    #[cfg(feature = "last_access_time")]
    last_access_time: CoarseInstant,

    freq: u8,

    flags: u8, // packs is_num:1, deleted:1, has_accessed_since_write:1, accessed_since_snapshot:1, olen:4
}

impl ItemHeader {
    /// Get the magic bytes from the header
    #[cfg(feature = "magic")]
    #[inline]
    pub fn magic(&self) -> u32 {
        self.magic
    }

    /// Write the magic bytes into the header
    #[cfg(feature = "magic")]
    #[inline]
    pub fn set_magic(&mut self) {
        self.magic = ITEM_MAGIC;
    }

    /// Check the magic bytes
    #[inline]
    pub fn check_magic(&self) {
        #[cfg(feature = "magic")]
        assert_eq!(self.magic(), ITEM_MAGIC);
    }

    /// Get the item's key length
    #[inline]
    pub fn klen(&self) -> u8 {
        self.len as u8
    }

    /// Get the item's value length
    #[inline]
    pub fn vlen(&self) -> u32 {
        self.len >> VLEN_SHIFT
    }

    /// get the optional data length
    #[inline]
    pub fn olen(&self) -> u8 {
        0
        // self.flags & OLEN_MASK
    }

    /// Is the item a numeric value?
    #[inline]
    pub fn is_num(&self) -> bool {
        self.flags & NUM_MASK != 0
    }

    #[inline]
    pub fn has_accessed_since_write(&self) -> bool {
        self.flags & ACCESSED_SINCE_WRITE_MASK != 0
    }

    #[inline]
    #[allow(dead_code)]
    pub fn has_accessed_since_snapshot(&self) -> bool {
        self.flags & ACCESSED_SINCE_SNAPSHOT_MASK != 0
    }

    /// Is the item deleted?
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.flags & DEL_MASK != 0
    }

    /// Set the key length by changing just the low byte
    #[inline]
    pub fn set_klen(&mut self, len: u8) {
        self.len = (self.len & !KLEN_MASK) | (len as u32);
    }

    /// Set the value length by changing just the upper bytes
    // TODO(bmartin): where should we do error handling for out-of-range?
    #[inline]
    pub fn set_vlen(&mut self, len: u32) {
        debug_assert!(len <= (u32::MAX >> VLEN_SHIFT));
        self.len = (self.len & !VLEN_MASK) | (len << VLEN_SHIFT);
    }

    /// Mark the item as deleted
    #[inline]
    pub fn set_deleted(&mut self, deleted: bool) {
        if deleted {
            self.flags |= DEL_MASK
        } else {
            self.flags &= !DEL_MASK
        }
    }

    /// Mark the item as numeric
    #[inline]
    pub fn set_num(&mut self, num: bool) {
        if num {
            self.flags |= NUM_MASK
        } else {
            self.flags &= !NUM_MASK
        }
    }

    #[inline]
    pub fn set_accessed_since_write(&mut self, accessed: bool) {
        if accessed {
            self.flags |= ACCESSED_SINCE_WRITE_MASK
        } else {
            self.flags &= !ACCESSED_SINCE_WRITE_MASK
        }
    }

    #[inline]
    pub fn set_accessed_since_snapshot(&mut self, accessed: bool) {
        if accessed {
            self.flags |= ACCESSED_SINCE_SNAPSHOT_MASK
        } else {
            self.flags &= !ACCESSED_SINCE_SNAPSHOT_MASK
        }
    }

    #[inline]
    pub fn set_freq(&mut self, freq: u8) {
        self.freq = freq;
    }

    #[inline]
    pub fn get_freq(&self) -> u8 {
        self.freq as u8
    }

    /// Set the optional length
    #[inline]
    pub fn set_olen(&mut self, len: u8) {
        debug_assert!(len <= OLEN_MASK);
        self.flags = (self.flags & !OLEN_MASK) | len;
    }

    // #[inline]
    // #[allow(dead_code)]
    // pub fn set_create_time(&mut self, create_time: CoarseInstant) {
    //     self.create_time = create_time;
    // }

    // #[allow(dead_code)]
    // #[inline]
    // pub fn get_create_time(&self) -> CoarseInstant {
    //     self.create_time
    // }

    #[inline]
    #[cfg(feature = "last_access_time")]
    #[allow(dead_code)]
    pub fn get_last_access_time(&self) -> CoarseInstant {
        self.last_access_time
    }

    #[inline]
    #[cfg(feature = "last_access_time")]
    pub fn set_last_access_time(&mut self, last_access_time: CoarseInstant) {
        self.last_access_time = last_access_time;
    }

    // #[inline]
    // #[allow(dead_code)]
    // pub fn get_last_access_age(&self) -> u32 {
    //     CoarseInstant::recent().as_secs() - self.last_access_time.as_secs()
    // }

    // #[inline]
    // #[allow(dead_code)]
    // pub fn get_create_age(&self) -> u32 {
    //     (CoarseInstant::now() - self.create_time).as_secs()
    // }


    #[inline]
    pub fn update_access_metadata(&mut self, _last_access_time: CoarseInstant) {
        // let old_age = (self.last_access_time - self.create_time).as_secs(); 
        // if (last_access_time - self.last_access_time).as_secs() > old_age / 10 {
        //     self.last_access_time = last_access_time;

        //     if self.freq < u8::MAX / 2 {
        //         self.freq += 1;
        //     } else {
        //         if self.freq < u8::MAX && quickrandom() % (self.freq as u64) < 2 {
        //             self.freq += 1;
        //         }
        //     }    
        // }

        // self.last_access_time = last_access_time;

        // if self.freq < u8::MAX {
            // self.freq += 1;
        // } 

        if self.freq < 8 {
            self.freq += 1;
        } else {
            if self.freq < u8::MAX && quickrandom() % (self.freq as u64) < 2 {
                self.freq += 1;
            }
        }


        self.set_accessed_since_write(true); 
    }

    #[inline]
    #[cfg(feature = "oracle_reuse")]
    pub fn get_future_reuse_time(&self) -> i64 {
        self.future_reuse_time
    }

    #[inline]
    #[cfg(feature = "oracle_reuse")]
    pub fn set_future_reuse_time(&mut self, future_reuse_time: i64) {
        self.future_reuse_time = future_reuse_time;
    }
}

#[cfg(not(feature = "magic"))]
impl std::fmt::Debug for ItemHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("ItemHeader")
            .field("klen", &self.klen())
            .field("vlen", &self.vlen())
            .field("is_num", &self.is_num())
            .field("deleted", &self.is_deleted())
            .field("olen", &self.olen())
            .field("freq", &self.get_freq())
            .finish()
    }
}

#[cfg(feature = "magic")]
impl std::fmt::Debug for ItemHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let magic = self.magic;
        f.debug_struct("ItemHeader")
            .field("magic", &format!("0x{:X}", magic))
            .field("klen", &self.klen())
            .field("vlen", &self.vlen())
            .field("is_num", &self.is_num())
            .field("deleted", &self.is_deleted())
            .field("olen", &self.olen())
            .field("freq", &self.get_freq())
            .finish()
    }
}
