// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Hash buckets are used to store a group of item entries with a shared
//! metadata entry.
//!
//! Bucket Info:
//! ```text
//! ┌──────┬──────┬──────────────┬──────────────────────────────┐
//! │ ---- │CHAIN │  TIMESTAMP   │             CAS              │
//! │      │ LEN  │              │                              │
//! │8 bit │8 bit │    16 bit    │            32 bit            │
//! │      │      │              │                              │
//! │0    7│8   15│16          31│32                          63│
//! └──────┴──────┴──────────────┴──────────────────────────────┘
//! ```
//!
//! Item Info:
//! ```text
//! ┌──────────┬──────┬──────────────────────┬──────────────────┐
//! │   TAG    │ FREQ │        SEG ID        │      OFFSET      │
//! │          │      │                      │                  │
//! │  12 bit  │8 bit │        24 bit        │      20 bit      │
//! │          │      │                      │                  │
//! │0       11│12  19│20                  43│44              63│
//! └──────────┴──────┴──────────────────────┴──────────────────┘
//! ```

use super::*;

/// A mask to get the bits containing the chain length from the bucket info
pub(crate) const BUCKET_CHAIN_LEN_MASK: u64 = 0x00FF_0000_0000_0000;
/// A mask to get the bits containing the timestamp from the bucket info
// pub(crate) const TS_MASK: u64 = 0x0000_FFFF_0000_0000;
/// A mask to get the bits containing the CAS value from the bucket info
pub(crate) const CAS_MASK: u64 = 0x0000_0000_FFFF_FFFF;

/// Number of bits to shift the bucket info masked with the chain length mask
/// to get the actual chain length
pub(crate) const BUCKET_CHAIN_LEN_BIT_SHIFT: u64 = 48;
/// Number of bits to shift the bucket info masked with the timestamp mask to
/// get the timestamp
// pub(crate) const TS_BIT_SHIFT: u64 = 32;

// item info

/// A mask to get the bits containing the item tag from the item info
pub(crate) const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
/// A mask to get the bits containing the containing segment id from the item
/// info
pub(crate) const SEG_ID_MASK: u64 = 0x0000_0FFF_FFF0_0000;
/// A mask to get the bits containing the offset within the containing segment
/// from the item info
pub(crate) const OFFSET_MASK: u64 = 0x0000_0000_000F_FFFF;


// pub(crate) const GHOST_MASK: u64 = 0x0008_0000_0000_0000;
// // max index is 1 << 15 - 1
// pub(crate) const TRAIN_DATA_IDX_MASK: u64 = 0x0007_FFF0_0000_0000;
// // snapshot time // 8 (snapshot_time >> 3), max snapshot time is 2^21 - 1
// pub(crate) const SNAPSHOT_TIME_IDX: u64 = 0x0000_000F_FFFC_0000;
// // object size // 16 (size >> 4), max size is 2^22-1 as 4MB
// pub(crate) const SIZE_IDX: u64 = 0x0000_0000_0003_FFFF;


// pub(crate) const TRAIN_DATA_IDX_BIT_SHIFT: u64 = 36;
// pub(crate) const SNAPSHOT_TIME_BIT_SHIFT: u64 = 18;


/// Number of bits to shift the item info masked with the segment id mask to get
/// the actual segment id
pub(crate) const SEG_ID_BIT_SHIFT: u64 = 20;
/// Offset alignment in bits, this value results in 8byte alignment within the
/// segment
pub(crate) const OFFSET_UNIT_IN_BIT: u64 = 3;

/// Mask to get the item info without the frequency smoothing bit set
// pub(crate) const CLEAR_FREQ_SMOOTH_MASK: u64 = 0xFFF7_FFFF_FFFF_FFFF;

/// Mask to get the lower 16 bits from a timestamp
// pub(crate) const PROC_TS_MASK: u64 = 0x0000_0000_0000_FFFF;

#[derive(Copy, Clone)]
pub(crate) struct HashBucket {
    pub(super) data: [u64; N_BUCKET_SLOT],
}

impl HashBucket {
    pub fn new() -> Self {
        Self {
            data: [0; N_BUCKET_SLOT],
        }
    }
}

/// Calculate a item's tag from the hash value
#[inline]
pub const fn tag_from_hash(hash: u64) -> u64 {
    (hash & TAG_MASK) | 0x0010000000000000
}

/// Get the item's offset from the item info
#[inline]
pub const fn get_offset(item_info: u64) -> u64 {
    (item_info & OFFSET_MASK) << OFFSET_UNIT_IN_BIT
}

/// Get the item's segment from the item info
#[inline]
pub const fn get_seg_id(item_info: u64) -> Option<NonZeroU32> {
    NonZeroU32::new(((item_info & SEG_ID_MASK) >> SEG_ID_BIT_SHIFT) as u32)
}

/// Get the CAS value from the bucket info
#[inline]
pub const fn get_cas(bucket_info: u64) -> u32 {
    (bucket_info & CAS_MASK) as u32
}

// /// Get the timestamp from the bucket info
// #[inline]
// pub const fn get_ts(bucket_info: u64) -> u64 {
//     (bucket_info & TS_MASK) >> TS_BIT_SHIFT
// }

/// Get the tag from the item info
#[inline]
pub const fn get_tag(item_info: u64) -> u64 {
    item_info & TAG_MASK
}


// /// Get the item frequency from the item info
// #[inline]
// pub const fn is_ghost(item_info: u64) -> bool {
//     (item_info & GHOST_MASK) == 0
// }

// /// Returns the item info with the frequency cleared
// #[inline]
// pub const fn clear_ghost(item_info: u64) -> u64 {
//     item_info & !GHOST_MASK
// }

// #[inline]
// pub const fn get_train_data_idx(item_info: u64) -> u64 {
//     (item_info & TRAIN_DATA_IDX_MASK) >> TRAIN_DATA_IDX_BIT_SHIFT
// }

// #[inline]
// pub const fn get_snapshot_time(item_info: u64) -> u64 {
//     (item_info & !GHOST_MASK) >> SNAPSHOT_TIME_BIT_SHIFT
// }

// #[inline]
// pub const fn get_size(item_info: u64) -> u64 {
//     item_info & !GHOST_MASK
// }




/// Get the chain length from the bucket info
#[inline]
pub const fn chain_len(bucket_info: u64) -> u64 {
    (bucket_info & BUCKET_CHAIN_LEN_MASK) >> BUCKET_CHAIN_LEN_BIT_SHIFT
}

/// Create the item info from the tag, segment id, and offset
#[inline]
pub const fn build_item_info(tag: u64, seg_id: NonZeroU32, offset: u64) -> u64 {
    tag | ((seg_id.get() as u64) << SEG_ID_BIT_SHIFT) | (offset >> OFFSET_UNIT_IN_BIT)
}

// #[inline]
// pub const fn build_ghost_entry(tag: u64, train_data_idx: u64, snapshot_time: u64, item_size: u64) -> u64 {
//     tag | (train_data_idx << TRAIN_DATA_IDX_BIT_SHIFT) | (snapshot_time << SNAPSHOT_TIME_BIT_SHIFT) | item_size | GHOST_MASK
// }
