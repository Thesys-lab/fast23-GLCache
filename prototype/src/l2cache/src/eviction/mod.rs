// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Eviction is used to select a segment to remove when the cache becomes full.
//! An eviction [`Policy`] determines what data will be evicted from the cache.

use core::cmp::{max, Ordering};
use core::num::NonZeroU32;

use rand::Rng;
use rustcommon_time::CoarseInstant;

use crate::rng;
use crate::segments::*;
use crate::learning::L2Learner;
use crate::Random;
mod policy;

pub use policy::Policy;

/// The `Eviction` struct is used to rank and return segments for eviction. It
/// implements eviction strategies corresponding to the `Policy`.
pub struct Eviction {
    pub policy: Policy,
    pub last_update_time: CoarseInstant,
    pub ranked_segs: Box<[Option<NonZeroU32>]>,
    pub index: usize,
    rng: Box<Random>,
}

impl Eviction {
    /// Creates a new `Eviction` struct which will handle up to `nseg` segments
    /// using the specified eviction policy.
    pub fn new(nseg: usize, policy: Policy) -> Self {
        let mut ranked_segs = Vec::with_capacity(0);
        ranked_segs.reserve_exact(nseg);
        ranked_segs.resize_with(nseg, || None);
        let ranked_segs = ranked_segs.into_boxed_slice();

        Self {
            policy,
            last_update_time: CoarseInstant::recent(),
            ranked_segs,
            index: 0,
            rng: Box::new(rng()),
        }
    }

    #[inline]
    pub fn policy(&self) -> &Policy {
        &self.policy
    }
    
    #[inline]
    pub fn policy_mut(&mut self) -> &mut Policy {
        &mut self.policy
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_l2cache(&self) -> bool {
        match self.policy {
            Policy::LearnedMerge { .. } => true,
            _ => false,
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_l2learner(&mut self) -> &mut L2Learner {
        match self.policy {
            Policy::LearnedMerge { ref mut learner, .. } => learner,
            _ => panic!("not a l2cache"),
        }
    }

    /// Returns the segment id of the least valuable segment
    pub fn least_valuable_seg(&mut self) -> Option<NonZeroU32> {
        // println!("index {} evict seg {:?}", self.index, self.ranked_segs[self.index]); 
        if self.index < self.ranked_segs.len() {
            self.index += 1; 
            self.ranked_segs[self.index - 1]
        } else {
            None
        }
    }

    /// Returns a random u32
    #[inline]
    pub fn random(&mut self) -> u32 {
        self.rng.gen()
    }

    #[inline]
    pub fn should_rerank(&self) -> bool {
        let now = CoarseInstant::recent();
        match self.policy {
            Policy::None | Policy::Random | Policy::RandomFifo | Policy::Merge{..} => false,
            Policy::Fifo | Policy::Cte | Policy::Util => {
                if self.ranked_segs[0].is_none()
                    || (now - self.last_update_time).as_secs() > 1
                    || self.ranked_segs.len() < (self.index + 8)
                {
                    true
                } else {
                    false
                }
            }, 
            Policy::LearnedMerge{rerank_interval, ..} |
                Policy::OracleMerge{rerank_interval, ..} => {
                
                if self.ranked_segs[0].is_none() {
                    return true;
                }

                if self.index >= self.ranked_segs.len() / rerank_interval as usize {
                    return true;
                } 

                false
            }
        }
    }

    pub fn rerank(&mut self, headers: &[SegmentHeader]) {
        let mut ids: Vec<NonZeroU32> = headers.iter().map(|h| h.id()).collect();
        match &self.policy {
            Policy::None | Policy::Random | Policy::RandomFifo | Policy::Merge{..} => {
                return;
            }
            Policy::Fifo { .. } => {
                ids.sort_by(|a, b| {
                    Self::compare_fifo(
                        &headers[a.get() as usize - 1],
                        &headers[b.get() as usize - 1],
                    )
                });
            }
            Policy::Cte { .. } => {
                ids.sort_by(|a, b| {
                    Self::compare_cte(
                        &headers[a.get() as usize - 1],
                        &headers[b.get() as usize - 1],
                    )
                });
            }
            Policy::Util { .. } => {
                ids.sort_by(|a, b| {
                    Self::compare_util(
                        &headers[a.get() as usize - 1],
                        &headers[b.get() as usize - 1],
                    )
                });
            }
            Policy::OracleMerge { .. } => {
                ids.sort_by(|a, b| {
                    Self::compare_segment_utility(
                        &headers[a.get() as usize - 1],
                        &headers[b.get() as usize - 1],
                    )
                });
            }
            Policy::LearnedMerge { .. } => {
                ids.sort_by(|a, b| {
                    Self::compare_segment_utility(
                        &headers[a.get() as usize - 1],
                        &headers[b.get() as usize - 1],
                    )
                });
                // println!("ranked seg {:?}", ids);
                // println!("segment util {:?}", ids.iter().map(|id| headers[id.get() as usize - 1].pred_utility).collect::<Vec<_>>());
            }
        }
        for (i, id) in self.ranked_segs.iter_mut().enumerate() {
            *id = Some(ids[i]);
        }
        self.index = 0;
        self.last_update_time = CoarseInstant::recent();
    }

    fn compare_fifo(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else if max(lhs.create_at(), lhs.merge_at()) > max(rhs.create_at(), rhs.merge_at()) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }

    fn compare_cte(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else if (lhs.create_at() + lhs.ttl()) > (rhs.create_at() + rhs.ttl()) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }

    // utilization 
    fn compare_util(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else if lhs.live_bytes() > rhs.live_bytes() {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }

    fn compare_segment_utility(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if lhs.pred_utility > rhs.pred_utility {
            Ordering::Greater
        } else if lhs.pred_utility < rhs.pred_utility {
            Ordering::Less
        } else {
            if max(lhs.create_at(), lhs.merge_at()) > max(rhs.create_at(), rhs.merge_at()) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
    }

    #[inline]
    /// Returns the maximum number of segments which can be merged during a
    /// single merge operation. Applies to both eviction and compaction merge
    /// passes.
    pub fn max_merge(&self) -> usize {
        match self.policy {
            Policy::Merge { max, .. } => max,
            Policy::LearnedMerge{max_merge, .. } => max_merge,
            Policy::OracleMerge{max_merge, .. } => max_merge, 
            _ => 0
        } 
    }

    #[inline]
    /// Returns the number of segments which should be combined during an
    /// eviction merge.
    pub fn n_merge(&self) -> usize {
        match self.policy {
            Policy::Merge { merge, .. } => merge,
            Policy::LearnedMerge{n_merge, .. } => n_merge,
            Policy::OracleMerge{n_merge, .. } => n_merge, 
            _ => 0
        } 
    }

    #[inline]
    #[allow(dead_code)]
    /// Returns the number of segments which should be combined during a
    /// compaction merge.
    pub fn n_compact(&self) -> usize {
        if let Policy::Merge { compact, .. } = self.policy {
            compact
        } else {
            2
        }
    }

    // #[inline]
    // /// The compact ratio serves as a low watermark for triggering compaction
    // /// and combining segments without eviction.
    // pub fn compact_ratio(&self) -> f64 {
    //     if self.n_compact() == 0 {
    //         0.0
    //     } else {
    //         1.0 / self.n_compact() as f64
    //     }
    // }

    #[inline]
    #[allow(dead_code)]
    /// The target ratio is used during eviction based merging and represents
    /// the desired occupancy of a segment once least accessed items are
    /// evicted.
    pub fn target_ratio(&self) -> f64 {
        1.0 / self.n_merge() as f64
    }

    #[inline]
    #[allow(dead_code)]
    /// The stop ratio is used during merging as a high watermark and causes
    /// the merge pass to stop when the target segment has a higher occupancy
    pub fn stop_ratio(&self) -> f64 {
        // self.target_ratio() * (self.n_merge() - 1) as f64 + 0.05
        0.95
    }
}
