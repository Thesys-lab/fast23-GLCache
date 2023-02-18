// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! A collection of [`TtlBucket`]s which covers the full range of TTLs.
//!
//! We use a total of 1024 buckets to represent the full range of TTLs. We
//! divide the buckets into 4 ranges:
//! * 1-128s are stored in buckets which are 1 sec wide,
//! * 128-2048s (1 second - ~34 minutes) are stored in buckets which are 8s wide.
//! * 2048-32_768s (~34 minutes - ~9 hours) are stored in buckets which are 128s
//!   (~2 minutes) wide.
//! * 32_768-524_288s (~9 hours - ~6 days) are stored in buckets which are 2048s
//!   (~34 minutes) wide.
//! * 524_288-8_388_608s (~6 days - ~97 days) are stored in buckets which are
//!   32_768s (~9 hours) wide.
//! * TTLs beyond 8_388_608s (~97 days) and TTLs of 0 are all treated as the max
//!   TTL.
//! all time range are [) range, the first is included, the last is excluded.
//!
//! See the
//! [Segcache paper](https://www.usenix.org/system/files/nsdi21-yang.pdf) for
//! more detail.

use crate::*;

// const N_BUCKET_PER_STEP_N_BIT: usize = 8;
// const N_BUCKET_PER_STEP: usize = 1 << N_BUCKET_PER_STEP_N_BIT;

// const TTL_BUCKET_INTERVAL_N_BIT_1: usize = 3;
// const TTL_BUCKET_INTERVAL_N_BIT_2: usize = 7;
// const TTL_BUCKET_INTERVAL_N_BIT_3: usize = 11;
// const TTL_BUCKET_INTERVAL_N_BIT_4: usize = 15;

// const TTL_BUCKET_INTERVAL_1: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_1;
// const TTL_BUCKET_INTERVAL_2: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_2;
// const TTL_BUCKET_INTERVAL_3: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_3;
// const TTL_BUCKET_INTERVAL_4: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_4;

// const TTL_BOUNDARY_1: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_1 + N_BUCKET_PER_STEP_N_BIT);
// const TTL_BOUNDARY_2: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_2 + N_BUCKET_PER_STEP_N_BIT);
// const TTL_BOUNDARY_3: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_3 + N_BUCKET_PER_STEP_N_BIT);

// const MAX_N_TTL_BUCKET: usize = N_BUCKET_PER_STEP * 4;
// const MAX_TTL_BUCKET_IDX: usize = MAX_N_TTL_BUCKET - 1;

// pub const TTL_BUCKET_INTERVAL1: i32 = 1;
// pub const TTL_BUCKET_INTERVAL2: i32 = 16;
// pub const TTL_BUCKET_INTERVAL3: i32 = 128;
// pub const TTL_BUCKET_INTERVAL4: i32 = 2048;
// pub const TTL_BUCKET_INTERVAL5: i32 = 32768;

// pub const TTL_BOUNDARY1: i32 = 128;
// pub const TTL_BOUNDARY2: i32 = 2048;
// pub const TTL_BOUNDARY3: i32 = 32768;
// pub const TTL_BOUNDARY4: i32 = 524_288;
// pub const TTL_BOUNDARY5: i32 = 8_388_608;

// pub const N_TTL_BUCKET1: usize = (TTL_BOUNDARY1 / TTL_BUCKET_INTERVAL1) as usize;
// pub const N_TTL_BUCKET2: usize = ((TTL_BOUNDARY2 - TTL_BOUNDARY1) / TTL_BUCKET_INTERVAL2) as usize;
// pub const N_TTL_BUCKET3: usize = ((TTL_BOUNDARY3 - TTL_BOUNDARY2) / TTL_BUCKET_INTERVAL3) as usize;
// pub const N_TTL_BUCKET4: usize = ((TTL_BOUNDARY4 - TTL_BOUNDARY3) / TTL_BUCKET_INTERVAL4) as usize;
// pub const N_TTL_BUCKET5: usize = ((TTL_BOUNDARY5 - TTL_BOUNDARY4) / TTL_BUCKET_INTERVAL5) as usize;

// pub const N_TTL_BUCKET2_ACCU: usize = N_TTL_BUCKET1 + N_TTL_BUCKET2;
// pub const N_TTL_BUCKET3_ACCU: usize = N_TTL_BUCKET2_ACCU + N_TTL_BUCKET3;
// pub const N_TTL_BUCKET4_ACCU: usize = N_TTL_BUCKET3_ACCU + N_TTL_BUCKET4;
// pub const MAX_N_TTL_BUCKET: usize =
//     N_TTL_BUCKET1 + N_TTL_BUCKET2 + N_TTL_BUCKET3 + N_TTL_BUCKET4 + N_TTL_BUCKET5;

pub const TTL_BUCKET_INTERVAL1: i32 = 1;
pub const TTL_BUCKET_INTERVAL2: i32 = 4;
pub const TTL_BUCKET_INTERVAL3: i32 = 32;
pub const TTL_BUCKET_INTERVAL4: i32 = 128;
pub const TTL_BUCKET_INTERVAL5: i32 = 2048;
pub const TTL_BUCKET_INTERVAL6: i32 = 32768;

pub const TTL_BOUNDARY1: i32 = 24;
pub const TTL_BOUNDARY2: i32 = 128;
pub const TTL_BOUNDARY3: i32 = 2048;
pub const TTL_BOUNDARY4: i32 = 32768;
pub const TTL_BOUNDARY5: i32 = 524_288;
pub const TTL_BOUNDARY6: i32 = 8_388_608;

pub const N_TTL_BUCKET1 : usize = (TTL_BOUNDARY1 / TTL_BUCKET_INTERVAL1) as usize;
pub const N_TTL_BUCKET2 : usize = ((TTL_BOUNDARY2 - TTL_BOUNDARY1) / TTL_BUCKET_INTERVAL2) as usize;
pub const N_TTL_BUCKET3 : usize = ((TTL_BOUNDARY3 - TTL_BOUNDARY2) / TTL_BUCKET_INTERVAL3) as usize;
pub const N_TTL_BUCKET4 : usize = ((TTL_BOUNDARY4 - TTL_BOUNDARY3) / TTL_BUCKET_INTERVAL4) as usize;
pub const N_TTL_BUCKET5 : usize = ((TTL_BOUNDARY5 - TTL_BOUNDARY4) / TTL_BUCKET_INTERVAL5) as usize;
pub const N_TTL_BUCKET6 : usize = ((TTL_BOUNDARY6 - TTL_BOUNDARY5) / TTL_BUCKET_INTERVAL6) as usize;

pub const N_TTL_BUCKET2_ACCU : usize = N_TTL_BUCKET1 + N_TTL_BUCKET2;
pub const N_TTL_BUCKET3_ACCU : usize = N_TTL_BUCKET2_ACCU + N_TTL_BUCKET3;
pub const N_TTL_BUCKET4_ACCU : usize = N_TTL_BUCKET3_ACCU + N_TTL_BUCKET4;
pub const N_TTL_BUCKET5_ACCU : usize = N_TTL_BUCKET4_ACCU + N_TTL_BUCKET5;
pub const MAX_N_TTL_BUCKET: usize = N_TTL_BUCKET1 + N_TTL_BUCKET2 + N_TTL_BUCKET3 + N_TTL_BUCKET4 + N_TTL_BUCKET5 + N_TTL_BUCKET6;

pub struct TtlBuckets {
    pub(crate) buckets: Box<[TtlBucket]>,
    pub(crate) last_expired: CoarseInstant,
    pub(crate) first_in_use_bucket_idx: Option<u32>,
}

impl TtlBuckets {
    pub fn new() -> Self {
        let intervals = [
            TTL_BUCKET_INTERVAL1,
            TTL_BUCKET_INTERVAL2,
            TTL_BUCKET_INTERVAL3,
            TTL_BUCKET_INTERVAL4,
            TTL_BUCKET_INTERVAL5,
            TTL_BUCKET_INTERVAL6,
        ];
        let boundaries = [
            TTL_BOUNDARY1,
            TTL_BOUNDARY2,
            TTL_BOUNDARY3,
            TTL_BOUNDARY4,
            TTL_BOUNDARY5,
            TTL_BOUNDARY6,
        ];

        let mut buckets = Vec::with_capacity(0);
        buckets.reserve_exact(MAX_N_TTL_BUCKET);

        for j in 0..TTL_BOUNDARY1 / TTL_BUCKET_INTERVAL1 {
            let ttl = TTL_BUCKET_INTERVAL1 * j;
            let bucket = TtlBucket::new(ttl as i32);
            buckets.push(bucket);
        }

        for i in 1..intervals.len() {
            let n_bucket = (boundaries[i] - boundaries[i - 1]) / intervals[i];
            for j in 0..n_bucket {
                let ttl = boundaries[i - 1] + intervals[i] * j;
                let bucket = TtlBucket::new(ttl as i32);
                buckets.push(bucket);
            }
        }

        let buckets = buckets.into_boxed_slice();
        let last_expired = CoarseInstant::now();

        Self {
            buckets,
            last_expired,
            first_in_use_bucket_idx: None, 
        }
    }

    #[allow(dead_code)]
    pub fn get_num_active(&self) -> usize {
        let mut n_active_buckets = 0;
        for bucket in self.buckets.iter() {
            if let Some(_) = bucket.head() {
                n_active_buckets += 1;
            }
        }
        n_active_buckets
    }

    #[allow(dead_code)]
    pub(crate) fn check_next_to_merge(&self, segments: &Segments) {
        for bucket in self.buckets.iter() {
            if let Some(segment) = bucket.next_to_merge() {
                assert_eq!(
                    segments.headers[segment.get() as usize - 1].ttl().as_secs(),
                    bucket.ttl() as u32,
                    "next to merge is not same, bucket {}",
                    segment.get()
                );
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_age_of_buckets(&self, segments: &Segments) {
        println!("###########################################");
        for (idx, bucket) in self.buckets.iter().enumerate() {
            if let Some(seg_id) = bucket.head() {
                let mut seg = &segments.headers[seg_id.get() as usize - 1];
                let age = seg.get_age();
                // let mut nseg = 1;
                print!(
                    "bucket {} ttl {} age {} {} seg, can_evict? ",
                    idx,
                    bucket.ttl(),
                    age,
                    bucket.nseg()
                );

                let mut n = 0;
                while n < 8 {
                    print!("{}, ", seg.not_evictable_reason());
                    n += 1;
                    let next_seg = seg.next_seg();
                    if let Some(next_seg_id) = next_seg {
                        seg = &segments.headers[next_seg_id.get() as usize - 1];
                    } else {
                        break;
                    }
                }
                // assert_eq!(bucket.get_nseg(), true);
                // println!("{}", seg.not_evictable_reason());
                println!("");
                // bucket.verify_ttl_bucket(segments);
            }
        }
        println!("###########################################");
    }

    #[inline]
    #[allow(dead_code)]
    pub fn check_is_next_to_merge(&self, id: u32) {
        for bucket in self.buckets.iter() {
            if let Some(segment) = bucket.next_to_merge() {
                if segment.get() == id {
                    panic!("find it in next to merge, bucket {:?}", bucket);
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn get_n_used_segments(&self) -> usize {
        let mut n_used_segments = 0;
        for bucket in self.buckets.iter() {
            n_used_segments += bucket.nseg();
        }
        n_used_segments as usize
    }

    pub(crate) fn get_bucket_index(&self, ttl: CoarseDuration) -> usize {
        // cannot happen < 0
        let ttl = ttl.as_secs() as i32;

        age_to_bucket_idx(ttl)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn find_next_in_use_bucket_idx(&self, curr_idx: i32) -> Option<i32> {
        for idx in curr_idx as usize + 1..self.buckets.len() {
            if self.buckets[idx].head().is_some() {
                return Some(idx as i32);
            }
        }
        None
    }

    #[inline]
    pub(crate) fn get_n_used_buckets(&self) -> usize {
        let mut cnt: usize = 0;
        for idx in 0..self.buckets.len() {
            if self.buckets[idx].head().is_some() {
                cnt += 1
            }
        }
        cnt
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn print_used_buckets(&self) {
        for idx in 0..self.buckets.len() {
            if self.buckets[idx].head().is_some() {
                print!("{}({}),", idx, self.buckets[idx].ttl()); 
            }
        }
        println!(""); 

        let mut curr_idx = self.first_in_use_bucket_idx.unwrap();
        loop {
            match self.buckets[curr_idx as usize].next_in_use_bucket_idx() {
                None => break,
                Some(idx) => {print!("{}({}),", curr_idx, self.buckets[curr_idx as usize].ttl()); curr_idx = idx; },
            }
        }
        println!("");
    }

    #[allow(dead_code)]
    pub(crate) fn get_n_linked_buckets(&self, msg: &str) -> usize {
        let mut cnt: usize = 0;
        let mut curr_idx = self.first_in_use_bucket_idx.unwrap();
        loop {
            assert!(self.buckets[curr_idx as usize].head().is_some(), 
                    "linked bucket ttl {} does not have seg {}", self.buckets[curr_idx as usize].ttl(), msg);
            cnt += 1;
            match self.buckets[curr_idx as usize].next_in_use_bucket_idx() {
                None => break,
                Some(idx) => curr_idx = idx,
            }
        }
        cnt
    }


    #[allow(dead_code)]
    pub(crate) fn verify_linked_buckets(&self, msg: &str) {
        if CoarseInstant::now().as_secs() < 2 {
            return;
        }
        let n_used = self.get_n_used_buckets();
        let n_linked = self.get_n_linked_buckets(msg);
        
        if n_used != n_linked {
            self.print_used_buckets(); 
        }

        assert_eq!(n_used, n_linked, "{}", msg);
    }

    // TODO(bmartin): confirm handling for negative TTLs here...
    pub(crate) fn get_mut_bucket(&mut self, ttl: CoarseDuration) -> &mut TtlBucket {
        let index = self.get_bucket_index(ttl);

        // NOTE: since get_bucket_index() must return an index within the slice,
        // we do not need to worry about UB here.
        unsafe { self.buckets.get_unchecked_mut(index) }
    }

    pub(crate) fn expire(
        &mut self,
        hashtable: &mut HashTable,
        segments: &mut Segments,
    ) -> usize {
        let now = CoarseInstant::now();

        if now == self.last_expired {
            return 0;
        } else {
            self.last_expired = now;
        }

        // let start = Instant::now();
        let mut expired = 0;
        let mut need_fix: bool = false; 
        for bucket in self.buckets.iter_mut() {
            let n = bucket.expire(hashtable, segments);
            expired += n;
            if n > 0 && bucket.nseg() == 0 {
                need_fix = true; 
            }
        }
        // let duration = start.elapsed();
        // println!("{:?} expired: {} segments in {:?}", now, expired, duration);

        if need_fix {
            self.fix_bucket_linking();
        }

        expired
    }
    
    /// because we do not remove link, so we need to fix it 
    #[inline]
    pub fn fix_bucket_linking(&mut self) {
        let mut last_bucket_idx : Option<u32> = None;
        for idx in (0..MAX_N_TTL_BUCKET).rev() {
            if self.buckets[idx].head().is_some() {
                self.buckets[idx].set_next_in_use_bucket_idx(last_bucket_idx);
                last_bucket_idx = Some(idx as u32);
            }
        }
        self.first_in_use_bucket_idx = last_bucket_idx;
    }

    /// find the bucket with segment most close to expire 
    #[allow(dead_code)]
    pub(crate) fn find_ttl_bucket_idx_cte(&self, segments: &Segments) -> i32 {
        let mut curr_idx = self.first_in_use_bucket_idx.unwrap() as usize;
        let mut idx_cte = -1 as i32; 
        let mut min_time_to_expire = i32::MAX; 
        loop {
            if self.buckets[curr_idx].nseg() > segments.evict.n_merge() as i32 {
                let seg_id = self.buckets[curr_idx].head().unwrap().get(); 
                let time_to_expire = segments.headers[seg_id as usize - 1].create_at().as_secs() as i32 + self.buckets[curr_idx].ttl();
                if time_to_expire < min_time_to_expire {
                    min_time_to_expire = time_to_expire;
                    idx_cte = curr_idx as i32;
                }
            }

            if self.buckets[curr_idx].next_in_use_bucket_idx().is_none() {
                break;
            } else {
                curr_idx = self.buckets[curr_idx].next_in_use_bucket_idx().unwrap() as usize;
                // if curr_idx == MAX_N_TTL_BUCKET -1 {
                //     break; 
                // }
            }
        }

        if idx_cte == -1 {
            self.print_age_of_buckets(segments);
        }
        assert_ne!(idx_cte, -1); 
        idx_cte 
    }
}

#[inline]
pub fn age_to_bucket_idx(age: i32) -> usize {
    let mut idx = 1; 
    if age <= 0 {
        idx = MAX_N_TTL_BUCKET - 1;
    } else if age < TTL_BOUNDARY1 {
        if age != 1 {
            idx = (age / TTL_BUCKET_INTERVAL1) as usize; // leave the first bucket not used
        }
    } else if age < TTL_BOUNDARY2 {
        idx = N_TTL_BUCKET1 + ((age - TTL_BOUNDARY1) / TTL_BUCKET_INTERVAL2) as usize;
    } else if age < TTL_BOUNDARY3 {
        idx =  N_TTL_BUCKET2_ACCU + ((age - TTL_BOUNDARY2) / TTL_BUCKET_INTERVAL3) as usize;
    } else if age < TTL_BOUNDARY4 {
        idx =  N_TTL_BUCKET3_ACCU + ((age - TTL_BOUNDARY3) / TTL_BUCKET_INTERVAL4) as usize;
    } else if age < TTL_BOUNDARY5 {
        idx =  N_TTL_BUCKET4_ACCU + ((age - TTL_BOUNDARY4) / TTL_BUCKET_INTERVAL5) as usize;
    } else if age < TTL_BOUNDARY6 {
        idx =  N_TTL_BUCKET5_ACCU + ((age - TTL_BOUNDARY5) / TTL_BUCKET_INTERVAL6) as usize;
    } else {
        idx = MAX_N_TTL_BUCKET - 1;
    }
    
    // println!("age_to_bucket_idx: age {} {} {}", age, idx, bucket_idx_to_age(idx));
    idx
}

// #[inline]
// pub fn bucket_idx_to_age(idx: usize) -> i32 {
//     // 0 is mapped to 0, and not used for TTL buckets
//     if idx < N_TTL_BUCKET1 {
//         return idx as i32 * TTL_BUCKET_INTERVAL1;
//     } else if idx < N_TTL_BUCKET2_ACCU {
//         return TTL_BOUNDARY1 + (idx - N_TTL_BUCKET1) as i32 * TTL_BUCKET_INTERVAL2;
//     } else if idx < N_TTL_BUCKET3_ACCU {
//         return TTL_BOUNDARY2 + (idx - N_TTL_BUCKET2_ACCU) as i32 * TTL_BUCKET_INTERVAL3;
//     } else if idx < N_TTL_BUCKET4_ACCU {
//         return TTL_BOUNDARY3 + (idx - N_TTL_BUCKET3_ACCU) as i32 * TTL_BUCKET_INTERVAL4;
//     // } else if idx < MAX_N_TTL_BUCKET {
//     //     return TTL_BOUNDARY4 + (idx - N_TTL_BUCKET4_ACCU) as i32 * TTL_BUCKET_INTERVAL5;
//     } else if idx < N_TTL_BUCKET5_ACCU {
//         return TTL_BOUNDARY4 + (idx - N_TTL_BUCKET4_ACCU) as i32 * TTL_BUCKET_INTERVAL5;
//     } else if idx < MAX_N_TTL_BUCKET {
//         return TTL_BOUNDARY5 + (idx - N_TTL_BUCKET5_ACCU) as i32 * TTL_BUCKET_INTERVAL6;
//     } else {
//         return -1;
//     }
// }

// /// the time width between bucket idx + 1 and idx  
// #[inline]
// pub fn bucket_idx_step(idx: usize) -> usize {
//     if idx < N_TTL_BUCKET1 {
//         return TTL_BUCKET_INTERVAL1 as usize;
//     } else if idx < N_TTL_BUCKET2_ACCU {
//         return TTL_BUCKET_INTERVAL2 as usize;
//     } else if idx < N_TTL_BUCKET3_ACCU {
//         return TTL_BUCKET_INTERVAL3 as usize;
//     } else if idx < N_TTL_BUCKET4_ACCU {
//         return TTL_BUCKET_INTERVAL4 as usize;
//     } else if idx < N_TTL_BUCKET5_ACCU {
//         return TTL_BUCKET_INTERVAL5 as usize;
//     } else if idx < MAX_N_TTL_BUCKET {
//         return TTL_BUCKET_INTERVAL6 as usize;
//     // } else if idx < MAX_N_TTL_BUCKET {
//     //     return TTL_BUCKET_INTERVAL5 as usize;
//     } else {
//         panic!("invalid bucket idx: {}", idx);
//     }
// }

impl Default for TtlBuckets {
    fn default() -> Self {
        Self::new()
    }
}
