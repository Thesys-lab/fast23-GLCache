use super::request::Request;
use crate::cmd::*;
use l2cache;
use rustcommon_time::refresh_with_sec_timestamp;
use rustcommon_time::CoarseInstant;
use segv1;

const KB: usize = 1024;
const MB: usize = 1024 * 1024;
#[allow(dead_code)]
const GB: usize = 1024 * MB;

// segcachev1 chooses bucket based on the num of segments and adds object metadata
pub enum Cache {
    SegCacheV1(segv1::Seg, usize, String),
    L2Cache(l2cache::L2Cache, usize, String),
}

impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Cache::SegCacheV1(_, size_in_mb, name) => {
                write!(f, "SegCachev1(size_MB {:?}, {:?})", size_in_mb, name)
            }
            Cache::L2Cache(_, size_in_mb, name) => {
                write!(f, "L2Cache(size_MB {:?}, {:?})", size_in_mb, name)
            }
        }
    }
}

pub fn create_cache(args: &Box<Args>) -> Cache {
    match args.cache_type.to_ascii_lowercase().as_str() {
        "segcache" => {
            let mut cb = segv1::Seg::builder()
                .segment_size((args.segment_size_in_kb * KB) as i32)
                .heap_size(args.cache_size_in_mb * MB)
                .hash_power(args.hash_power)
                .overflow_factor(1.0)
                .eviction(segv1::Policy::Merge {
                    max: 16,
                    merge: 4,
                    compact: 0,
                });

            if args.datapool_path.len() > 0 {
                cb = cb.datapool_path(Some(args.datapool_path.clone()));
            }

            let cache = cb.build();

            Cache::SegCacheV1(cache, args.cache_size_in_mb, args.cache_type.to_string())
        }
        "l2cache" => {
            let mut cb = l2cache::L2Cache::builder()
                .segment_size((args.segment_size_in_kb * KB) as i32)
                .heap_size(args.cache_size_in_mb * MB)
                .hash_power(args.hash_power)
                .overflow_factor(1.0);
            // cb = cb.eviction(l2cache::Policy::Fifo);
            // cb = cb.eviction(l2cache::Policy::Merge {
            //     max: 128,
            //     merge: 4,
            //     compact: 2,
            // });

            if args.use_oracle {
                cb = cb.eviction(l2cache::Policy::OracleMerge {
                    max_merge: 128,
                    n_merge: 5,
                    rerank_interval: 20,
                });
            } else {
                let time_before_first_train_data = 86400;
                let train_interval_sec = 86400;
                let n_seg = args.cache_size_in_mb * 1024 / args.segment_size_in_kb;
                let mut learner = l2cache::L2Learner::new(n_seg, 5);
                learner.next_train_time = time_before_first_train_data + train_interval_sec;

                cb = cb.eviction(l2cache::Policy::LearnedMerge {
                    max_merge: 128,
                    n_merge: 5,
                    train_interval_sec: train_interval_sec,
                    rerank_interval: 20, // every N_seg / 20 evictions
                    time_before_first_train_data: time_before_first_train_data,
                    learner: learner,
                });
            }

            if args.datapool_path.len() > 0 {
                cb = cb.datapool_path(Some(args.datapool_path.clone()));
            }

            let cache = cb.build();

            Cache::L2Cache(cache, args.cache_size_in_mb, args.cache_type.to_string())
        }
        _ => panic!("cache type not supported {}", args.cache_type),
    }
}

impl Cache {
    #[allow(dead_code)]
    pub fn get_size_in_mb(&self) -> usize {
        match self {
            Cache::SegCacheV1(_, size_in_mb, _) => *size_in_mb,
            Cache::L2Cache(_, size_in_mb, _) => *size_in_mb,
        }
    }

    #[allow(dead_code)]
    pub fn get_name(&self) -> &str {
        match self {
            Cache::SegCacheV1(_, _, name) => name,
            Cache::L2Cache(_, _, name) => name,
        }
    }

    pub fn get(&mut self, req: &Request, _buf: &mut [u8]) -> bool {
        // println!("{:?}", CoarseInstant::recent());
        if req.real_time < CoarseInstant::recent().as_secs() {
            println!(
                "time becomes small {} -> {}",
                req.real_time,
                CoarseInstant::recent().as_secs()
            );
        } else {
            let _time_updated = refresh_with_sec_timestamp(req.real_time);
        }

        match self {
            Cache::SegCacheV1(seg, _, _) => {
                // if time_updated {
                //     // we need this because it does not check expiration during get
                //     seg.expire();
                // }
                let res = seg.get(&req.key[..req.keylen as usize]);
                if let Some(_item) = res {
                    // buf[0..item.value().len()].copy_from_slice(item.value());
                    return true;
                } else {
                    return false;
                }
            }
            Cache::L2Cache(l2cache, _, _) => {
                // needs to update time so that age is correct
                // if time_updated {
                //     // l2cache.expire();
                // }

                let res = l2cache.get(&req.key[..req.keylen as usize], req.next_access_vtime);
                if let Some(_item) = res {
                    // buf[0..item.value().len()].copy_from_slice(item.value());
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    pub fn set(&mut self, req: &Request) {
        let mut time_updated = false;
        if req.real_time < CoarseInstant::recent().as_secs() {
            println!(
                "time becomes small {} -> {}",
                req.real_time,
                CoarseInstant::recent().as_secs()
            );
        } else {
            time_updated = refresh_with_sec_timestamp(req.real_time);
        }
        match self {
            Cache::SegCacheV1(seg, _, _) => {
                if time_updated {
                    seg.expire();
                }

                let r = seg.insert(
                    &req.key[..req.keylen as usize],
                    &req.val[..req.vallen as usize],
                    None,
                    req.ttl,
                );
                if let Err(err) = r {
                    panic!("err {}: cannot set {}", err, req);
                }
            }
            Cache::L2Cache(l2cache, _, _) => {
                if time_updated {
                    l2cache.expire();
                }

                let r = l2cache.insert(
                    &req.key[..req.keylen as usize],
                    &req.val[..req.vallen as usize],
                    None,
                    req.ttl,
                    req.next_access_vtime,
                );
                if let Err(err) = r {
                    panic!("err {}: cannot set {}", err, req);
                }
            }
        }
    }

    pub fn del(&mut self, req: &Request) -> bool {
        match self {
            Cache::SegCacheV1(seg, _, _) => seg.delete(&req.key[..req.keylen as usize]),
            Cache::L2Cache(l2cache, _, _) => l2cache.delete(&req.key[..req.keylen as usize]),
        }
    }
}
