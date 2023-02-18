use lazy_static::lazy_static;
use std::fmt; 
use byteorder::{ByteOrder, LittleEndian};
use rustcommon_time::CoarseDuration;


const MAX_KEY_LEN: usize = 256;
const MAX_VALUE_LEN: usize = 1024 * 1024 * 128;
lazy_static! {
    static ref VALUE_STR: Vec<u8> = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .repeat(MAX_VALUE_LEN / 26)
        .into_bytes();
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub real_time: u32,
    pub key: [u8; MAX_KEY_LEN],
    pub keylen: u8,
    pub vallen: usize,
    pub op: Op,
    pub ttl: CoarseDuration,
    pub ns: i32,
    pub next_access_vtime: i64,
    pub val: &'static [u8],
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Op {
    Invalid,
    Get,
    Set,
    // Add,  // add is get + set, because on-demand fill, add is the same as get
    Del,
}

impl Default for Request {
    fn default() -> Self {
        Self {
            real_time: 0,
            key: [0; MAX_KEY_LEN],
            keylen: 0,
            vallen: 0,
            op: Op::Invalid,
            ttl: CoarseDuration::ZERO,
            ns: -1,
            next_access_vtime: -2,
            val: &VALUE_STR, 
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Request(real_time={}, key={}, keylen={}, vallen={}, op={:?}, ttl={:?}, ns={}, next_access_vtime={})",
            self.real_time,
            LittleEndian::read_u64(&self.key[..std::mem::size_of::<u64>()]),
            self.keylen,
            self.vallen,
            self.op,
            self.ttl,
            self.ns,
            self.next_access_vtime,
        )
    }
}

#[derive(Debug, Default)]
#[repr(C)]
struct oracleSysTwrNSReq {
    real_time_: u32,
    obj_id_: u64,
    key_size_: u16,
    val_size_: u32,
    op_: u16,
    ns: u16,
    ttl_: i32,
    next_access_vtime_: i64,
}
