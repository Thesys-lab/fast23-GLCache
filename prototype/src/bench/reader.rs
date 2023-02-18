// extern crate memmap;

use byteorder::{ByteOrder, LittleEndian};
use lazy_static::lazy_static;
use memmap::{Mmap, MmapOptions};
use std::collections::HashMap;
use std::fs;
use thiserror::Error;

use rustcommon_time::{refresh_with_sec_timestamp, CoarseDuration, CoarseInstant};
// use rustcommon_time::CoarseDuration;

use super::request::{Op, Request};

lazy_static! {
    static ref TRACE_RECORD_SIZE_MAP: HashMap<&'static str, u64> = HashMap::from([
        ("oracleSysTwrNS", 34),
        ("oracleSimTwrNS", 30), 
        ("oracleCF1", 49),
        ("oracleAkamai", 100000000),
        ("oracleGeneral", 24), 
        ("generalNS", 18), 
        ("twr", 20), 
    ]);
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TraceType {
    Twr,
    OracleSysTwrNS,
    OracleSimTwrNS,
    OracleCF1,
    OracleAkamai,
    OracleGeneral,
    GeneralNS, 
}

#[derive(Debug)]
pub struct Reader {
    pub mmap: Mmap,
    pub pos: usize,
    pub n_read_req: u64,
    pub n_total_req: u64,
    pub record_size: u64,
    pub trace_type: TraceType,
    pub trace_start_ts: u32, 
    pub nottl: bool, 
    pub trace_path: String, 

    pub ttl_counter: HashMap<i32, i32>,
    pub last_size: HashMap<u64, usize>, 
}

#[allow(dead_code)]
#[derive(Error, Debug, Eq, PartialEq)]
pub enum ReaderError {
    #[error("invalid trace type")]
    InvalidTraceType,
    #[error("invalid op")]
    InvalidOp,
    #[error("EOF")]
    EOF,
    #[error("request size is zero")]
    SizeZeroReq,
    #[error("request size is too large")]
    SizeTooLargeReq,
    #[error("skip request")]
    SkipReq,
}

impl Reader {
    pub fn new(trace_path_str: &str, trace_type_str: &str, nottl: bool) -> Self {
        let file = fs::File::open(trace_path_str)
            .expect(format!("Failed to open file: {}", trace_path_str).as_str());
        // let buf_reader = std::io::BufReader::with_capacity(1048576*20, file);
        let metadata = fs::metadata(trace_path_str).expect("unable to read metadata");
        let record_size = *TRACE_RECORD_SIZE_MAP
            .get(trace_type_str)
            .expect(format!("trace type is invalid {}", trace_type_str).as_str());
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        assert_eq!(mmap.len() % record_size as usize, 0, "please check trace format");
        assert_eq!(metadata.len(), mmap.len() as u64);

        let trace_type = match trace_type_str {
            "twr" => TraceType::Twr, 
            "oracleSysTwrNS" => TraceType::OracleSysTwrNS,
            "oracleSimTwrNS" => TraceType::OracleSimTwrNS,
            "oracleCF1" => TraceType::OracleCF1,
            "oracleAkamai" => TraceType::OracleAkamai,
            "oracleGeneral" => TraceType::OracleGeneral,
            "generalNS" => TraceType::GeneralNS,
            _ => panic!("trace type is invalid {}", trace_type_str),
        };

        // update time
        let ptr = mmap.as_ref();
        
        let trace_start = LittleEndian::read_u32(&ptr[0..0 + 4]);
        let _curr_time = CoarseInstant::now();
        refresh_with_sec_timestamp(1);
        
        let reader = Reader {
            mmap: mmap,
            pos: 0,
            n_read_req: 0,
            record_size: record_size,
            n_total_req: metadata.len() / record_size,
            trace_type: trace_type,
            trace_start_ts: trace_start,
            nottl: nottl,
            trace_path: trace_path_str.to_string(),
            
            ttl_counter: HashMap::new(),
            last_size: HashMap::new(),
        };

        reader
    }

    // TODO: replace this with iterator
    pub fn read(&mut self, req: &mut Request) -> Result<(), ReaderError> {
        let r = match self.trace_type {
            TraceType::Twr => read_twr(self, req),
            TraceType::OracleSysTwrNS => read_oracle_sys_twr_ns(self, req),
            TraceType::OracleSimTwrNS => read_oracle_sim_twr_ns(self, req), 
            TraceType::OracleCF1 => read_oracle_cf1(self, req),
            // "oracleAkamai" => read_oracle_akamai(self, req),
            TraceType::OracleGeneral => read_oracle_general(self, req),
            TraceType::GeneralNS => read_general_ns(self, req),
            _ => Err(ReaderError::InvalidTraceType),
        };

        if let Err(er) = r {
            return Err(er);
        }

        if self.trace_type == TraceType::OracleCF1 {
            if req.real_time < self.trace_start_ts {
                if req.real_time < 800000 && req.real_time > 700000 {
                    self.trace_start_ts = 0;
                }
            }
        }

        debug_assert!(req.real_time >= self.trace_start_ts, "current time {:?}, {} {}", CoarseInstant::now(), req.real_time, self.trace_start_ts); 
        req.real_time = req.real_time - self.trace_start_ts + 1;

        if self.nottl {
            req.ttl = CoarseDuration::new(2000000);
        }

        // println!("{}", req);

        r
    }
}

fn read_oracle_sys_twr_ns(reader: &mut Reader, req: &mut Request) -> Result<(), ReaderError> {
    if reader.n_read_req == reader.n_total_req {
        return Err(ReaderError::EOF);
    }
    let pos = reader.pos;
    let ptr = reader.mmap.as_ref();
    assert!(pos + reader.record_size as usize <= ptr.len());

    req.real_time = LittleEndian::read_u32(&ptr[pos..pos + 4]);
    let obj_id = LittleEndian::read_u64(&ptr[pos + 4..pos + 12]);
    req.keylen = std::cmp::max(LittleEndian::read_u16(&ptr[pos + 12..pos + 14]) as u8, 8);
    req.vallen = LittleEndian::read_u32(&ptr[pos + 14..pos + 18]) as usize;
    if req.vallen > 1048000 {
        reader.pos += 34;
        reader.n_read_req += 1;
        return Err(ReaderError::SkipReq);
    } else if req.vallen == 0 {
        // there are workloads which `set` size zero requests
        // println!("find size zero request");
    }

    // req.key[..req.keylen as usize]
    //     .copy_from_slice(format!("{:0keylen$}", obj_id, keylen = req.keylen as usize).as_bytes());

    // TODO just copy from mmap
    LittleEndian::write_u64(&mut req.key, obj_id);
    // req.key[..std::mem::size_of::<u64>()].write_u64::<LittleEndian>(obj_id).unwrap();  // = u64::to_le_bytes(obj_id);

    let op = LittleEndian::read_u16(&ptr[pos + 18..pos + 20]) as u8;
    match op {
        0 => {
            req.op = Op::Invalid;
            eprintln!("invalid op {}", op)
        }
        1 | 2 | 10 | 11 => req.op = Op::Get,
        3 | 6 => req.op = Op::Set, // set + replace
        4 | 5 => req.op = Op::Get, // add + cas
        9 => req.op = Op::Del,     // remove
        7 | 8 => req.op = Op::Get, // append + prepend
        // 7|8 => {req.op = Op::Invalid; return Err(ReaderError::SkipReq)}, // append + prepend
        _ => {
            eprintln!("invalid op {}", op);
            return Err(ReaderError::InvalidOp);
        }
    }

    req.ns = LittleEndian::read_u16(&ptr[pos + 20..pos + 22]) as i32;
    let mut ttl = LittleEndian::read_i32(&ptr[pos + 22..pos + 26]);
    if ttl > 20 && ttl % 10 != 0 {
        ttl = ttl - ttl % 10 + 10;
    }
    
    if ttl == 0 {
        req.ttl = CoarseDuration::new(86400);
    } else {
        req.ttl = CoarseDuration::new(ttl as u32);
    }

    // let ttl = req.ttl.as_secs() as i32;
    // *(reader.ttl_counter.entry(ttl).or_insert(0)) += 1;


    // req.op = Op::Get; 
    req.next_access_vtime = LittleEndian::read_i64(&ptr[pos + 26..pos + 34]);
    if req.next_access_vtime == -1 {
        req.next_access_vtime = i64::MAX;
    }

    reader.pos += 34;
    reader.n_read_req += 1;

    Ok(())
}

fn read_oracle_sim_twr_ns(reader: &mut Reader, req: &mut Request) -> Result<(), ReaderError> {
    if reader.n_read_req == reader.n_total_req {
        return Err(ReaderError::EOF);
    }
    let pos = reader.pos;
    let ptr = reader.mmap.as_ref();
    assert!(pos + reader.record_size as usize <= ptr.len());

    req.real_time = LittleEndian::read_u32(&ptr[pos..pos + 4]);
    let obj_id = LittleEndian::read_u64(&ptr[pos + 4..pos + 12]);
    req.keylen = 8; 
    req.vallen = LittleEndian::read_u32(&ptr[pos + 12..pos + 16]) as usize;

    // req.key[..req.keylen as usize]
    //     .copy_from_slice(format!("{:0keylen$}", obj_id, keylen = req.keylen as usize).as_bytes());

    LittleEndian::write_u64(&mut req.key, obj_id);
    // req.key[..std::mem::size_of::<u64>()].write_u64::<LittleEndian>(obj_id).unwrap();  // = u64::to_le_bytes(obj_id);

    // let ttl = LittleEndian::read_u32(&ptr[pos + 16..pos + 20]);
    req.op = Op::Get; 
    req.ns = LittleEndian::read_u16(&ptr[pos + 20..pos + 22]) as i32;
    req.ttl = CoarseDuration::new(8640000);
    req.next_access_vtime = LittleEndian::read_i64(&ptr[pos + 22..pos + 30]);
    if req.next_access_vtime == -1 {
        req.next_access_vtime = i64::MAX;
    }

    reader.pos += 30;
    reader.n_read_req += 1;

    Ok(())
}

fn read_oracle_cf1(reader: &mut Reader, req: &mut Request) -> Result<(), ReaderError> {
    if reader.n_read_req == reader.n_total_req {
        return Err(ReaderError::EOF);
    }
    let pos = reader.pos;
    let ptr = reader.mmap.as_ref();
    assert!(pos + reader.record_size as usize <= ptr.len());

    req.real_time = LittleEndian::read_u32(&ptr[pos..pos + 4]);
    let obj_id = LittleEndian::read_u64(&ptr[pos + 4..pos + 12]);
    req.keylen = 8;
    req.vallen = LittleEndian::read_u64(&ptr[pos + 12..pos + 20]) as usize;
    req.vallen /= 10; 
    if req.vallen > 1048000 * 4 {
        req.vallen = 1048000 * 4; 
        // reader.pos += 49;
        // reader.n_read_req += 1;
        // return Err(ReaderError::SkipReq);
    } 

    LittleEndian::write_u64(&mut req.key, obj_id);
    req.op = Op::Get;
    let ttl = LittleEndian::read_i32(&ptr[pos + 20..pos + 24]);
    if ttl == 0 {
        req.ttl = CoarseDuration::new(86400);
    } else {
        req.ttl = CoarseDuration::new(ttl as u32);
    }
    // req.ttl = CoarseDuration::new(2000000 as u32);

    let _age = LittleEndian::read_u32(&ptr[pos + 24..pos + 28]);
    let _hostname = LittleEndian::read_u32(&ptr[pos + 28..pos + 32]);
    req.next_access_vtime = LittleEndian::read_i64(&ptr[pos + 32..pos + 40]);
    let content_type = LittleEndian::read_u16(&ptr[pos + 40..pos + 42]);
    let _extension = LittleEndian::read_u16(&ptr[pos + 42..pos + 44]);
    let _n_level = LittleEndian::read_u16(&ptr[pos + 44..pos + 46]);
    let _n_param : u8 = ptr[pos + 46..pos + 47][0];
    let _method : u8 = ptr[pos + 47..pos + 48][0];
    let _colo : u8 = ptr[pos + 48..pos + 49][0];

    req.ns = content_type as i32;
    // req.ns = _extension as i32;
    // req.ns = (req.vallen as f64).log2().round() as i32;

    reader.pos += 49;
    reader.n_read_req += 1;

    Ok(())
}

fn read_oracle_general(reader: &mut Reader, req: &mut Request) -> Result<(), ReaderError> {
    if reader.n_read_req == reader.n_total_req {
        return Err(ReaderError::EOF);
    }
    let pos = reader.pos;
    let ptr = reader.mmap.as_ref();
    assert!(pos + reader.record_size as usize <= ptr.len());

    req.real_time = LittleEndian::read_u32(&ptr[pos..pos + 4]);
    let obj_id = LittleEndian::read_u64(&ptr[pos + 4..pos + 12]);
    req.keylen = 8;
    req.vallen = LittleEndian::read_u32(&ptr[pos + 12..pos + 16]) as usize;
    if req.vallen > 1048500 {
        req.vallen = 1048500;
    }
    req.next_access_vtime = LittleEndian::read_i64(&ptr[pos + 16..pos + 24]);
    if req.next_access_vtime == -1 {
        req.next_access_vtime = i64::MAX;
    }

    LittleEndian::write_u64(&mut req.key, obj_id);
    req.op = Op::Get;
    req.ttl = CoarseDuration::new(2000000 as u32);

    reader.pos += 24;
    reader.n_read_req += 1;

    Ok(())
}

fn read_twr(reader: &mut Reader, req: &mut Request) -> Result<(), ReaderError> {
    if reader.n_read_req == reader.n_total_req {
        return Err(ReaderError::EOF);
    }
    let pos = reader.pos;
    let ptr = reader.mmap.as_ref();
    assert!(pos + reader.record_size as usize <= ptr.len());

    req.real_time = LittleEndian::read_u32(&ptr[pos..pos + 4]);
    let obj_id = LittleEndian::read_u64(&ptr[pos + 4..pos + 12]);

    let kv_size = LittleEndian::read_u32(&ptr[pos + 12..pos + 16]) as u32;
    let op_ttl = LittleEndian::read_u32(&ptr[pos + 16..pos + 20]) as u32;

    // uint32_t op = ((op_ttl >> 24) & (0x00000100 - 1));
    
    req.keylen = ((kv_size >> 22) & (0x00000400 - 1)) as u8;
    req.vallen = (kv_size & (0x00400000 - 1)) as usize;
    // if req.vallen == 0 {
        // reader.pos += 20;
        // return read_twr(reader, req); 
    // }
    req.ttl    = CoarseDuration::new(op_ttl & (0x01000000 - 1)); 

    LittleEndian::write_u64(&mut req.key, obj_id);
    req.op = Op::Get;
    req.ttl = CoarseDuration::new(2000000 as u32);

    reader.pos += 20;
    reader.n_read_req += 1;

    Ok(())
}

fn read_general_ns(reader: &mut Reader, req: &mut Request) -> Result<(), ReaderError> {
    if reader.n_read_req == reader.n_total_req {
        return Err(ReaderError::EOF);
    }
    let pos = reader.pos;
    let ptr = reader.mmap.as_ref();
    assert!(pos + reader.record_size as usize <= ptr.len());

    req.real_time = LittleEndian::read_u32(&ptr[pos..pos + 4]);
    let obj_id = LittleEndian::read_u64(&ptr[pos + 4..pos + 12]);
    req.keylen = 8;
    req.vallen = LittleEndian::read_u32(&ptr[pos + 12..pos + 16]) as usize;
    req.vallen /= 32; 

    LittleEndian::write_u64(&mut req.key, obj_id);
    req.op = Op::Get;
    req.ttl = CoarseDuration::new(2000000 as u32);

    req.ns = LittleEndian::read_u16(&ptr[pos + 16..pos + 18]) as i32;

    reader.pos += 18;
    reader.n_read_req += 1;

    Ok(())
}
/*
 * oracle binary trace format
 *
 *  struct {
 *    uint32_t real_time;
 *    uint64_t obj_id;
 *    uint32_t obj_size;
 *    int64_t next_access_ts;
 *  };
 *
 *
 */

/* struct {
 *   uint32_t real_time;
 *   uint64_t obj_id;
 *   uint32_t obj_size;
 *   int16_t customer_id;
 *   int16_t bucket_id;
 *   int16_t content;
 *   int64_t next_access_ts;
 * }__attribute__((packed));
 *
*/

/*
 * format
 * IQQiiiQhhhbbb
 *
 * struct reqEntryReuseCF {
 *     uint32_t real_time_;
 *     uint64_t obj_id_;
 *     uint64_t obj_size_;
 *     uint32_t ttl_;
 *     uint32_t age_;
 *     uint32_t hostname_;
 *     int64_t next_access_vtime_;
 *
 *     uint16_t content_type_;
 *     uint16_t extension_;
 *     uint16_t n_level_;
 *     uint8_t n_param_;
 *     uint8_t method_;
 *     uint8_t colo_;
 * }
 */
