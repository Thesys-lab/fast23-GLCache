
mod bench;
use bench::reader::Reader;
use bench::bench::Bench;
use bench::cmd;
use bench::cache::{Cache, create_cache}; 

use rustcommon_time::{CoarseInstant};
use simple_logger::SimpleLogger;

// use std::time::Duration;
// use std::thread::sleep;

fn main() {
    SimpleLogger::new().init().unwrap();

    let args = cmd::parse_args();

    let reader = Reader::new(
        &args.trace_path,
        &args.trace_type,
        args.nottl, 
    ); 
    
    let cache: Cache = create_cache(&args); 

    let mut bench = Bench::new(
        reader, cache, args.bench_time, args.warmup_sec, args.report_interval
    );

    bench.run();
    bench.report();

    println!("{:?} {:?}", CoarseInstant::recent(), args);
}

