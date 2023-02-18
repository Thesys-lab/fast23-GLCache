///
/// Parse command line arguments.
///
///
use argparse::{ArgumentParser, Store, StoreTrue};

#[derive(Debug)]
pub struct Args {
    pub cache_size_in_mb: usize,
    pub segment_size_in_kb: usize, 
    pub hash_power: u8,
    pub cache_type: String,
    pub trace_path: String,
    pub trace_type: String,
    pub datapool_path: String, 
    pub nottl: bool,
    pub use_oracle: bool, 
    pub warmup_sec: i32,
    pub learn_interval: u32, 
    pub bench_time: i32,
    pub report_interval: i32,
}

pub fn parse_args() -> Box<Args> {
    let mut args = Box::new(Args {
        cache_size_in_mb: 0,
        segment_size_in_kb: 1024,
        hash_power: 24,
        cache_type: String::from("l2cache"),
        trace_path: String::new(),
        trace_type: String::from("oracleGeneral"),
        nottl: true,
        use_oracle: false, 
        warmup_sec: 86400*3, 
        report_interval: 86400,
        bench_time: 86400000,
        datapool_path: String::from(""),
        learn_interval: 86400, 
    });

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Cache benchmark");
        ap.refer(&mut args.report_interval).add_option(
            &["-r", "--report_interval"],
            Store,
            "report result every report_interval in sec",
        );
        ap.refer(&mut args.bench_time).add_option(
            &["--bench-time"],
            Store,
            "bench time",
        );
        ap.refer(&mut args.cache_size_in_mb)
            .add_option(&["-c", "--cache-size-mb"], Store, "Cache size in MB")
            .required();
        ap.refer(&mut args.segment_size_in_kb)
            .add_option(&["-s", "--segment-size"], Store, "Segment size in KB");
        ap.refer(&mut args.datapool_path)
            .add_option(&["-d", "--datapool-path"], Store, "Path to datapool");
        ap.refer(&mut args.hash_power)
            .add_option(&["-n", "--hash-power"], Store, "Hash power");
        ap.refer(&mut args.cache_type)
            .add_option(&["-m", "--cache-type"],Store, "Cache type (segcachev0/segcachev1/L2Cache)");
        ap.refer(&mut args.trace_path)
            .add_option(&["-i", "--trace-path"], Store, "trace path")
            .required();
        ap.refer(&mut args.warmup_sec)
            .add_option(&["-w", "--warmup-sec"], Store, "warmup time in sec");
        ap.refer(&mut args.nottl)
            .add_option(&["--nottl"], StoreTrue, "not to use TTL"); 
        ap.refer(&mut args.use_oracle)
            .add_option(&["--use-oracle"], StoreTrue, "use oracle"); 
        ap.refer(&mut args.trace_type)
            .add_option(&["-t", "--trace-type"], Store, "trace type");
        ap.refer(&mut args.learn_interval)
            .add_option(&["--train-interval"], Store, "the interval between training");

        ap.parse_args_or_exit();
    }
    args
}
