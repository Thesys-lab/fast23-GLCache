# prototype 


## Build

```bash
cargo build --release
```


## Usage
```bash
export OMP_NUM_THREADS=1
# -t: trace type, oracleGeneral is the only supported type
# -i: trace path
# -c: cache size in MB
# -m: cache type, l2cache or segcache
# -r: how often report stats
# -n: hashpower (the estimated number of objects in the cache is 2^n)
./target/release/bench -t oracleGeneral -i /disk/data/w96.oracleGeneral.bin -c 1000 -m l2cache -r 86400 -n 24
```


