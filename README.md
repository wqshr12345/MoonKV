## MoonKV: Optimizing Update-intensive Workloads for NVM-based Key-value Stores

**1 Introduction**

This is the code implemented according to the paper MoonKV: Optimizing Update-intensive Workloads for NVM-based Key-value Stores.


**2 Pre Install**

**2.1 NVM**: MoonKV acesses NVM via PMDK. To run MoonKV, please install PMDK first.

**2.2 Folly**: Moonkv implements EstimateSeek through folly's ConcurrentHashMap.To run Moonkv, you need to install folly first.

```
sudo apt-get install openssl libssl-dev autoconf
make checkout_folly
```

**2 Compilation**

We only support cmake instead of Makefile currently.

```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_SNAPPY=1 -DUSE_FOLLY=1 -DUSE_CSL=1 -USE_RTTI .. && make -j16

```

**3.Run**

**3.1 change options**

Our implementation is based on RocksDB. We implemented all of the innovations mentioned in the paper, and these can be easily turned on and off or controlled by options. Below are some representative options.

```
bool enable_vertical_compaction = true; // Whether to enable vertical compaction
int vertical_start_level = 2; // The starting level of vertical compaction
bool enable_estimate_seek = true; // Whether to enable estimate seek
double gc_garbage_threshold = 0.30; // the ratio threshold of valid keys when selecting gc table
......
```

You can find more detail option introductions in include/options.h.

For convenience, we have implemented the option to be obtained from the configuration file, you only need to modify the corresponding value in **ycsb/rocksdb/moonkv.properties** to realize easy control of the database.

**3.2 run moonkv**

```
cd ycsb
./build_moonkv.sh
./shell/run/run_moonkv.sh
```

**3.3 run other dbs(matrixkv rocksdb rocksdb_blob)**

In fact, we also allow you to run tests on rocksdb, matrixkv. This requires you to have a compiled matrixkv (or rocksdb) link library and header files. 

Take rocksdb as an example, after you have compiled your local rocksdb code, you need to modify the code here in /ycsb/MakeFile

```
ifeq ($(ROCKSDB), 1)
#note: next line should use RocksDB's "include folder" path, xxx is an example
	CXXFLAGS += -I/xxx/include 
#note: next line should use RocksDB's link library path, xxx is an example
	LDFLAGS += -L/xxx 
	BIND_ROCKSDB = 1
endif
```

And then modify the code here in /ycsb/shell/run/run_rocksdb.sh

```
#note: next line should use RocksDB's link library path, xxx is an example
export LD_LIBRARY_PATH=/xxx
```


And then

```
./build_rocbksdb.sh
./shell/run/run_rocksdb.sh // run rocksdb
./shell/run/run_rocksdb_blob.sh // run rocksdb-blobdb
```