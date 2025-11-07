For micro-benchmarking, we need to:  
    1. disable turbo  
    2. enable perf  
    3. pin CPU cores

(1) + (2) can be done by calling the prepare_env.sh script  
(3) now needs to happen manually

Checkout from this branch: https://github.com/eth-easl/CacheLib/tree/benchmark-tx  
BTW the slab size is 4MB in this branch (see [here](https://github.com/eth-easl/CacheLib/blob/592c0653dfc164e29b8b8742cb167328b680316a/cachelib/allocator/memory/Slab.h#L80)). If you want to benchmark for using 1MB slabs, update 22 -> 20 and rebuild.

There are 3 threads when launching cachebench 
1. the main thread: for request serving  
2. the pool rebalancer thread
3. the trace file reading thread:  
    - this thread feeds requests into a big producer-consumer queue, so that the main thread can fetch requests from it asynchronously.
    - upon start, we force the main thread to sleep for a while (see [here](https://github.com/eth-easl/CacheLib/blob/592c0653dfc164e29b8b8742cb167328b680316a/cachelib/cachebench/runner/CacheStressor.h#L191)), such that the trace file reading thread can pre-load the big queue with enough requests (see [here](https://github.com/eth-easl/CacheLib/blob/592c0653dfc164e29b8b8742cb167328b680316a/cachelib/cachebench/workload/OGBinaryReplayGenerator.h#L158))

We want to pin 1, 2, 3 to the same NUMA for better data locality. Within the same NUMA, 1+2 are pinned to one group of core, and 3 is pinned to another group, such that request loading won't interfere with actual query processing and rebalancing. To do that:
1. pin the main thread (see [here](https://github.com/eth-easl/CacheLib/blob/592c0653dfc164e29b8b8742cb167328b680316a/cachelib/cachebench/runner/CacheStressor.h#L212))
2. pin the pool rebalancer thread (see [here](https://github.com/eth-easl/CacheLib/blob/592c0653dfc164e29b8b8742cb167328b680316a/cachelib/common/PeriodicWorker.cpp#L115))
3. pin the trace file reader thread (see [here](https://github.com/eth-easl/CacheLib/blob/592c0653dfc164e29b8b8742cb167328b680316a/cachelib/cachebench/workload/OGBinaryReplayGenerator.h#L102))

Update the core ID list based on the CPU specifics in your machine. 

Once got everything above prepared, next:
1. build cachelib
2. download trace: 
   ```bash
   wget https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/cacheDatasets/metaKV/202206_kv_traces_all.csv.oracleGeneral.zst
   zstd -d 202206_kv_traces_all.csv.oracleGeneral.zst
   ``` 
3. update configs.json file, fill in 'cachelib_path' and 'trace_file_path'
4. cd gen_confs && python prepare_configs_cycle.py
5. cd ../master && nohup python master.py &




