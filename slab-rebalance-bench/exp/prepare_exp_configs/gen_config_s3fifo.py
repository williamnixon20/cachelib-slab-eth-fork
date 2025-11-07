import itertools
import os
import json
import copy
import sys
import math
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *
from util import dict_hash
import shutil

import subprocess
import json
import glob
import math
import os
import itertools


import pandas as pd

import os, glob, json, math, copy, itertools, subprocess

dir_glob = "/home/cc/cachelib-1mb/ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/cacheDatasets/twitter/sample10"
WORK_DIR = "../work_dir_s3fifo"
# create workdir
os.makedirs(WORK_DIR, exist_ok=True)

used_allocators = ["SIMPLE2Q", "LRU2Q", "TINYLFU", "TINYLFUTail", "S3FIFO"]
used_strats = ["marginal-hits-old", "hits", "disabled"]

# Build allocator config dicts correctly
def make_allocator_dict(name: str):
    if name == "SIMPLE2Q":
        return {"allocator": "SIMPLE2Q", "lru2qHotPct": 100, "lru2qColdPct": 0}
    if name == "LRU2Q":
        return {"allocator": "LRU2Q", "lru2qHotPct": 30, "lru2qColdPct": 30}
    if name == "TINYLFU":
        return {"allocator": "TINYLFU"}
    if name == "TINYLFUTail":
        # Tail-augmented TinyLFU (the one that works with marginal-hits)
        return {"allocator": "TINYLFUTail"}
    if name == "S3FIFO":
        return {"allocator": "S3FIFO"}
    raise ValueError(f"Unknown allocator {name}")


VALID_ALLOCATOR_REBALANCE_COMBINATIONS = {
    "SIMPLE2Q": {"marginal-hits-old", "marginal-hits-new", "free-mem", "disabled", "hits", "tail-age", "lama", "eviction-rate"},
    "LRU2Q":    {"marginal-hits-old", "marginal-hits-new", "free-mem", "disabled", "hits", "tail-age", "lama", "eviction-rate"},
    "TINYLFU":  {"free-mem", "disabled", "hits", "tail-age", "eviction-rate", "lama"},
    "TINYLFUTail": {"marginal-hits-old", "marginal-hits-new"},
    "S3FIFO":   {"marginal-hits-old", "disabled", "hits", "hits-toggle"},
}

# Keep only the strategies you said you’ll use
def filter_cache_configs(cache_configs, used):
    return {k:v for k,v in cache_configs.items() if k in used}

# Call the WSS tool you have (stdout is a single JSON line)
def compute_wss(trace_file):
    ## Check if there's a cache in /output_wss_calc
    cache_path = os.path.join("output_wss_calc", os.path.basename(trace_file) + ".ws.json")
    if os.path.exists(cache_path):
        with open(cache_path, 'r') as f:
            j = json.load(f)
        return {
            "file_name": j.get("file_name") or os.path.basename(trace_file),
            "file_path": j.get("file_path") or trace_file,
            "file_size_mb": j.get("file_size_mb"),
            "wss_mb": j["unique_bytes_estimated"] / (1024 * 1024),
        }
    print(f"Computing WSS for {trace_file}")
    out = subprocess.run(["/home/cc/cachelib-1mb/slab-rebalance-bench/tools/zstd_reader", trace_file], stdout=subprocess.PIPE, text=True, check=True)
    j = json.loads(out.stdout.strip())
    # Working-set size = unique_bytes_estimated (bytes) → MB
    wss_mb = j["unique_bytes_estimated"] / (1024 * 1024)
    return {
        "file_name": j.get("file_name") or os.path.basename(trace_file),
        "file_path": j.get("file_path") or trace_file,
        "file_size_mb": j.get("file_size_mb"),
        "wss_mb": wss_mb,
    }

# Your existing knobs
working_set_ratios = [0.01]
rebalance_intervals = [50_000]
placeholder_interval = 50_000

cache_configs = {
    "marginal-hits-old": [
        {"wakeUpRebalancerEveryXReqs": w, "mhMovingAverageParam": 0.3}
        for w in rebalance_intervals
    ],
    "marginal-hits-new": [
        {
            "wakeUpRebalancerEveryXReqs": w,
            "mhMovingAverageParam": 0.3,
            "mhOnlyUpdateHitIfRebalance": True,
            "minRequestsObserved": w,
            "maxDecayInterval": w,
            "mhMinDiff": 2,
            "mhMinDiffRatio": 0.00,
            "emrLow": 0.5,
            "emrHigh": 0.95,
            "thresholdAI": True,
            "thresholdAD": False,
            "thresholdMI": False,
            "thresholdMD": True,
        }
        for w in rebalance_intervals
    ],
    "disabled": [{"wakeUpRebalancerEveryXReqs": placeholder_interval}],
    "hits": [{"rebalanceDiffRatio": 0.1, "wakeUpRebalancerEveryXReqs": w} for w in rebalance_intervals],
    "tail-age": [{"rebalanceDiffRatio": 0.25, "wakeUpRebalancerEveryXReqs": w} for w in rebalance_intervals],
    "free-mem": [{"wakeUpRebalancerEveryXReqs": w} for w in rebalance_intervals],
    "eviction-rate": [{"rebalanceDiffRatio": 0.1, "wakeUpRebalancerEveryXReqs": w} for w in rebalance_intervals],
    "lama": [{"wakeUpRebalancerEveryXReqs": 1_000_000, "lamaMinThreshold": 0.00001}],
}
cache_configs = filter_cache_configs(cache_configs, set(used_strats))

TWITTER_TO_USE=["cluster52", "cluster17", "cluster18", "cluster24", "cluster44", "cluster45", "cluster29"]

# dict_hash comes from your util; assumed imported
def generate_configs(base_config_path="base_config.json", force_delete=False):
    total_confs = 0
    new_confs = 0
    deleted_confs = 0
    intended_uuids = set()

    traces = glob.glob(os.path.join(dir_glob, "*.zst"))
    print(f"Found {len(traces)} traces")
    
    # Filter traces to only Twitter clusters you want
    traces = [t for t in traces if any(cluster in t for cluster in TWITTER_TO_USE)]
    print(f"Using {len(traces)} traces after filtering")

    # Create allocator dicts from names
    allocator_dicts = [make_allocator_dict(a) for a in used_allocators]

    for allocator_config, trace_file in itertools.product(allocator_dicts, traces):
        info = compute_wss(trace_file)
        file_name = info["file_name"]
        wss_mb = info["wss_mb"]
        tot_req = info["total_requests"] if "total_requests" in info else 100_000_000_000

        # slab size rule
        slab_size_mb = 1 if "cluster" in file_name.lower() else 4

        with open(base_config_path, "r") as f:
            base_config = json.load(f)

        # Only strategies valid for this allocator AND in used_strats
        valid_strats = VALID_ALLOCATOR_REBALANCE_COMBINATIONS[allocator_config["allocator"]]
        strats_to_use = [s for s in cache_configs.keys() if s in valid_strats]

        for wsr in working_set_ratios:
            for rebalanceStrategy in strats_to_use:
                    
                for param in cache_configs[rebalanceStrategy]:
                    print(f"Generating config for trace {file_name}, allocator {allocator_config['allocator']}, strategy {rebalanceStrategy}")
                    cachebench_config = copy.deepcopy(base_config)
                    cachebench_config["cache_config"]["rebalanceStrategy"] = rebalanceStrategy
                    
                    if "marginal" in rebalanceStrategy:
                        cachebench_config["cache_config"]["enableTailHitsTracking"] = True
                        cachebench_config["cache_config"]["countColdTailHitsOnly"] = True

                    raw_size_mb = wss_mb * wsr
                    slab_cnt = math.ceil(raw_size_mb / slab_size_mb)

                    # overhead slabs (your prior heuristic)
                    num_slab_for_headers = math.ceil((7 * slab_cnt + slab_size_mb * 1024 - 1) / (slab_size_mb * 1024))
                    total_slabs = slab_cnt + num_slab_for_headers
                    rounded_size_mb = total_slabs * slab_size_mb

                    cachebench_config["cache_config"]["cacheSizeMB"] = rounded_size_mb
                    cachebench_config["cache_config"]["maxAllocSize"] = slab_size_mb * 1024 * 1024
                    cachebench_config["cache_config"].update(param)
                    cachebench_config["cache_config"].update(allocator_config)

                    cachebench_config["test_config"]["traceFileName"] = trace_file
                    cachebench_config["test_config"]["numOps"] = tot_req

                    uuid = f"{file_name}-{dict_hash(cachebench_config)}"
                    intended_uuids.add(uuid)

                    meta_config = {
                        "trace_name": file_name,
                        "uuid": uuid,
                        "trace_file": trace_file,
                        "slab_size": slab_size_mb,
                        "purpose": "efficiency",
                        "wsr": wsr,
                        "slab_cnt": slab_cnt,
                        "wss_mb": wss_mb,
                        "allocator": allocator_config["allocator"],
                        "rebalanceStrategy": rebalanceStrategy,
   
                        "memory_requirement": cachebench_config["cache_config"]["cacheSizeMB"] * 1.5,
                        "cpu_requirement": 2.5,  
                        "purpose": "efficiency",
                    }

                    subdir = os.path.join(WORK_DIR, uuid)
                    total_confs += 1

                    if os.path.exists(subdir):
                        print(f"Directory {subdir} exists, skipping.")
                        continue

                    new_confs += 1
                    os.makedirs(subdir, exist_ok=True)
                    with open(os.path.join(subdir, "config.json"), "w") as f:
                        json.dump(cachebench_config, f, indent=2)
                    with open(os.path.join(subdir, "meta.json"), "w") as f:
                        json.dump(meta_config, f, indent=2)

    if force_delete:
        for sub in os.listdir(WORK_DIR):
            sub_path = os.path.join(WORK_DIR, sub)
            if os.path.isdir(sub_path) and sub not in intended_uuids:
                print(f"Deleting obsolete: {sub_path}")
                shutil.rmtree(sub_path)
                deleted_confs += 1

    print(f"Total configs: {total_confs}")
    print(f"New configs created: {new_confs}")
    print(f"Deleted obsolete configs: {deleted_confs}")

generate_configs(force_delete=True)