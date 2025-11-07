import itertools
import os
import json
import copy
import sys
import math
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import VALID_ALLOCATOR_REBALANCE_COMBINATIONS
from util import dict_hash
import shutil


import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import VALID_ALLOCATOR_REBALANCE_COMBINATIONS
from util import dict_hash, read_overhead_configs
import shutil

# Read trace file path from configs.json
overhead_configs = read_overhead_configs()
TRACE_FILE_PATH = overhead_configs['trace_file_path']

def read_trace_info_csv(csv_path):
    """
    Reads trace_info.csv and returns a dictionary:
    key = trace_name, value = dict of other fields with correct types
    """
    df = pd.read_csv(csv_path)
    df = df.convert_dtypes()  # Use best possible dtypes
    trace_dict = {}
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        trace_name = row_dict.pop('trace_name')
        trace_dict[trace_name] = row_dict
    return trace_dict


allocators = [
    # lru
    {
        "lru2qHotPct": 100, # placeholders for simple2q
        "lru2qColdPct": 0,
        "allocator": "SIMPLE2Q",
        "rebalanceOnRecordAccess": True,
    },
    # 2q
    {
        "lru2qHotPct": 30, 
        "lru2qColdPct": 30,
        "allocator": "LRU2Q",
        "rebalanceOnRecordAccess": True,
        "countColdTailHitsOnly": True
    },
    # tinylfu
    {
        'allocator': 'TINYLFU'
    },
    # tinylfu with tail hits tracking
    {
        'allocator': 'TINYLFUTail'
    }  
]


trace_names = ['meta_202210_kv'] 
    
working_set_ratios = [0.01, 0.1]

rebalance_intervals = [50_000]
placeholder_interval = 50_000
cache_configs = {
    "marginal-hits-old": [
        {
            "wakeUpRebalancerEveryXReqs": wakeup,
            "mhMovingAverageParam": 0.3, 
        }
        for wakeup in rebalance_intervals
    ], 
    "marginal-hits-new": [
        {
            "wakeUpRebalancerEveryXReqs": wakeup,
            "mhMovingAverageParam": 0.3,
            "mhOnlyUpdateHitIfRebalance": True,
            "minRequestsObserved": wakeup,
            "maxDecayInterval": wakeup,
            "mhMinDiff": 2, 
            "mhMinDiffRatio": 0.00,
            "emrLow": 0.5,
            "emrHigh": 0.95,
            "thresholdAI": True,
            "thresholdAD": False,
            "thresholdMI": False,
            "thresholdMD": True,
        }
        for wakeup in rebalance_intervals
    ],
    "disabled": [{"wakeUpRebalancerEveryXReqs": placeholder_interval}],
    "hits": [{"rebalanceDiffRatio": 0.1, "wakeUpRebalancerEveryXReqs": wakeup} for wakeup in rebalance_intervals],
    "tail-age": [{"rebalanceDiffRatio": 0.25, "wakeUpRebalancerEveryXReqs": wakeup} for wakeup in rebalance_intervals],
    "free-mem": [{"wakeUpRebalancerEveryXReqs": wakeup} for wakeup in rebalance_intervals],
    "eviction-rate": [{"rebalanceDiffRatio": 0.1, "wakeUpRebalancerEveryXReqs": wakeup} for wakeup in rebalance_intervals],
    "lama": [{"wakeUpRebalancerEveryXReqs": 1000_000, "lamaMinThreshold": 0.00001}],
}

# Global work directory configuration
WORK_DIR = f"../work_dir_cycles"



def generate_configs(
    trace_info_csv="../../trace_info.csv",
    base_config_path="base_config.json",
    force_delete=False
):
    total_confs = 0
    new_confs = 0
    deleted_confs = 0
    intended_uuids = set()

    trace_info_dict = read_trace_info_csv(trace_info_csv)

    for allocator_config, (trace_name, trace_info) in itertools.product(allocators, trace_info_dict.items()):
        if trace_name not in trace_names:
            continue
        download_path = trace_info["download_path"]
        file_name = trace_info["file_name"]
        slab_size = int(trace_info["slab_size"])
        wss = float(trace_info["wss"])

        with open(base_config_path, "r") as f:
            base_config = json.load(f)

        # --- Begin: Restrict combinations as requested ---
        for wsr in working_set_ratios:
            for rebalanceStrategy, rebalanceParamsList in cache_configs.items():
                if rebalanceStrategy not in VALID_ALLOCATOR_REBALANCE_COMBINATIONS.get(allocator_config["allocator"], set()):
                    continue

                for param in rebalanceParamsList:
                    cachebench_config = copy.deepcopy(base_config)
                    # cache config
                    cachebench_config["cache_config"]["rebalanceStrategy"] = rebalanceStrategy
                    raw_size = wss * wsr * 1024
                    slab_cnt = int(math.ceil(raw_size / slab_size))
                    num_slab_for_headers = (7 * slab_cnt + slab_size * 1024 - 1) // (slab_size * 1024)
                    total_slabs = slab_cnt + num_slab_for_headers
                    if slab_cnt < trace_info["num_slab_classes"]:
                        print(f"Trace {trace_name} wsr: {wsr} with slab size {slab_size} has fewer slabs ({slab_cnt}) than classes ({trace_info['num_slab_classes']}). Skipping.")
                        continue
                        
                    rounded_size = total_slabs * slab_size
                    cachebench_config["cache_config"]["cacheSizeMB"] = rounded_size
                    cachebench_config["cache_config"]["maxAllocSize"] = slab_size * 1024 * 1024
                    cachebench_config["cache_config"].update(param)
                    cachebench_config["cache_config"].update(allocator_config)

                    # test_config
                    cachebench_config["test_config"]["traceFileName"] = f"{TRACE_FILE_PATH}/{file_name}"
                    cachebench_config["test_config"]["numOps"] = int(trace_info["number_of_requests"])

                    uuid = f"{trace_name}-{dict_hash(cachebench_config)}"
                    intended_uuids.add(uuid)

                    meta_config = {
                        "trace_name": trace_name,
                        "uuid": uuid,
                        "memory_requirement": cachebench_config["cache_config"]["cacheSizeMB"] * 1.5,
                        "cpu_requirement": 2.5,  
                        "trace_file": f"{TRACE_FILE_PATH}/{file_name}",
                        "slab_size": slab_size,
                        "purpose": "efficiency",
                        "wsr": wsr,
                        "slab_cnt": slab_cnt,
                    }
                    
                    meta_config.update(trace_info)

                    subdir = os.path.join(WORK_DIR, uuid)
                    total_confs += 1
                    if os.path.exists(subdir):
                        print(f"Directory {subdir} exists, skipping.")
                        continue
                    new_confs += 1
                    os.makedirs(subdir)
                    with open(os.path.join(subdir, "config.json"), "w") as f:
                        json.dump(cachebench_config, f, indent=2)
                    with open(os.path.join(subdir, "meta.json"), "w") as f:
                        json.dump(meta_config, f, indent=2)
        # --- End: Restrict combinations as requested ---

    # Delete subdirs not in intended_uuids if force_delete is True
    if force_delete:
        for sub in os.listdir(WORK_DIR):
            sub_path = os.path.join(WORK_DIR, sub)
            if os.path.isdir(sub_path) and sub not in intended_uuids:
                print(f"Deleting obsolete config directory: {sub_path}")
                shutil.rmtree(sub_path)
                deleted_confs += 1

    print(f"Total configs: {total_confs}")
    print(f"New configs created: {new_confs}")
    print(f"Deleted obsolete configs: {deleted_confs}")


generate_configs(force_delete=True)