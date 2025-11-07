# Configuration parameters
result_dir = "cycle_results"
output_csv_path = "result_digested/result_cycles.csv"

import os
import json
import re



def extract_log_metrics(log_path):
    cpu_cycles = None
    op_latency = None
    pool_rebalancer_cycles = None
    with open(log_path, "r") as f:
        for line in f:
            if cpu_cycles is None:
                m = re.search(r"CPU cycles for serving requests:\s*(\d+)", line)
                if m:
                    cpu_cycles = int(m.group(1))
            if op_latency is None:
                m = re.search(r"Total op latency \(ns\):\s*(\d+)", line)
                if m:
                    op_latency = int(m.group(1))
            if pool_rebalancer_cycles is None:
                m = re.search(r"\[PeriodicWorker\] Thread name: PoolRebalancer, Total CPU cycles in work\(\):\s*(\d+)", line)
                if m:
                    pool_rebalancer_cycles = int(m.group(1))
    return cpu_cycles, op_latency, pool_rebalancer_cycles

def process_subdir(subdir):
    # Read config.json
    config_path = os.path.join(subdir, "config.json")
    with open(config_path) as f:
        config = json.load(f)
    cache_config = config.get("cache_config", {})

    # Read meta.json
    meta_path = os.path.join(subdir, "meta.json")
    with open(meta_path) as f:
        meta = json.load(f)

    # Read logs
    log_metrics = []
    for i in range(3):
        log_path = os.path.join(subdir, f"log_{i}.txt")
        log_metrics.append(extract_log_metrics(log_path))

    # Read tx files
    tx_metrics = []
    for i in range(3):
        # Find tx_*.json file for this index
        tx_files = [f for f in os.listdir(subdir) if f.startswith(f"tx_{i}.") and f.endswith(".json")]
        if not tx_files:
            tx_metrics.append((None, None, None))
            continue
        tx_path = os.path.join(subdir, tx_files[0])
        with open(tx_path) as f:
            tx = json.load(f)
        tx_metrics.append((tx.get("duration_ns"), tx.get("ops"), tx.get("throughput")))

    # Read result files
    result_metrics = []
    for i in range(3):
        result_path = os.path.join(subdir, f"result_{i}.json")
        if not os.path.exists(result_path):
            result_metrics.append({})
            continue
        with open(result_path) as f:
            result = json.load(f)
        result_metrics.append(result)

    # Output rows
    rows = []
    for i in range(3):
        row = {}
        row.update(cache_config)
        row.update(meta)
        row.update(result_metrics[i])  # Add all fields from result file
        row["cpu_cycles_for_serving_requests"] = log_metrics[i][0]
        row["total_op_latency_ns"] = log_metrics[i][1]
        row["pool_rebalancer_cpu_cycles"] = log_metrics[i][2]
        row["duration_ns"] = tx_metrics[i][0]
        row["ops"] = tx_metrics[i][1]
        row["throughput"] = tx_metrics[i][2]
        rows.append(row)
    return rows

def main():
    all_rows = []
    for subdir in sorted(os.listdir(result_dir)):
        subdir_path = os.path.join(result_dir, subdir)
        if not os.path.isdir(subdir_path):
            continue
        rows = process_subdir(subdir_path)
        all_rows.extend(rows)
    # Print as CSV
    if all_rows:
        import csv
        import pandas as pd
        
        # Collect all unique keys from all rows
        keys = set()
        for row in all_rows:
            keys.update(row.keys())
        keys = list(keys)
        with open(output_csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for row in all_rows:
                writer.writerow(row)
        print(f"Wrote {len(all_rows)} rows to {output_csv_path}")
        
        # Extra processing for analysis
        df = pd.DataFrame(all_rows)
        
        # Process allocator names
        df['allocator'] = df['allocator'].replace({
            'TINYLFUTail': 'TinyLFU',
            'TINYLFU': 'TinyLFU', 
            'LRU2Q': 'TwoQ',
            'SIMPLE2Q': 'LRU'
        })
        
        # Process rebalance strategy names
        df['rebalance_strategy'] = df['rebalanceStrategy'].replace({
            'marginal-hits-old': 'marginal-hits',
            'marginal-hits-new': 'marginal-hits-tuned'
        })
        
        # Calculate rebalance cycle percentage
        df['rebalance_cycle_pct'] = df['pool_rebalancer_cpu_cycles'] / df['cpu_cycles_for_serving_requests']
        
        # Output main processed dataframe
        df[['trace_name', 'wsr', 'allocator', 'rebalance_strategy', 'cpu_cycles_for_serving_requests', 'pool_rebalancer_cpu_cycles',
            'rebalance_cycle_pct', 'throughput']].to_csv('meta_2022_overhead.csv', index=False)
        
        # Output tail hits overhead analysis
        tail_hits_overhead = df[(df['rebalanceStrategy'] == 'disabled')][['allocator', 'wsr', 'enableTailHitsTracking', 'cpu_cycles_for_serving_requests']]
        tail_hits_overhead['enableTailHitsTracking'].fillna('null', inplace=True)
        tail_hits_overhead.to_csv('tail_hits_overhead.csv', index=False)
        
        print(f"Created processed analysis files: meta_2022_overhead.csv and tail_hits_overhead.csv")
    else:
        print("No data found.")

if __name__ == "__main__":
    main()