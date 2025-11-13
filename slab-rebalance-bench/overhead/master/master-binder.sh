#!/bin/bash
set -euo pipefail

# === Configuration ===
PYTHON_BIN="python3"
SCRIPT_PATH="/home/cc/CacheLib/slab-rebalance-bench/overhead/master/master.py"
WORK_DIR="/home/cc/CacheLib/slab-rebalance-bench/exp/work_dir_metakv_var_size_alt_strategies"

CORES_PER_TASK=3
MAX_CORES=46

# Derived number of workers = MAX_CORES / CORES_PER_TASK
NUM_WORKERS=$(( MAX_CORES / CORES_PER_TASK ))

LOG_DIR="/home/cc/CacheLib/slab-rebalance-bench/overhead/logs"
mkdir -p "$LOG_DIR"

echo "Launching $NUM_WORKERS workers (each $CORES_PER_TASK cores) from 0–$((MAX_CORES-1))..."
echo "Logs will be stored in $LOG_DIR"

# === Main loop ===
for ((i=0; i<NUM_WORKERS; i++)); do
    start_core=$(( i * CORES_PER_TASK ))
    end_core=$(( start_core + CORES_PER_TASK - 1 ))
    time=$(date +"%Y%m%d-%H%M%S")

    echo "→ Starting worker $i (cores $start_core–$end_core)"
    nohup taskset -c "$start_core"-"$end_core" \
        $PYTHON_BIN "$SCRIPT_PATH" \
        --multiplier "$i" \
        --cores-per-task "$CORES_PER_TASK" \
        --max-cores "$MAX_CORES" \
        --work-dir "$WORK_DIR" \
        > "$LOG_DIR/worker_${i}_${time}_${start_core}_${end_core}.out" 2>&1 &
    sleep 1  # small stagger to avoid I/O contention
done

echo "All workers launched. Use 'htop' or 'ps -ef | grep master.py' to monitor."

# pkill -f cachebench
