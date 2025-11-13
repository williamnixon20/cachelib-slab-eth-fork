import sys
import os
import json
import logging
from datetime import datetime
import argparse

# === Path setup ===
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *
from util import run_cachebench


def main():
    # === Parse command-line arguments ===
    parser = argparse.ArgumentParser(description="Master controller for cachebench experiments.")
    parser.add_argument(
        "--multiplier", type=int, default=0,
        help="Core group index multiplier (0-based). Example: 0 for cores 0–1, 1 for 2–3, etc."
    )
    parser.add_argument(
        "--cores-per-task", type=int, default=2,
        help="Number of cores per task. Default = 2."
    )
    parser.add_argument(
        "--max-cores", type=int, default=48,
        help="Maximum available cores (used for overflow check). Default = 48."
    )
    parser.add_argument(
        "--work-dir", type=str,
        default="/home/cc/CacheLib/slab-rebalance-bench/exp/work_dir_metakv_var_size_small",
        help="Working directory path containing experiment subdirectories."
    )
    args = parser.parse_args()

    # === CPU binding setup ===
    multiplier = args.multiplier
    cores_per_task = args.cores_per_task
    MAX_CORES = args.max_cores

    start_core = multiplier * cores_per_task
    end_core = start_core + cores_per_task - 1

    if end_core > MAX_CORES:
        print(f"Error: Ran out of cores (requested {end_core}, max {MAX_CORES}).")
        sys.exit(1)

    # Bind master process to selected cores
    os.sched_setaffinity(0, {i for i in range(start_core, end_core + 1)})
    print(f"Bound process to cores {start_core}–{end_core}")

    # === Logging setup ===
    log_date = datetime.now().strftime("%d-%m-%Y-%H:%M:%S")
    log_filename = f"master_{log_date}_{start_core}_{end_core}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        filename=log_filename,
        filemode="a",
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    logging.info(f"Started master with cores {start_core}–{end_core}")
    logging.info(f"Working directory: {args.work_dir}")

    working_on_it_file_global = "working_on_it.txt"

    # === Main loop ===
    try:
        work_dir = args.work_dir
        subdirs = [
            os.path.join(work_dir, d)
            for d in os.listdir(work_dir)
            if os.path.isdir(os.path.join(work_dir, d))
        ]
        logging.info(f"Found {len(subdirs)} subdirectories in {work_dir}")

        for subdir in subdirs:
            rc_file = os.path.join(subdir, "rc.txt")
            done_file = os.path.join(subdir, "done.txt")
            working_on_it_file = os.path.join(subdir, "working_on_it.txt")
            working_on_it_file_global = working_on_it_file

            if os.path.exists(done_file):
                logging.info(f"Skipping {subdir}: already done")
                if not os.path.exists(rc_file):
                    logging.error(f"ERROR: {subdir} has done.txt but no rc.txt")
                continue

            if os.path.exists(working_on_it_file):
                logging.info(f"Skipping {subdir}: already being worked on")
                continue

            meta_file = os.path.join(subdir, "meta.json")
            with open(meta_file, "r") as f:
                meta_content = json.load(f)
                mem_req_gb = float(meta_content.get("memory_requirement", 0)) / 1024.0
                if mem_req_gb > 100.0:
                    logging.info(f"Skipping {subdir}: memory requirement {mem_req_gb:.2f} GB exceeds 100 GB")
                    continue

            if os.path.exists(rc_file):
                with open(rc_file, "r") as f:
                    rc_content = f.read().strip()
                    if rc_content == "0":
                        logging.info(f"Skipping {subdir}: rc.txt indicates success")
                        continue
                logging.info(f"Re-running {subdir}: rc.txt indicates failure ({rc_content})")
                os.remove(rc_file)

            with open(working_on_it_file, "w") as f:
                f.write("working\n")

            logging.info(f"Running cachebench for {subdir}")
            ret = run_cachebench(subdir, repeat=1, cores=(start_core, end_core))
            if ret == 0:
                logging.info(f"cachebench succeeded for {subdir}")
                with open(done_file, "w") as f:
                    f.write("done\n")
            else:
                logging.warning(f"cachebench failed for {subdir} with return code {ret}")

            os.remove(working_on_it_file)

    except KeyboardInterrupt:
        logging.info("Master script interrupted. Cleaning up...")
        if os.path.exists(working_on_it_file_global):
            os.remove(working_on_it_file_global)
        logging.info("Exiting gracefully.")


if __name__ == "__main__":
    main()
