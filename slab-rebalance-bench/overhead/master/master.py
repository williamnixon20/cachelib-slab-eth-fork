import sys
import os 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *
from util import run_cachebench

import logging
from datetime import datetime

import os

multiplier = 0
cores_per_task = 48
MAX_CORES = 48

start_core = 0 + multiplier * cores_per_task
end_core = start_core + cores_per_task - 1
if end_core > MAX_CORES:
    print("Ran out of cores.")
    exit(1)
    
WORK_DIR = "/home/cc/CacheLib/slab-rebalance-bench/exp/work_dir_metakv_var_size_small"
# WORK_DIR = "/home/cc/CacheLib/slab-rebalance-bench/exp/work_dir_metakv_var_size_small"
# Pin master process to cores 0–3
os.sched_setaffinity(0, {i for i in range(start_core, end_core + 1)})


log_date = datetime.now().strftime("%d-%m-%Y-%H:%M:%S")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    filename=f'master_{log_date}.log',  # Log file name
    filemode='a'            # Append mod
)
# still log to console too
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# Configuration parameters
working_on_it_file_global = "working_on_it.txt"
def main():
    try:
        work_dir = WORK_DIR
        subdirs = [os.path.join(work_dir, d) for d in os.listdir(work_dir) if os.path.isdir(os.path.join(work_dir, d))]
        logging.info(f"Found {len(subdirs)} subdirectories in {work_dir}")

        for subdir in subdirs:
            rc_file = os.path.join(subdir, "rc.txt")
            done_file = os.path.join(subdir, "done.txt")
            working_on_it_file = os.path.join(subdir, "working_on_it.txt")
            working_on_it_file_global = working_on_it_file
            
            # if done_file exists, skip
            if os.path.exists(done_file):
                logging.info(f"Skipping {subdir}: already done")
                # See if rc.txt exists, if not then throw error
                if not os.path.exists(rc_file):
                    logging.error(f"ERROR: {subdir} has done.txt but no rc.txt")
                continue
            
            # if working_on_it_file exists, skip
            if os.path.exists(working_on_it_file):
                logging.info(f"Skipping {subdir}: already being worked on")
                continue
                

            meta_file = os.path.join(subdir, "meta.json")
            # Open meta file, see mem requirements
            with open(meta_file, 'r') as f:
                meta_content = json.load(f)
                mem_req_gb = float(meta_content.get("memory_requirement", 0)) / 1024.0
                # Skip if mem_req > 1 GB
                if mem_req_gb > 100.0:
                    logging.info(f"Skipping {subdir}: memory requirement {mem_req_gb:.2f} GB exceeds 100 GB")
                    continue
                
            if os.path.exists(rc_file):
                ## See output of rc.txt to determine success or failure
                with open(rc_file, "r") as f:
                    rc_content = f.read().strip()
                    if rc_content == "0":
                        logging.info(f"Skipping {subdir}: rc.txt indicates success")
                        continue
            
                logging.info(f"Re-running {subdir}: rc.txt indicates failure with code {rc_content}")
                os.remove(rc_file)
                
            # Create a working on it file, touch
            with open(working_on_it_file, "w") as f:
                f.write("working\n")
            
            logging.info(f"Running cachebench for {subdir}")
            ret = run_cachebench(subdir, repeat=1, cores=(start_core, end_core))
            if ret == 0:
                logging.info(f"cachebench succeeded for {subdir}")
                # touch a done file
                done_file = os.path.join(subdir, "done.txt")
                with open(done_file, "w") as f:
                    f.write("done\n")
            else:
                logging.warning(f"cachebench failed for {subdir} with return code {ret}")
            # Remove working on it file
            os.remove(working_on_it_file)
    # Except keyboard interrupt
    except KeyboardInterrupt:
        logging.info("Master script interrupted by user, removing working_on_it.txt.")
        if os.path.exists(working_on_it_file_global):
            os.remove(working_on_it_file_global)
        logging.info("Exiting.")

if __name__ == "__main__":
    main()

# python3 /home/cc/CacheLib/slab-rebalance-bench/overhead/master/master.py
