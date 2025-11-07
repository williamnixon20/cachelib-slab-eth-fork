import sys
import os 
os.sched_setaffinity(0, {1}) # numa 1
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *
from util import run_cachebench

import logging
from datetime import datetime

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
WORK_DIR = "/home/cc/CacheLib/slab-rebalance-bench/exp/work_dir_s3fifo"

def main():
    work_dir = WORK_DIR
    subdirs = [os.path.join(work_dir, d) for d in os.listdir(work_dir) if os.path.isdir(os.path.join(work_dir, d))]
    logging.info(f"Found {len(subdirs)} subdirectories in {work_dir}")

    for subdir in subdirs:
        rc_file = os.path.join(subdir, "rc.txt")
        # if no rc_file, skip
        if not os.path.exists(rc_file):
            logging.info(f"Skipping {subdir}: no rc.txt found")
            continue
        meta_file = os.path.join(subdir, "meta.json")
        # Open meta file, see mem requirements
        with open(meta_file, 'r') as f:
            meta_content = json.load(f)
            mem_req_gb = float(meta_content.get("memory_requirement", 0)) / 1024.0
            # Skip if mem_req > 1 GB
            if mem_req_gb > 1.0:
                logging.info(f"Skipping {subdir}: memory requirement {mem_req_gb:.2f} GB exceeds 1 GB")
                continue
        if "cluster" in os.path.basename(subdir):
            logging.info(f"Skipping {subdir}: contains cluster in name")
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
        
        logging.info(f"Running cachebench for {subdir}")
        ret = run_cachebench(subdir, repeat=1)
        if ret == 0:
            logging.info(f"cachebench succeeded for {subdir}")
            # touch a done file
            done_file = os.path.join(subdir, "done.txt")
            with open(done_file, "w") as f:
                f.write("done\n")
        else:
            logging.warning(f"cachebench failed for {subdir} with return code {ret}")

if __name__ == "__main__":
    main()
