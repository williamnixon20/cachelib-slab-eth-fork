import os

import json
import subprocess
import time
import requests
from collections import defaultdict
import logging
from logging.handlers import RotatingFileHandler
import random
from datetime import datetime, timedelta
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *

def _read_master_configs():
    """Read configuration from configs.json file for master.py."""
    try:
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to find configs.json
        config_file = os.path.join(script_dir, "..", "configs.json")
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Check that required fields exist
        required_fields = ['work_dirs', 'local_trace_file_dir', 'python_path']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field '{field}' in {config_file}")
        
        # Get optional field with default
        need_download_traces = config.get('need_download_traces', True)
        
        return config['work_dirs'], config['local_trace_file_dir'], config['python_path'], need_download_traces
    except (FileNotFoundError, IOError) as e:
        raise RuntimeError(f"Could not read configs.json ({e}). Please ensure {config_file} exists and is readable.")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in configs.json ({e}). Please check the file format.")

def generate_node_resources(hosts):
    """
    Dynamically generate NODE_RESOURCES by querying each host for CPU and memory info.
    First host is treated as master with reduced capacity.
    Helps us decide how many slots on each host
    """
    node_resources = {}
    
    for i, host in enumerate(hosts):
        is_master = (i == 0)  # First host is master
        
        try:
            # Get CPU count using lscpu
            cpu_cmd = "lscpu | grep '^CPU(s):' | awk '{print $2}'"
            cpu_result = subprocess.run(
                ["ssh", host, cpu_cmd],
                capture_output=True, text=True, check=True, timeout=30
            )
            total_cpus = int(cpu_result.stdout.strip())
            
            # Get total memory in MB
            mem_cmd = "free -m | awk '/^Mem:/ {print $2}'"
            mem_result = subprocess.run(
                ["ssh", host, mem_cmd],
                capture_output=True, text=True, check=True, timeout=30
            )
            total_mem_mb = int(mem_result.stdout.strip())
            
            # Calculate usable resources based on role
            if is_master:
                # Master: 50% CPU, 40% memory, make sure the master node is not overloaded
                usable_cpus = int(total_cpus * 0.5)
                usable_mem_mb = int(total_mem_mb * 0.4)
                role = "master"
            else:
                # Worker: 100% CPU, 90% memory
                usable_cpus = total_cpus
                usable_mem_mb = int(total_mem_mb * 0.9)
                role = "worker"
            
            node_resources[host] = {"cpu": usable_cpus, "mem": usable_mem_mb}
            
            logging.info(f"Host {host} ({role}): {total_cpus} CPUs -> {usable_cpus} usable, "
                        f"{total_mem_mb}MB -> {usable_mem_mb}MB usable")
            
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError) as e:
            # Fallback to conservative defaults if SSH fails
            default_cpu = 4 if is_master else 8
            default_mem = 8192 if is_master else 16384
            node_resources[host] = {"cpu": default_cpu, "mem": default_mem}
            
            logging.warning(f"Failed to query {host} ({e}). Using defaults: "
                          f"{default_cpu} CPUs, {default_mem}MB memory")
    
    # Log NODE_RESOURCES summary instead of full dictionary to avoid long lines
    total_hosts = len(node_resources)
    total_cpu = sum(res["cpu"] for res in node_resources.values())
    total_mem = sum(res["mem"] for res in node_resources.values())
    logging.info(f"Generated NODE_RESOURCES for {total_hosts} hosts: {total_cpu} total CPUs, {total_mem}MB total memory")
    return node_resources

# Read configuration from configs.json
WORK_DIRS, TRACE_DIR, PYTHON_EXEC, NEED_DOWNLOAD_TRACES = _read_master_configs()

SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Parent directory of master/
HOSTS_FILE = os.path.join(os.path.dirname(SCRIPTS_DIR), "hosts", "hosts.txt")  # hosts/hosts.txt

# Create timestamped directory for logs and state files
TIMESTAMP_DIR = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), TIMESTAMP_DIR)
os.makedirs(LOG_DIR, exist_ok=True)

STATE_FILE = os.path.join(LOG_DIR, "scheduler_state.json")      # File for dumping the central state
LOG_FILE = os.path.join(LOG_DIR, "master.log")                 # Log file for the scheduler

# =====================

# --- HELPER FUNCTIONS ---

def safe_log_string(text, max_length=200):
    """
    Safely truncate long strings for logging to prevent text editor rendering issues.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length] + f"... (truncated, total length: {len(text)})"

def get_remote_file_size(url):
    """Gets the size of a remote file in bytes using an HTTP HEAD request."""
    try:
        r = requests.head(url, allow_redirects=True, timeout=10)
        r.raise_for_status()  # Raise an exception for bad status codes
        size = r.headers.get('Content-Length')
        return int(size) if size else None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get size for {url}: {e}")
        return None

def get_nfs_free_bytes(path):
    """Gets the free space in bytes for the filesystem that a path resides on."""
    try:
        stat = os.statvfs(path)
        return stat.f_bavail * stat.f_frsize * 0.9
    except FileNotFoundError:
        logging.error(f"Path not found for statvfs: {path}. Returning 0 free space.")
        return 0

def scan_experiments(work_dir):
    """Scans the work directory to find all experiment subdirectories and their metadata."""
    exps = []
    if not os.path.isdir(work_dir):
        logging.critical(f"WORK_DIR '{work_dir}' does not exist. Exiting.")
        exit(1)
    for subdir in os.listdir(work_dir):
        exp_dir = os.path.join(work_dir, subdir)
        if not os.path.isdir(exp_dir):
            continue
        meta_path = os.path.join(exp_dir, "meta.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        exps.append({"dir": exp_dir, "meta": meta})
    return exps

def group_by_trace(exps):
    """Groups experiments by their required trace file."""
    trace_to_exps = defaultdict(list)
    for exp in exps:
        trace_file = exp["meta"]["trace_file"]
        trace_to_exps[trace_file].append(exp)
    return trace_to_exps

def all_exps_done(exps):
    """Checks if all experiments in a list are finished (have an rc.txt file)."""
    for exp in exps:
        if not os.path.exists(os.path.join(exp["dir"], "rc.txt")):
            return False
    return True

def download_trace(meta):
    """
    Downloads a trace file if it doesn't exist and there is enough space.
    Uses 'sudo' for the download, assuming passwordless sudo is configured.
    """
    url = meta["download_path"]
    url = f"{WGET_PATH}/{url}"
    local_path = meta["trace_file"]
    trace_dir = os.path.dirname(local_path)

    # Ensure the trace directory exists using sudo
    if not os.path.exists(trace_dir):
        subprocess.run(["sudo", "mkdir", "-p", trace_dir], check=True)
        subprocess.run(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", trace_dir], check=True)

    # If the file already exists, we are done.
    if os.path.exists(local_path):
        logging.info(f"Trace {os.path.basename(local_path)} already exists.")
        return True

    # Get the file size for space estimation
    file_size = get_remote_file_size(url)
    if file_size is None:
        logging.warning(f"Could not determine file size of {url}. Cannot download.")
        return False
        
    # Check if we have enough space for the file
    free_space = get_nfs_free_bytes(trace_dir)

    if free_space < file_size:
        logging.warning(f"Not enough space for {local_path} ({file_size/1e9:.2f}GB needed, {free_space/1e9:.2f}GB free)")
        return False

    logging.info(f"Downloading {url} to {local_path} with sudo...")
    
    # Download file directly without decompression
    download_cmd = f"wget -q '{url}' -O '{local_path}'"

    # Use sudo to run the download command
    res = subprocess.run(["sudo", "bash", "-c", download_cmd], capture_output=True)

    if res.returncode != 0:
        stderr_output = safe_log_string(res.stderr.decode(), 300)
        logging.error(f"Failed to download {url}. wget stderr: {stderr_output}")
        # Clean up potentially incomplete file
        if os.path.exists(local_path):
             subprocess.run(["sudo", "rm", "-f", local_path])
        return False
    subprocess.run(["sudo", "chown", f"{os.getuid()}:{os.getgid()}", local_path])
    subprocess.run(["sudo", "chmod", "644", local_path])

    logging.info(f"Successfully downloaded {os.path.basename(local_path)}.")
    
    
    return True


def delete_trace(trace_file):
    """
    Deletes a trace file from the filesystem using sudo.
    """
    #pass
    if os.path.exists(trace_file):
        logging.info(f"Deleting trace file {trace_file} with sudo...")
        subprocess.run(["sudo", "rm", "-f", trace_file])


def get_hosts(hosts_file):
    """Reads a list of hosts from a file."""
    try:
        with open(hosts_file) as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except FileNotFoundError:
        logging.critical(f"Hosts file '{hosts_file}' not found. Exiting.")
        exit(1)

def log_status_summary(exps, running_jobs):
    """Logs a summary of the status of all experiments."""
    status_count = defaultdict(int)
    for exp in exps:
        status = get_exp_status(exp)
        status_count[status] += 1
    logging.info(f"STATUS: {status_count['todo']} ToDo, {len(running_jobs)} Running, "
                 f"{status_count['finished']} Finished, {status_count['failed']} Failed.")


def get_host_mem_free_percent(host):
    """
    Returns the percentage of free memory on the remote host.
    Returns None if unable to retrieve.
    """
    mem_util_cmd = "free -m | awk '/^Mem:/ {printf \"%.2f\", $7/$2 * 100.0}'"
    try:
        mem_result = subprocess.run(
            ["ssh", host, mem_util_cmd],
            capture_output=True, text=True, check=True, timeout=10
        )
        # Sanitize output by removing null bytes and non-printable characters
        mem_output = mem_result.stdout.replace('\x00', '').strip()
        return float(mem_output) if mem_output else None
    except Exception:
        return None

def log_node_system_stats(hosts):
    """Logs the current CPU and Memory utilization for each host."""
    logging.info("--- System-wide Resource Utilization ---")
    for host in hosts:
        try:
            # Get CPU Utilization using vmstat (more reliable than top)
            cpu_util_cmd = "vmstat 1 2 | tail -1 | awk '{print 100.0 - $15}'"
            cpu_result = subprocess.run(
                ["ssh", host, cpu_util_cmd],
                capture_output=True, text=True, check=True, timeout=15
            )
            # Sanitize output by removing null bytes and non-printable characters
            cpu_output = cpu_result.stdout.replace('\x00', '').strip()
            cpu_util = float(cpu_output) if cpu_output else 0.0

            # Get Free Memory Percentage ( (available / total) * 100 )
            mem_util_cmd = "free -m | awk '/^Mem:/ {printf \"%.2f\", $7/$2 * 100.0}'"
            mem_result = subprocess.run(
                ["ssh", host, mem_util_cmd],
                capture_output=True, text=True, check=True, timeout=10
            )
            # Sanitize output by removing null bytes and non-printable characters
            mem_output = mem_result.stdout.replace('\x00', '').strip()
            mem_free_percent = float(mem_output) if mem_output else 0.0

            logging.info(f"  - Host: {host:<30} CPU Util: {cpu_util:5.1f}% | Mem Free: {mem_free_percent:5.1f}%")

        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError) as e:
            logging.warning(f"  - Host: {host:<30} FAILED to retrieve stats. Reason: {e}")
    logging.info("----------------------------------------")

def log_running_job_stats(running_jobs):
    """Logs the runtime of each currently running job."""
    if not running_jobs:
        return
    logging.info("--- Running Job Runtimes ---")
    now = time.time()
    for exp_dir, job_info in sorted(running_jobs.items(), key=lambda item: item[1]['start_time']):
        run_time_seconds = now - job_info['start_time']
        # Format seconds into a more readable HH:MM:SS format
        run_time_formatted = str(timedelta(seconds=int(run_time_seconds)))
        host = job_info['host']
        uuid = os.path.basename(exp_dir)
        logging.info(f"  - Job: {uuid} on {host:<25} has been running for {run_time_formatted}")
    logging.info("----------------------------")


# --- STATE MANAGEMENT FUNCTIONS (ENHANCED) ---

def is_process_actually_running(hostname, uuid):
    """
    Connects to a remote host and checks if a process with a specific
    tag (UUID) is currently running.
    
    Returns:
        True: Process is running OR unable to verify (SSH failures, errors)  
        False: Process is confirmed not running (SSH successful, no matching processes)
    """
    process_tag = f"CACHEBENCH_UUID={uuid}"
    # Use ps to avoid the pgrep self-matching issue
    check_cmd = f"ps aux | grep '{process_tag}' | grep -v grep | wc -l"
    try:
        result = subprocess.run(
            ["ssh", hostname, check_cmd], 
            check=False,  # Don't raise exception on non-zero return code
            capture_output=True, 
            timeout=30,
            text=True
        )
        
        if result.returncode == 0:
            # Command succeeded, check the count
            count = int(result.stdout.strip())
            if count > 0:
                logging.debug(f"Process {uuid} is running on {hostname} ({count} processes found)")
                return True
            else:
                logging.debug(f"Process {uuid} not found on {hostname} (SSH successful)")
                return False
        else:
            # Command failed - treat as still running to be safe
            stderr_output = safe_log_string(result.stderr.strip(), 200)
            logging.warning(f"Process check failed (code {result.returncode}) when checking {uuid} on {hostname}: {stderr_output}. Assuming still running.")
            return True
            
    except subprocess.TimeoutExpired:
        # SSH connection timeout - treat as still running to avoid premature cleanup
        logging.warning(f"SSH timeout when checking process on {hostname} for task {uuid}. Assuming still running.")
        return True
    except Exception as e:
        # Other SSH-related errors - treat as still running to be safe
        logging.warning(f"SSH error when checking process on {hostname} for task {uuid}: {e}. Assuming still running.")
        return True

def get_running_info(exp):
    """
    Checks if an experiment is running. If so, returns the hostname it's running on.
    Otherwise, returns None.
    """
    lock_file = os.path.join(exp["dir"], "running.lock")
    if os.path.exists(lock_file):
        with open(lock_file, 'r') as f:
            hostname = f.read().strip()
        return hostname if hostname else None
    return None

def mark_exp_running(exp, hostname):
    """Marks an experiment as running by writing the hostname to the lock file."""
    lock_file = os.path.join(exp["dir"], "running.lock")
    with open(lock_file, 'w') as f:
        f.write(hostname)

def unmark_exp_running(exp):
    """Unmarks an experiment by deleting the lock file. Called when job finishes."""
    lock_file = os.path.join(exp["dir"], "running.lock")
    if os.path.exists(lock_file):
        os.remove(lock_file)

def get_exp_status(exp, grace_period=300):
    """
    Determines the status of an experiment with process-level verification and a grace period.
    States: finished, failed, running, todo.
    """
    rc_file = os.path.join(exp["dir"], "rc.txt")
    lock_file = os.path.join(exp["dir"], "running.lock")
    grace_file = lock_file + ".grace"

    # --- Case 1: The job has a definitive result file. ---
    if os.path.exists(rc_file):
        with open(rc_file) as f:
            rc = f.read().strip()
        
        # Clean up any leftover state files
        if os.path.exists(lock_file):
            unmark_exp_running(exp)
        if os.path.exists(grace_file):
            os.remove(grace_file)
            
        return "finished" if rc == "0" else "failed"

    # --- Case 2: The job has a lock file, meaning it was running. ---
    hostname = get_running_info(exp)
    if hostname:
        uuid = os.path.basename(exp["dir"])
        # Check if the process is still alive
        process_status = is_process_actually_running(hostname, uuid)
        
        if process_status is True:
            # Process is running (or we can't verify due to SSH issues - treat as running)
            return "running"
        elif process_status is False:
            # SSH worked, but process is confirmed gone - start grace period
            now = time.time()
            if not os.path.exists(grace_file):
                # Start the grace period
                with open(grace_file, "w") as f:
                    f.write(str(now))
                logging.info(f"Process for {uuid} is gone. Starting {grace_period}s grace period.")
                return "running" # Pretend it's running during the grace period
            else:
                # Check if grace period has expired
                with open(grace_file) as f:
                    start_time = float(f.read().strip())
                
                if now - start_time < grace_period:
                    return "running" # Still within grace period
                else:
                    # Grace period expired. Before marking as stale, check if job wrote rc.txt during grace period
                    if os.path.exists(rc_file):
                        with open(rc_file) as f:
                            rc = f.read().strip()
                        # Job actually completed during grace period - clean up and return actual status
                        unmark_exp_running(exp)
                        os.remove(grace_file)
                        logging.info(f"Job {uuid} completed during grace period with exit code {rc}.")
                        return "finished" if rc == "0" else "failed"
                    else:
                        # Grace period expired and no rc.txt - this is now a confirmed stale job.
                        logging.warning(f"Stale job {uuid} on {hostname} failed after grace period.")
                        with open(rc_file, 'w') as f:
                            f.write("-99") # Mark as failed
                        
                        # Clean up all state files
                        unmark_exp_running(exp)
                        os.remove(grace_file)
                        
                        return "failed"
            
    # --- Case 3: No lock file and no rc.txt. The job is waiting to be scheduled. ---
    return "todo"

def dump_state_to_file(all_exps, running_jobs, filename):
    """
    Gathers the state of all experiments and dumps it to a JSON file.
    """
    logging.info(f"Dumping current state to {filename}...")
    state_data = []
    now = time.time()

    for exp in all_exps:
        exp_dir = exp['dir']
        uuid = os.path.basename(exp_dir)
        status = get_exp_status(exp)
        
        host = None
        start_time_unix = None
        start_time_str = None
        duration_str = None

        if exp_dir in running_jobs:
            job_info = running_jobs[exp_dir]
            host = job_info['host']
            start_time_unix = job_info['start_time']
            start_time_str = datetime.fromtimestamp(start_time_unix).isoformat()
            duration_seconds = now - start_time_unix
            duration_str = str(timedelta(seconds=int(duration_seconds)))

        state_data.append({
            "uuid": uuid,
            "status": status,
            "host": host,
            "start_time_unix": start_time_unix,
            "start_time_iso": start_time_str,
            "duration": duration_str
        })
    
    try:
        with open(filename, 'w') as f:
            json.dump(state_data, f, indent=4)
    except IOError as e:
        logging.error(f"Failed to dump state to {filename}: {e}")


def trace_file_status_count(exps, status):
    from collections import defaultdict
    count = defaultdict(int)
    for exp in exps:
        if get_exp_status(exp) == status:
            trace_file = exp["meta"]["trace_file"]
            count[trace_file] += 1
    return count

def schedule_experiments_reconstructable():
    """Main scheduler function with state reconstruction - processes all work directories together by flattening experiments."""
    log_formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)  # 10MB per file, keep 5 backups
    handler.setFormatter(log_formatter)
    logging.getLogger().handlers = []  # Remove any existing handlers
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    logging.info("--- Scheduler Starting ---")
    logging.info(f"Will process {len(WORK_DIRS)} work directories")
    # Log work directories individually to avoid extremely long lines
    for i, work_dir in enumerate(WORK_DIRS, 1):
        logging.info(f"  Work directory {i}: {work_dir}")
    
    hosts = get_hosts(HOSTS_FILE)
    logging.info("Found hosts: " + ", ".join(hosts))
    
    # Log trace download configuration
    logging.info(f"Trace download/deletion enabled: {NEED_DOWNLOAD_TRACES}")
    if not NEED_DOWNLOAD_TRACES:
        logging.info("Assuming all trace files exist. No downloading or deletion will occur.")
    
    # Generate node resources dynamically
    NODE_RESOURCES = generate_node_resources(hosts)
    
    # Flatten all experiments from all work directories
    logging.info("=== Scanning and flattening experiments from all work directories ===")
    all_exps = []
    work_dir_stats = {}
    
    for work_dir in WORK_DIRS:
        if not os.path.exists(work_dir):
            logging.warning(f"Work directory {work_dir} does not exist. Skipping.")
            work_dir_stats[work_dir] = 0
            continue
        
        work_dir_exps = scan_experiments(work_dir)
        work_dir_stats[work_dir] = len(work_dir_exps)
        all_exps.extend(work_dir_exps)
        logging.info(f"Found {len(work_dir_exps)} experiments in {work_dir}")
    
    if not all_exps:
        logging.info("No experiments found in any work directory. Exiting.")
        return
    
    logging.info(f"Total experiments across all work directories: {len(all_exps)}")
    for work_dir, count in work_dir_stats.items():
        logging.info(f"  - {work_dir}: {count} experiments")
        
    trace_to_exps = group_by_trace(all_exps)
    logging.info(f"Experiments grouped into {len(trace_to_exps)} trace files")
    
    master_start_time = time.time() # For reconstructed jobs

    # --- STATE TRACKING ---
    node_usage = {host: {"cpu": 0, "mem": 0} for host in hosts}
    running_jobs = {}  # {exp_dir: {"host": host, "start_time": timestamp}}

    # --- STATE RECONSTRUCTION ON STARTUP ---
    logging.info("Reconstructing state from filesystem for all experiments...")
    for exp in all_exps:
        # Use the robust get_exp_status during reconstruction
        status = get_exp_status(exp)
        if status == "running":
            logging.info(f"Reconstructing running job: {exp['dir']}")
            exp_dir = exp["dir"]
            running_host = get_running_info(exp) # We know this is valid now
            meta = exp["meta"]
            cpu_req = meta["cpu_requirement"]
            mem_req = meta["memory_requirement"] 
            node_usage[running_host]["cpu"] += cpu_req
            node_usage[running_host]["mem"] += mem_req
            running_jobs[exp_dir] = {"host": running_host, "start_time": master_start_time}
            logging.info(f"Reconstructed state for running job {os.path.basename(exp_dir)} on {running_host}")

    logging.info("--- State Reconstruction Complete for all experiments. Starting Main Loop. ---")
    
    last_system_log_time = 0

    # --- MAIN SCHEDULING LOOP FOR ALL FLATTENED EXPERIMENTS ---
    while True:
        # 1. UPDATE STATE: Check for finished jobs and free up resources
        finished_jobs = []
        for exp_dir, job_info in list(running_jobs.items()):
            host = job_info['host']
            exp_obj = next((exp for exp in all_exps if exp["dir"] == exp_dir), None)
            if exp_obj is None: continue
            
            status = get_exp_status(exp_obj)
            if status != "running": # Job has finished or failed (including stale)
                meta = exp_obj["meta"]
                cpu_req = meta["cpu_requirement"]
                mem_req = meta["memory_requirement"]
                node_usage[host]["cpu"] = max(0, node_usage[host]["cpu"] - cpu_req)
                node_usage[host]["mem"] = max(0, node_usage[host]["mem"] - mem_req)
                
                end_time = time.time()
                run_time_seconds = end_time - job_info['start_time']
                run_time_formatted = str(timedelta(seconds=int(run_time_seconds)))
                
                logging.info(f"Job {os.path.basename(exp_dir)} ended on {host} with status '{status}' after {run_time_formatted}. Freed resources.")
                finished_jobs.append(exp_dir)
        
        for job_dir in finished_jobs:
            if job_dir in running_jobs:
                del running_jobs[job_dir]

        # 2. MANAGE TRACES: Check if any traces can be deleted (only if downloading is enabled)
        if NEED_DOWNLOAD_TRACES:
            for trace_file, exps_for_trace in trace_to_exps.items():
                if os.path.exists(trace_file) and all_exps_done(exps_for_trace):
                    is_still_running = any(exp['dir'] in running_jobs for exp in exps_for_trace)
                    if not is_still_running:
                        delete_trace(trace_file)

        # 3. SCHEDULE NEW JOBS (MODIFIED LOGIC)
        progress_made = False
        pending_exps = [exp for exp in all_exps if get_exp_status(exp) == "todo"]

        trace_file_to_pending_count = trace_file_status_count(all_exps, "todo")
        trace_file_to_finished_count = trace_file_status_count(all_exps, "finished")

        random.shuffle(pending_exps)
        pending_exps.sort(
            key=lambda exp: (
                -trace_file_to_finished_count[exp["meta"]["trace_file"]],
                trace_file_to_pending_count[exp["meta"]["trace_file"]]
            )
        )
        #random.shuffle(pending_exps)

        # --- Phase 1: Schedule all possible jobs (with trace checks if downloading enabled) ---
        for exp in pending_exps:
            trace_file = exp["meta"]["trace_file"]
            # Skip trace existence check if downloading is disabled (assume all traces exist)
            if NEED_DOWNLOAD_TRACES and not os.path.exists(trace_file):
                continue # Skip if trace doesn't exist and downloading is enabled
            
            exp_dir = exp["dir"]
            meta = exp["meta"]
            cpu_req = meta["cpu_requirement"]
            mem_req = meta["memory_requirement"]
            
            eligible_hosts = []
            # First, filter hosts by static resource availability (CPU and memory requirements)
            candidate_hosts = []
            for host in hosts:
                node_res = NODE_RESOURCES.get(host, {"cpu": 0, "mem": 0})
                usage = node_usage.get(host, {"cpu": 0, "mem": 0})
                if usage["cpu"] + cpu_req <= node_res["cpu"] and usage["mem"] + mem_req <= node_res["mem"]:
                    candidate_hosts.append(host)
            
            if candidate_hosts:
                for host in candidate_hosts:
                    mem_free_percent = get_host_mem_free_percent(host)
                    if mem_free_percent is not None and mem_free_percent > 15.0:
                        eligible_hosts.append((host, mem_free_percent))
            else:
                for i, host in enumerate(hosts):
                    if i == 0:
                        continue
                    mem_free_percent = get_host_mem_free_percent(host)
                    if mem_free_percent is not None and mem_free_percent >= 90.0:
                        eligible_hosts.append((host, mem_free_percent))
            if not eligible_hosts:
                continue
            
            # Choose the host with the largest free memory percentage
            chosen_host = max(eligible_hosts, key=lambda x: x[1])[0]
            uuid = os.path.basename(exp_dir)

            logging.info(f"Dispatching {uuid} to {chosen_host}...")
            
            remote_cmd_py = f'from util import run_cachebench; run_cachebench("{exp_dir}")'
            remote_cmd = (
                f"cd {SCRIPTS_DIR} && "
                f"nohup env CACHEBENCH_UUID={uuid} {PYTHON_EXEC} -c '{remote_cmd_py}' "
                f"> {exp_dir}/worker.log 2>&1 &"
            )
            
            subprocess.Popen(["ssh", chosen_host, remote_cmd])
            
            mark_exp_running(exp, chosen_host)
            node_usage[chosen_host]["cpu"] += cpu_req
            node_usage[chosen_host]["mem"] += mem_req
            running_jobs[exp_dir] = {"host": chosen_host, "start_time": time.time()}
            progress_made = True
            time.sleep(1)

        # --- Phase 2: If no progress was made, try to download one trace (only if downloading enabled) ---
        if NEED_DOWNLOAD_TRACES and not progress_made and pending_exps:
            logging.info("No launchable jobs with existing traces. Attempting to download a new trace.")
            # Find the first pending experiment that needs a trace
            #random.shuffle(pending_exps)  # Shuffle to avoid bias
            for exp in pending_exps:
                if not os.path.exists(exp["meta"]["trace_file"]):
                    if download_trace(exp["meta"]):
                        logging.info("Trace download successful. Will schedule jobs for it in the next cycle.")
                    else:
                        logging.warning("Trace download failed. Will try again later.")
                    # We consider the download attempt as progress to prevent a long sleep
                    progress_made = True
                    break # Only attempt one download per cycle
        
        # 4. LOGGING AND SLEEP
        now = time.time()
        if now - last_system_log_time > 60:
            log_node_system_stats(hosts)
            log_running_job_stats(running_jobs)
            dump_state_to_file(all_exps, running_jobs, STATE_FILE) # Dump state here
            last_system_log_time = now
            
        log_status_summary(all_exps, running_jobs)

        # Check if all jobs are in a finished or failed state for all experiments
        all_jobs_accounted_for = all(get_exp_status(exp) in ["finished", "failed"] for exp in all_exps)
        if not running_jobs and all_jobs_accounted_for:
            logging.info("All experiments completed across all work directories. Proceeding to summarization.")
            break
        
        sleep_time = 5 if progress_made else 60
        logging.info(f"Loop finished. Sleeping for {sleep_time} seconds.")
        time.sleep(sleep_time)

    # All experiments completed
    logging.info("=== ALL EXPERIMENTS COMPLETED. CALLING SUMMARIZE SCRIPT ===")
    
    # Call summarize_result.py to generate final report
    result_csv_path = os.path.join(LOG_DIR, "result.csv")
    summarize_script = os.path.join(SCRIPTS_DIR, "summarize_result.py")
    
    # Prepare the command arguments
    summarize_cmd = [
        PYTHON_EXEC, summarize_script,
        "--base-dirs"
    ] + WORK_DIRS + [
        "--output-file", result_csv_path
    ]
    
    logging.info(f"Running summarize script: {' '.join(summarize_cmd)}")
    
    try:
        # Run the summarize script
        result = subprocess.run(
            summarize_cmd,
            cwd=SCRIPTS_DIR,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        if result.returncode == 0:
            logging.info(f"Successfully generated summary report. Results have been written to {result_csv_path}")
            # Also log any output from the summarize script (truncated for safety)
            if result.stdout.strip():
                stdout_output = safe_log_string(result.stdout.strip(), 500)
                logging.info(f"Summarize script output: {stdout_output}")
        else:
            logging.error(f"Summarize script failed with return code {result.returncode}")
            stderr_output = safe_log_string(result.stderr, 500)
            logging.error(f"Error output: {stderr_output}")
            
    except subprocess.TimeoutExpired:
        logging.error("Summarize script timed out after 10 minutes")
    except Exception as e:
        logging.error(f"Failed to run summarize script: {e}")
    
    logging.info("=== SCHEDULER SHUTDOWN COMPLETE ===")


if __name__ == '__main__':
    schedule_experiments_reconstructable()
