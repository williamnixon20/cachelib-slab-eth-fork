import json
import os
from collections import defaultdict, Counter

# specify the the dir with the current scheduler_state.json file that the master process dumps to
input_dir = '../master/20250811_194327'


with open(f"{input_dir}/scheduler_state.json") as f:
    data = json.load(f)

summary = defaultdict(Counter)

for entry in data:
    uuid = entry.get("uuid", "")
    status = entry.get("status", "")
    trace_name = uuid.split("-", 1)[0]
    summary[trace_name][status] += 1

print(f"{'Trace Name':<25} {'todo':>6} {'running':>8} {'finished':>9} {'failed':>7}")
print("-" * 60)

# Calculate totals
total_todo = 0
total_running = 0
total_finished = 0
total_failed = 0

for trace_name, counts in sorted(summary.items()):
    todo = counts.get('todo', 0)
    running = counts.get('running', 0)
    finished = counts.get('finished', 0)
    failed = counts.get('failed', 0)
    
    print(f"{trace_name:<25} {todo:>6} {running:>8} {finished:>9} {failed:>7}")
    
    # Add to totals
    total_todo += todo
    total_running += running
    total_finished += finished
    total_failed += failed

# Print separator and totals
print("-" * 60)
print(f"{'TOTAL':<25} {total_todo:>6} {total_running:>8} {total_finished:>9} {total_failed:>7}")

# Summary of running jobs by host
print("\n" + "="*60)
print("RUNNING JOBS BY HOST")
print("="*60)

host_summary = Counter()
running_jobs_by_host = defaultdict(list)

for entry in data:
    status = entry.get("status", "")
    host = entry.get("host", "")
    uuid = entry.get("uuid", "")
    
    if status == "running" and host:
        host_summary[host] += 1
        running_jobs_by_host[host].append(uuid)

if host_summary:
    print(f"{'Host':<30} {'Running Jobs':>12}")
    print("-" * 45)
    
    for host, count in sorted(host_summary.items()):
        print(f"{host:<30} {count:>12}")
    
    print("-" * 45)
    print(f"{'TOTAL RUNNING':<30} {sum(host_summary.values()):>12}")
else:
    print("No running jobs found.")
