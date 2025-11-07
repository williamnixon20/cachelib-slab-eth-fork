"""
install scipy and zstandard beforehand

what this script does:

given a trace (csv or oracleGeneral format), we compute the optimal slab allocation for it
works by:
(1) process the trace and group objects by allocation size boundaries
(2) compute the miss ratio curve (MRC) for each allocation size (assuming LRU, we use PARDA)
(3) compute the optimal allocation based on the MRCs and access frequencies of each class
    - optimal allocation is based on dynamic programming, see calc_optimal.py
"""

import csv
import os
import bisect
import json
import sys
import numpy as np
from collections import Counter
from multiprocessing import Pool
from scipy.stats import linregress
import pandas as pd
import bisect
from collections import defaultdict
from calc_optimal_dp import calc_optimal_allocation
from calc_optimal_greedy import calc_optimal_allocation as calc_optimal_allocation_greedy
from zstandard import ZstdDecompressor


########################## HELPER FUNCTIONS #####################################
def get_aligned_size(size, alignment):
    return (size + alignment - 1) // alignment * alignment


def generate_alloc_sizes(factor, max_size, min_size, alignment=8):
    if max_size > 4 * 1024 * 1024:
        raise ValueError(f"maximum alloc size {max_size} is more than the slab size {1024 * 1024}")

    if factor <= 1.0:
        raise ValueError(f"invalid factor {factor}")

    alloc_sizes = set()
    size = min_size

    while size < max_size:
        n_per_slab = 4 * 1024 * 1024 // size  # Assuming Slab::kSize is 1MB
        if n_per_slab <= 1:
            break
        alloc_sizes.add(size)
        prev_size = size
        size = get_aligned_size(int(size * factor), alignment)
        if prev_size == size:
            raise ValueError(f"invalid incFactor {factor}")

    alloc_sizes.add(get_aligned_size(max_size, alignment))
    return alloc_sizes


def compute_reuse_distances(reference_sequence):
    """
    Computes reuse distances for each element in the sequence and returns a histogram.
    
    Args:
        reference_sequence (list): Sequence of data accesses (e.g., ['A', 'B', 'A']).
    
    Returns:
        tuple: (list of reuse distances, histogram as a dict).
    """
    last_access_time = {}  # Hash table (H): maps elements to last access time
    access_times_tree = []  # Binary search tree (T): maintains sorted access times
    hist = defaultdict(int) # Histogram of reuse distances

    for current_time, element in enumerate(reference_sequence):
        reuse_distance = -1  # Default: no prior access (∞)
        
        if element in last_access_time:
            last_time = last_access_time[element]
            # Find the number of accesses after `last_time` (reuse distance)
            idx = bisect.bisect_right(access_times_tree, last_time)
            reuse_distance = len(access_times_tree) - idx
            # Remove the old access time from the tree
            del access_times_tree[bisect.bisect_left(access_times_tree, last_time)]

        hist[reuse_distance] += 1
        
        # Update tree and hash table
        bisect.insort(access_times_tree, current_time)
        last_access_time[element] = current_time
    
    return dict(hist)


def parse_binary_record(reader):
    """Parse a single binary record from the stream."""
    record = reader.read(24)
    if len(record) < 24:
        return None  # End of file
    
    obj_id = int.from_bytes(record[4:12], byteorder='little', signed=False)
    obj_size = int.from_bytes(record[12:16], byteorder='little', signed=False)
    obj_size = max(24, obj_size)
    obj_size += (32 + len(str(obj_id)) + 20)  # 20 for keysize, 32 is metadata
    
    return obj_id, obj_size


def parse_csv_record(row):
    """Parse a single CSV record."""
    object_id = row['object_id']
    object_size = int(row['object_size'])
    object_size = max(24, object_size)
    object_size += (32 + len(str(object_id)))  # Key size and meta-data overhead
    
    return object_id, object_size


def subtrace_statistics_helper(object_ids):
    """
    Compute statistics for a subtrace including Zipf fitting.
    
    Args:
        object_ids (list): List of object IDs in the subtrace
        
    Returns:
        tuple: (record_count, distinct_object_count, slope, intercept, zipf_r2, p_value)
    """
    # Total number of records in the subtrace
    record_count = len(object_ids)

    # Count distinct object IDs
    distinct_object_count = len(set(object_ids))

    # Perform Zipf linear fitting
    if distinct_object_count > 0:
        # Use NumPy to calculate frequencies
        object_ids_array = np.array(object_ids)
        _, frequencies = np.unique(object_ids_array, return_counts=True)

        # Sort frequencies in descending order
        frequencies = np.sort(frequencies)[::-1]

        # Generate ranks
        ranks = np.arange(1, len(frequencies) + 1)

        # Perform linear regression on log-log scale
        log_ranks = np.log(ranks)
        log_frequencies = np.log(frequencies)
        slope, intercept, r_value, p_value, stderr = linregress(log_ranks, log_frequencies)
        zipf_r2 = r_value**2
    else:
        # If no distinct objects, set Zipf fitting values to None
        slope, intercept, zipf_r2, p_value = None, None, None, None
    
    return record_count, distinct_object_count, slope, intercept, zipf_r2, p_value


###############################################################

def compute_optimal_allocation(
    trace_file_path,
    max_total_slabs,
    result_path=None,
    is_binary_compressed=False,
    tmp_work_dir="/tmp",
    slab_size=4,  # 4MB default (input in MB)
    alloc_sizes=None,
    min_alloc_size=None,
    max_alloc_size=None,
    alloc_factor=None
):
    """
    Compute optimal slab allocation for a given trace.
    
    Args:
        trace_file_path (str): Path to the trace file (CSV or binary format)
        max_total_slabs (int): Maximum number of slabs available
        result_path (str): Directory where results will be saved. Default: current directory
        is_binary_compressed (bool): Whether trace file is binary compressed. Default: False
        tmp_work_dir (str): Temporary working directory. Default: "/tmp"
        slab_size (int): Size of each slab in MB. Default: 4MB
        alloc_sizes (list): List of allocation sizes. If None, will be generated from other params
        min_alloc_size (int): Minimum allocation size (used if alloc_sizes is None)
        max_alloc_size (int): Maximum allocation size (used if alloc_sizes is None) 
        alloc_factor (float): Growth factor for allocation sizes (used if alloc_sizes is None)
    
    Returns:
        dict: Optimal allocation results
        
    Raises:
        ValueError: If neither alloc_sizes nor (min_alloc_size, max_alloc_size, alloc_factor) are provided
    """
    
    # Convert slab_size from MB to bytes
    slab_size_bytes = slab_size * 1024 * 1024
    
    # Validate input parameters
    if alloc_sizes is None:
        if min_alloc_size is None or max_alloc_size is None or alloc_factor is None:
            raise ValueError(
                "Must provide either 'alloc_sizes' or all of "
                "('min_alloc_size', 'max_alloc_size', 'alloc_factor')"
            )
        # Generate allocation sizes if not provided
        alloc_sizes = generate_alloc_sizes(alloc_factor, max_alloc_size, min_alloc_size)
    
    print(f"Processing trace: {trace_file_path}")
    print(f"Binary compressed: {is_binary_compressed}")
    print(f"Temp directory: {tmp_work_dir}")
    print(f"Slab size: {slab_size}MB ({slab_size_bytes} bytes)")
    print(f"Max total slabs: {max_total_slabs}")
    print(f"Allocation sizes: {sorted(alloc_sizes)}")
    print(f"Results will be saved to: {result_path}")
    
    # Create actual working directory based on trace filename
    trace_filename = os.path.basename(trace_file_path)  # Get filename only
    trace_name = trace_filename.split('.')[0]  # Get part before first dot
    actual_work_dir = os.path.join(tmp_work_dir, trace_name)
    
    print(f"Actual working directory: {actual_work_dir}")
    
    # (1) Process trace and group by allocation sizes
    os.makedirs(actual_work_dir, exist_ok=True)
    
    # Create subdirectories
    subtraces_dir = os.path.join(actual_work_dir, "subtraces")
    subtrace_mrcs_dir = os.path.join(actual_work_dir, "subtrace_mrcs")
    os.makedirs(subtraces_dir, exist_ok=True)
    os.makedirs(subtrace_mrcs_dir, exist_ok=True)
    
    alloc_sizes_list = sorted(alloc_sizes)
    alloc_sizes_json_path = os.path.join(actual_work_dir, "alloc_size.json")
    with open(alloc_sizes_json_path, 'w') as json_file:
        json.dump(alloc_sizes_list, json_file, indent=4)
    
    # Group object IDs by allocation size - write to temporary files in subtraces directory
    alloc_size_files = {}
    alloc_size_writers = {}
    record_count = 0
    
    # Initialize temporary files for each allocation size in subtraces directory
    for alloc_size in alloc_sizes_list:
        temp_file_path = os.path.join(subtraces_dir, f"subtrace_{alloc_size}.txt")
        alloc_size_files[alloc_size] = open(temp_file_path, 'w')
        alloc_size_writers[alloc_size] = alloc_size_files[alloc_size]
    
    if is_binary_compressed:
        # Binary compressed format processing
        with open(trace_file_path, 'rb') as binary_file:
            decompressor = ZstdDecompressor()
            reader = decompressor.stream_reader(binary_file)
            
            while True:
                parsed = parse_binary_record(reader)
                if parsed is None:
                    break  # End of file
                
                obj_id, obj_size = parsed
                
                # Find the smallest allocation size that can fit the object
                index = bisect.bisect_left(alloc_sizes_list, obj_size)
                if index < len(alloc_sizes_list):
                    alloc_size = alloc_sizes_list[index]
                else:
                    alloc_size = alloc_sizes_list[-1]  # Use largest if object too big
                
                # Write object_id to the corresponding temporary file
                alloc_size_writers[alloc_size].write(f"{obj_id}\n")
                record_count += 1
    
    else:
        # CSV format processing
        with open(trace_file_path, 'r') as csvfile:
            reader_csv = csv.DictReader(csvfile)
            
            for row in reader_csv:
                obj_id, obj_size = parse_csv_record(row)
                
                # Find the smallest allocation size that can fit the object
                index = bisect.bisect_left(alloc_sizes_list, obj_size)
                if index < len(alloc_sizes_list):
                    alloc_size = alloc_sizes_list[index]
                else:
                    alloc_size = alloc_sizes_list[-1]  # Use largest if object too big
                
                # Write object_id to the corresponding temporary file
                alloc_size_writers[alloc_size].write(f"{obj_id}\n")
                record_count += 1
    
    # Close all temporary files
    for f in alloc_size_files.values():
        f.close()
    
    print(f"Processed {record_count} records")
    print(f"Created subtrace files for {len(alloc_sizes_list)} allocation sizes")
    
    # (2) Process subtraces one by one: read, compute histogram, compute MRC, save MRC, delete subtrace
    print("Processing subtraces one by one...")
    reuse_histograms = {}
    alloc_size_counts = {}
    mrcs = {}
    memory_sizes = [i * slab_size_bytes for i in range(1, max_total_slabs + 1)]
    
    # Initialize subtrace statistics file
    subtrace_stats_path = os.path.join(actual_work_dir, "subtrace_stats.csv")
    with open(subtrace_stats_path, 'w', newline='') as stats_file:
        stats_writer = csv.writer(stats_file)
        stats_writer.writerow(['alloc_size', 'record_count', 'distinct_object_count', 'zipf_slope', 'zipf_intercept', 'zipf_r2', 'zipf_p_value'])
        
        for alloc_size in alloc_sizes_list:
            subtrace_file_path = os.path.join(subtraces_dir, f"subtrace_{alloc_size}.txt")
            mrc_file_path = os.path.join(subtrace_mrcs_dir, f"mrc_{alloc_size}.csv")
            
            print(f"Processing subtrace for alloc_size {alloc_size}...")
            
            # Read object IDs from subtrace file
            object_ids = []
            try:
                with open(subtrace_file_path, 'r') as f:
                    for line in f:
                        obj_id = line.strip()
                        if obj_id:  # Skip empty lines
                            object_ids.append(obj_id)
            except FileNotFoundError:
                # File doesn't exist, meaning no objects for this allocation size
                object_ids = []
            
            alloc_size_counts[alloc_size] = len(object_ids)
            
            # Compute subtrace statistics
            if object_ids:
                print(f"  Computing subtrace statistics ({len(object_ids)} objects)")
                record_count, distinct_object_count, slope, intercept, zipf_r2, p_value = subtrace_statistics_helper(object_ids)
                
                # Write statistics to CSV
                stats_writer.writerow([alloc_size, record_count, distinct_object_count, slope, intercept, zipf_r2, p_value])
                
                print(f"  Computing reuse distance histogram")
                # Compute reuse distance histogram
                histogram = compute_reuse_distances(object_ids)
                reuse_histograms[alloc_size] = histogram
                
                # Compute MRC for this allocation size
                print(f"  Computing miss ratio curve")
                total_records = sum(histogram.values())
                mrc = []
                
                # Save MRC to individual file
                with open(mrc_file_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['alloc_size', 'cache_size', 'slab_cnt', 'miss_count', 'miss_ratio', 'miss_ratio_delta'])
                    
                    last_miss_ratio = 1.0
                    for memory_size in memory_sizes:
                        slab_cnt = memory_size // slab_size_bytes
                        max_objects = memory_size // alloc_size
                        
                        miss_count = sum(
                            count for reuse_distance, count in histogram.items() 
                            if (reuse_distance >= max_objects or reuse_distance == -1)
                        )
                        miss_ratio = miss_count / total_records
                        miss_ratio_delta = last_miss_ratio - miss_ratio
                        
                        mrc_point = {
                            'cache_size': memory_size,
                            'slab_cnt': slab_cnt,
                            'miss_count': miss_count,
                            'miss_ratio': miss_ratio,
                            'miss_ratio_delta': miss_ratio_delta
                        }
                        mrc.append(mrc_point)
                        
                        writer.writerow([alloc_size, memory_size, slab_cnt, miss_count, miss_ratio, miss_ratio_delta])
                        last_miss_ratio = miss_ratio
                
                mrcs[alloc_size] = mrc
                print(f"  Saved MRC to {mrc_file_path}")
            else:
                print(f"  No objects found for alloc_size {alloc_size}")
                # Write empty statistics for allocation sizes with no objects
                stats_writer.writerow([alloc_size, 0, 0, None, None, None, None])
            
            # Delete subtrace file after processing
            try:
                os.remove(subtrace_file_path)
                print(f"  Deleted subtrace file {subtrace_file_path}")
            except FileNotFoundError:
                pass  # File already doesn't exist
    
    print(f"Generated {len(mrcs)} miss ratio curves")
    print(f"Saved subtrace statistics to {subtrace_stats_path}")
    
    # (3) Compute optimal allocation using both DP and greedy approaches
    print("Computing optimal allocation using DP approach...")
    
    # Prepare data for calc_optimal_allocation
    # This function expects: access_frequencies, miss_ratio_curves, max_total_slabs
    access_frequencies = {}
    miss_ratio_curves = {}
    
    for alloc_size in alloc_sizes_list:
        # Use the counts we tracked during subtrace processing
        access_frequencies[alloc_size] = alloc_size_counts.get(alloc_size, 0)
            
        if alloc_size in mrcs:
            # Convert MRC to the format expected by calc_optimal_allocation
            miss_ratio_curves[alloc_size] = [point['miss_ratio'] for point in mrcs[alloc_size]]
    
    # Call the DP optimal allocation function
    try:
        optimal_allocation_dp = calc_optimal_allocation(
            access_frequencies, 
            miss_ratio_curves, 
            max_total_slabs
        )
        print("DP allocation computation completed successfully")
    except Exception as e:
        print(f"Error in DP optimal allocation calculation: {e}")
        optimal_allocation_dp = None
    
    # Call the greedy optimal allocation function
    print("Computing optimal allocation using greedy approach...")
    try:
        optimal_allocation_greedy = calc_optimal_allocation_greedy(
            access_frequencies, 
            miss_ratio_curves, 
            max_total_slabs
        )
        print("Greedy allocation computation completed successfully")
    except Exception as e:
        print(f"Error in greedy optimal allocation calculation: {e}")
        optimal_allocation_greedy = None
    
    # Save optimal allocations DataFrames to CSV if available
    result_dir = result_path if result_path else "."
    os.makedirs(result_dir, exist_ok=True)
    
    optimal_allocations_dp_csv_path = None
    optimal_allocations_greedy_csv_path = None
    
    # Save DP results
    if optimal_allocation_dp and 'optimal_allocations_df' in optimal_allocation_dp:
        optimal_allocations_dp_csv_path = os.path.join(result_dir, f"{trace_name}_optimal_allocation_dp.csv")
        optimal_allocation_dp['optimal_allocations_df'].to_csv(
            optimal_allocations_dp_csv_path, 
            index=False, 
            float_format='%.6f'
        )
        print(f"DP optimal allocations DataFrame saved to: {optimal_allocations_dp_csv_path}")
    
    # Save greedy results
    if optimal_allocation_greedy and 'optimal_allocations_df' in optimal_allocation_greedy:
        optimal_allocations_greedy_csv_path = os.path.join(result_dir, f"{trace_name}_optimal_allocation_greedy.csv")
        optimal_allocation_greedy['optimal_allocations_df'].to_csv(
            optimal_allocations_greedy_csv_path, 
            index=False, 
            float_format='%.6f'
        )
        print(f"Greedy optimal allocations DataFrame saved to: {optimal_allocations_greedy_csv_path}")
    
    # Compare results at max total slabs if both are available
    if (optimal_allocation_dp and optimal_allocation_greedy and 
        optimal_allocation_dp.get('optimal_allocations') and 
        optimal_allocation_greedy.get('optimal_allocations')):
        
        dp_final = optimal_allocation_dp['optimal_allocations'][-1]
        greedy_final = optimal_allocation_greedy['optimal_allocations'][-1]
        
        print(f"\nComparison at {max_total_slabs} total slabs:")
        print(f"DP total miss ratio: {dp_final.get('total_miss_ratio', 'N/A'):.6f}")
        print(f"Greedy total miss ratio: {greedy_final.get('total_miss_ratio', 'N/A'):.6f}")
        
        if dp_final.get('total_miss_ratio') and greedy_final.get('total_miss_ratio'):
            ratio = greedy_final['total_miss_ratio'] / dp_final['total_miss_ratio']
            print(f"Greedy/DP ratio: {ratio:.6f} ({((ratio-1)*100):+.2f}%)")

def main():
    compute_optimal_allocation(
        trace_file_path='/mnt/cfs/hongshu/traces/synth_static_202.csv',
        max_total_slabs=4096,
        result_path=None,
        is_binary_compressed=False,
        tmp_work_dir='/mnt/cfs/hongshu/subtraces/',
        slab_size=4,
        alloc_sizes=[256, 512, 1024, 2048, 4096],
        min_alloc_size=None,
        max_alloc_size=None,
        alloc_factor=None
    )


if __name__ == "__main__":
    main()





