
import os
import pandas as pd
from collections import defaultdict


def build_dp_table(mrc_dict, max_total_slabs, trace_names, access_freqs, pretty_print=False):
    """
    Builds the DP table and allocation table for the given trace names and maximum total slabs.

    Parameters:
    mrc_dict (dict): A nested dictionary that maps trace_name to their miss ratio at different slab_cnt.
    max_total_slabs (int): The maximum number of slabs to consider.
    trace_names (list): The trace names that we are interested in.
    access_freqs (list): The access frequencies for the trace names.
    pretty_print (bool): If True, pretty print the DP table and allocation table.

    Returns:
    tuple: (dp, allocation) where:
        - dp: The DP table storing the minimum weighted miss ratio for each trace and slab count.
        - allocation: The allocation table storing the number of slabs allocated to each trace.
    """
    # Number of traces
    n = len(trace_names)
    
    # Initialize the DP table
    dp = [[float('inf')] * (max_total_slabs + 1) for _ in range(n + 1)]
    dp[0][0] = 0  # Base case: 0 slabs for 0 traces has a miss ratio of 0
    
    # Initialize the allocation table
    allocation = [[0] * (max_total_slabs + 1) for _ in range(n + 1)]
    
    # Fill the DP table
    for i in range(1, n + 1):
        trace_name = trace_names[i - 1]
        access_freq = access_freqs[i - 1]
        for j in range(max_total_slabs + 1):
            for k in range(j + 1):
                miss_ratio = mrc_dict[trace_name].get(k, 1)
                miss_count = miss_ratio * access_freq
                if dp[i - 1][j - k] + miss_count < dp[i][j]:
                    dp[i][j] = dp[i - 1][j - k] + miss_count
                    allocation[i][j] = k
                    
    
    # Pretty print the DP table and allocation table if requested
    if pretty_print:
        print("DP Table:")
        for row in dp:
            print(', '.join([f'{x:.4f}' for x in row]))
        print("\nAllocation Table:")
        for row in allocation:
            print(', '.join([f'{x:3d}' for x in row]))
    
    return dp, allocation


def backtrack_allocation(dp, allocation, trace_names, total_slabs, access_freqs):
    """
    Performs backtracking on the precomputed DP table to determine the optimal allocation for a given total_slabs.

    Parameters:
    dp (list): The DP table built by `build_dp_table`.
    allocation (list): The allocation table built by `build_dp_table`.
    trace_names (list): The trace names that we are interested in.
    total_slabs (int): The total number of slabs to allocate.
    access_freqs (list): The access frequencies for the trace names.

    Returns:
    tuple: (result, normalized_miss_ratio) where:
        - result: A dictionary with the optimal allocation of slabs for each trace name.
        - normalized_miss_ratio: The minimized weighted miss ratio normalized by the total access frequency.
    """
    # Number of traces
    n = len(trace_names)
    
    # Backtrack to find the optimal allocation
    result = {}
    j = total_slabs
    for i in range(n, 0, -1):
        trace_name = trace_names[i - 1]
        result[trace_name] = allocation[i][j]
        j -= allocation[i][j]
    
    # Normalized miss ratio
    normalized_miss_ratio = dp[n][total_slabs] / sum(access_freqs)
    
    return result, normalized_miss_ratio



def compute_optimal_allocations(mrc_dict, mrc_delta_dict, max_total_slabs, trace_names, access_freqs):
    """
    Compute the optimal slab allocations and miss ratios for each total_slab from 1 to max_total_slabs.

    Parameters:
    mrc_dict (dict): A nested dictionary that maps trace_name to their miss ratio at different slab_cnt.
    max_total_slabs (int): The maximum number of slabs to consider.
    trace_names (list): The trace names that we are interested in.
    access_freqs (list): The access frequencies for the trace names.

    Returns:
    pd.DataFrame: A DataFrame where each row corresponds to a total_slab and contains:
        - Columns for each trace_name (number of slabs allocated to the trace).
        - 'total_miss_ratio': The normalized miss ratio for the given total_slab.
        - 'total_slab_cnt': The total number of slabs.
    """

    dp, allocation = build_dp_table(mrc_dict, max_total_slabs, trace_names, access_freqs)


    results = []
    for total_slab in range(1, max_total_slabs + 1):
        alloc, miss_ratio = backtrack_allocation(dp, allocation, trace_names, total_slab, access_freqs)
        
        row = {trace_name: alloc[trace_name] for trace_name in trace_names}
        row['total_miss_ratio'] = miss_ratio
        row['total_slab_cnt'] = total_slab
        for trace_name in trace_names:
            row[f"{trace_name}_miss_ratio"] = mrc_dict[trace_name][alloc[trace_name]]
            row[f"{trace_name}_miss_ratio_delta"] = mrc_delta_dict[trace_name][alloc[trace_name]]
            row[f"{trace_name}_access_freq"] = access_freqs[trace_names.index(trace_name)]
        results.append(row)

    results_df = pd.DataFrame(results)
    return results, results_df 


def calc_optimal_allocation(access_frequencies, miss_ratio_curves, max_total_slabs):
    """
    Calculate optimal allocation given access frequencies and miss ratio curves.
    
    Parameters:
    access_frequencies (dict): Maps alloc_size to access frequency (number of accesses)
    miss_ratio_curves (dict): Maps alloc_size to list of miss ratios (indexed by slab count)
    max_total_slabs (int): Maximum number of slabs to consider
    
    Returns:
    dict: Optimal allocation results
    """
    # Convert to the format expected by compute_optimal_allocations
    mrc_dict = defaultdict(dict)
    mrc_delta_dict = defaultdict(dict)
    
    # Build mrc_dict and mrc_delta_dict from miss_ratio_curves
    for alloc_size, miss_ratios in miss_ratio_curves.items():
        # Set base case for 0 slabs
        mrc_dict[alloc_size][0] = 1.0
        mrc_delta_dict[alloc_size][0] = float('inf')
        
        # Fill in miss ratios for each slab count
        last_miss_ratio = 1.0
        for slab_cnt, miss_ratio in enumerate(miss_ratios, start=1):
            mrc_dict[alloc_size][slab_cnt] = miss_ratio
            miss_ratio_delta = last_miss_ratio - miss_ratio
            mrc_delta_dict[alloc_size][slab_cnt] = miss_ratio_delta
            last_miss_ratio = miss_ratio
    
    # Get the allocation sizes and their access frequencies
    alloc_sizes = list(miss_ratio_curves.keys())
    access_freqs = [access_frequencies.get(alloc_size, 0) for alloc_size in alloc_sizes]
    
    # Compute optimal allocations
    results, results_df = compute_optimal_allocations(
        mrc_dict, 
        mrc_delta_dict, 
        max_total_slabs, 
        alloc_sizes, 
        access_freqs
    )
    
    return {
        'optimal_allocations': results,
        'optimal_allocations_df': results_df,
        'mrc_dict': dict(mrc_dict),
        'access_frequencies': access_frequencies,
        'alloc_sizes': alloc_sizes
    }


def calc_optimal_allocation_from_files(directory, slab_upper_limit=4096):
    """
    Calculate optimal allocation from CSV files (backward compatibility).
    
    Parameters:
    directory (str): Directory containing subtrace_stats.csv and MRC files
    slab_upper_limit (int): Maximum number of slabs to consider
    
    Returns:
    dict: Optimal allocation results
    """
    import glob
    
    # Read subtrace statistics
    subtrace_stats_path = os.path.join(directory, "subtrace_stats.csv")
    if not os.path.exists(subtrace_stats_path):
        raise FileNotFoundError(f"Could not find {subtrace_stats_path}")
    
    subtrace_stat_df = pd.read_csv(subtrace_stats_path)
    
    # Read MRC files from subtrace_mrcs directory
    subtrace_mrcs_dir = os.path.join(directory, "subtrace_mrcs")
    if not os.path.exists(subtrace_mrcs_dir):
        raise FileNotFoundError(f"Could not find {subtrace_mrcs_dir}")
    
    # Build access frequencies from subtrace stats
    access_frequencies = {}
    for _, row in subtrace_stat_df.iterrows():
        access_frequencies[row['alloc_size']] = row['record_count']
    
    # Build miss ratio curves from individual MRC files
    miss_ratio_curves = {}
    mrc_files = glob.glob(os.path.join(subtrace_mrcs_dir, "mrc_*.csv"))
    
    for mrc_file in mrc_files:
        # Extract alloc_size from filename (e.g., "mrc_1024.csv" -> 1024)
        filename = os.path.basename(mrc_file)
        alloc_size = int(filename.replace("mrc_", "").replace(".csv", ""))
        
        # Read MRC data
        mrc_df = pd.read_csv(mrc_file)
        miss_ratios = mrc_df['miss_ratio'].tolist()
        miss_ratio_curves[alloc_size] = miss_ratios
    
    # Call the main function
    return calc_optimal_allocation(access_frequencies, miss_ratio_curves, slab_upper_limit)

