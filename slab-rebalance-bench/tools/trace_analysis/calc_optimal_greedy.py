import heapq
import pandas as pd
from collections import defaultdict


def compute_optimal_allocations(mrc_dict, mrc_delta_dict, max_total_slabs, trace_names, access_freqs):
    """
    Compute the greedy slab allocations and miss ratios for each total_slab from 1 to max_total_slabs.
    Uses a greedy approach instead of dynamic programming.

    Parameters:
    mrc_dict (dict): A nested dictionary that maps trace_name to their miss ratio at different slab_cnt.
    mrc_delta_dict (dict): A nested dictionary that maps trace_name to the reduction in miss ratio (delta) for each additional slab.
    max_total_slabs (int): The maximum number of slabs to consider.
    trace_names (list): The trace names that we are interested in.
    access_freqs (list): The access frequencies for the trace names.

    Returns:
    tuple: (results, results_df) where:
        - results: A list of dictionaries, each representing the allocation at a given total_slab.
        - results_df: A DataFrame where each row corresponds to a total_slab and contains:
            - Columns for each trace_name (number of slabs allocated to the trace).
            - 'total_miss_ratio': The normalized miss ratio for the given total_slab.
            - 'total_slab_cnt': The total number of slabs.
    """
    
    # Initialize allocation tracking
    allocation = {trace_name: 0 for trace_name in trace_names}
    results = []
    
    # Initialize priority queue with utilities for first slab allocation
    max_heap = []
    for i, trace_name in enumerate(trace_names):
        if 1 in mrc_delta_dict[trace_name]:
            utility = mrc_delta_dict[trace_name][1] * access_freqs[i]
            heapq.heappush(max_heap, (-utility, i, trace_name))
    
    # Greedy allocation for each total slab count
    for total_slab in range(1, max_total_slabs + 1):
        if not max_heap:
            # No more beneficial allocations possible
            break
        
        # Get the trace with highest utility
        neg_utility, trace_idx, trace_name = heapq.heappop(max_heap)
        
        # Allocate one more slab to this trace
        allocation[trace_name] += 1
        current_slabs = allocation[trace_name]
        
        # Add next allocation opportunity for this trace back to heap if available
        next_slabs = current_slabs + 1
        if next_slabs in mrc_delta_dict[trace_name]:
            next_utility = mrc_delta_dict[trace_name][next_slabs] * access_freqs[trace_idx]
            heapq.heappush(max_heap, (-next_utility, trace_idx, trace_name))
        
        # Calculate metrics for this allocation state
        total_miss_ratio = sum(
            mrc_dict[trace_name][allocation[trace_name]] * access_freqs[i]
            for i, trace_name in enumerate(trace_names)
        ) / sum(access_freqs)
        
        # Build result row
        row = {trace_name: allocation[trace_name] for trace_name in trace_names}
        row['total_miss_ratio'] = total_miss_ratio
        row['total_slab_cnt'] = total_slab
        
        # Add detailed metrics for each trace
        for trace_name in trace_names:
            row[f"{trace_name}_miss_ratio"] = mrc_dict[trace_name][allocation[trace_name]]
            row[f"{trace_name}_miss_ratio_delta"] = (
                mrc_delta_dict[trace_name][allocation[trace_name]]
                if allocation[trace_name] in mrc_delta_dict[trace_name]
                else 0
            )
            row[f"{trace_name}_access_freq"] = access_freqs[trace_names.index(trace_name)]
        
        results.append(row)
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    return results, results_df


def greedy_allocation_with_snapshots(mrc_dict, mrc_delta_dict, wss_slabs_dict, max_total_slabs, trace_names, access_freqs):
    """
    Greedy approach to allocate slabs based on utility, with tracking of allocation order and snapshots.

    Parameters:
    mrc_dict (dict): A nested dictionary that maps trace_name to their miss ratio at different slab counts.
    mrc_delta_dict (dict): A nested dictionary that maps trace_name to the reduction in miss ratio (delta) for each additional slab.
    max_total_slabs (int): The maximum number of slabs to allocate.
    trace_names (list): The trace names (class names) to allocate slabs to.
    access_freqs (list): The access frequencies for each trace.

    Returns:
    tuple: (allocation, normalized_miss_ratio, allocation_order, snapshots_df) where:
        - allocation: A dictionary mapping each trace_name to the number of slabs allocated.
        - normalized_miss_ratio: The normalized miss ratio after all slabs are allocated.
        - allocation_order: A list tracking the order in which slabs were allocated to traces.
        - snapshots_df: A DataFrame where each row corresponds to a snapshot of the allocation at a given total_slab.
    """

    allocation = {trace_name: 0 for trace_name in trace_names}
    allocation_order = []  
    snapshots = []  

    max_heap = []
    for i, trace_name in enumerate(trace_names):
        utility = mrc_delta_dict[trace_name][1] * access_freqs[i]
        heapq.heappush(max_heap, (-utility, False, i, trace_name))


    for total_slab in range(1, max_total_slabs + 1):
        if not max_heap:
            break  

        neg_utility, index, saturated, trace_name = heapq.heappop(max_heap)
        current_slabs = allocation[trace_name]
        allocation[trace_name] += 1  
        allocation_order.append(trace_name)  

        next_slabs = current_slabs + 1
        if next_slabs + 1 in mrc_delta_dict[trace_name]:  
            next_utility = mrc_delta_dict[trace_name][next_slabs + 1] * access_freqs[index]
            # Push (-utility, index, trace_name) to the heap to maintain tie-breaking
            heapq.heappush(max_heap, (-next_utility, index, next_slabs >= wss_slabs_dict[trace_name], trace_name))
        # no more increase after it saturates
        snapshot = {trace_name: min(allocation[trace_name], wss_slabs_dict[trace_name]) for trace_name in trace_names}
        snapshot['total_slab_cnt'] = total_slab
        snapshot['total_miss_ratio'] = sum(
            mrc_dict[trace_name][allocation[trace_name]] * access_freqs[i]
            for i, trace_name in enumerate(trace_names)
        ) / sum(access_freqs)
        for trace_name in trace_names:
            snapshot[f"{trace_name}_miss_ratio"] = mrc_dict[trace_name][allocation[trace_name]]
            snapshot[f"{trace_name}_miss_ratio_delta"] = (
                mrc_delta_dict[trace_name][allocation[trace_name]]
                if allocation[trace_name] in mrc_delta_dict[trace_name]
                else 0
            )
        snapshots.append(snapshot)

    # Calculate the normalized miss ratio
    total_miss_ratio = 0
    total_access_freq = sum(access_freqs)
    for i, trace_name in enumerate(trace_names):
        slabs_allocated = allocation[trace_name]
        miss_ratio = mrc_dict[trace_name][slabs_allocated]
        total_miss_ratio += miss_ratio * access_freqs[i]

    normalized_miss_ratio = total_miss_ratio / total_access_freq

    # Convert snapshots to a DataFrame
    snapshots_df = pd.DataFrame(snapshots)

    return allocation, normalized_miss_ratio, allocation_order, snapshots_df


def calc_optimal_allocation(access_frequencies, miss_ratio_curves, max_total_slabs):
    """
    Calculate optimal allocation using greedy approach given access frequencies and miss ratio curves.
    
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
    
    # Compute optimal allocations using greedy approach
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
