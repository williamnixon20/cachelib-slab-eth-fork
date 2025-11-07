import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path
import sys
import os

# Add parent directory to path to import const
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *

def plot_rebalanced_slabs(trace_name, data_path, output_dir="."):
    """
    Plot rebalanced_slabs for a given trace_name.
    Creates three separate figures, one for each allocator (LRU, LRU2Q, TINYLFU).
    
    Parameters:
    trace_name (str): The trace name to filter the data
    data_path (str): Path to the CSV data file
    output_dir (str): Directory to save the output plots (default: current directory)
    """
    
    # Read the data
    df = pd.read_csv(data_path)
    
    # Filter by trace_name and exclude 'disabled' strategy (always 0 rebalanced slabs)
    df_filtered = df[(df['trace_name'] == trace_name) & (df['rebalance_strategy'] != 'disabled')].copy()
    
    if df_filtered.empty:
        print(f"No data found for trace_name: {trace_name} (after excluding disabled strategy)")
        return
    
    # Convert wsr to percentage
    df_filtered['wsr_percent'] = df_filtered['wsr'] * 100
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    # Calculate global y-axis range for rebalanced_slabs across all allocators
    all_rebalanced_slabs = []
    for allocator in allocator_order:
        allocator_data = df_filtered[df_filtered['allocator'] == allocator]
        if not allocator_data.empty:
            all_rebalanced_slabs.extend(allocator_data['rebalanced_slabs'].values)
    
    if all_rebalanced_slabs:
        y_min = min(all_rebalanced_slabs)
        y_max = max(all_rebalanced_slabs)
        # Add some padding (5% on each side)
        y_range = y_max - y_min
        y_min_padded = max(0, y_min - 0.05 * y_range)
        y_max_padded = y_max + 0.05 * y_range
    else:
        y_min_padded, y_max_padded = 0, 100
    
    # Create three separate figures
    for i, (allocator, allocator_label) in enumerate(zip(allocator_order, allocator_labels)):
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Filter data for this allocator
        allocator_data = df_filtered[df_filtered['allocator'] == allocator]
        
        if allocator_data.empty:
            print(f"No data found for allocator: {allocator}")
            continue
        
        # Get unique strategies in this data and order them
        available_strategies = allocator_data['rebalance_strategy'].unique()
        strategies = [s for s in strategy_order if s in available_strategies]
        
        # Plot each strategy in the defined order
        for strategy in strategies:
            strategy_data = allocator_data[allocator_data['rebalance_strategy'] == strategy]
            
            if strategy_data.empty:
                continue
            
            # Sort by wsr_percent for proper line plotting
            strategy_data = strategy_data.sort_values('wsr_percent')
            
            # Get styling
            color = strategy_colors.get(strategy, '#000000')
            label = strategy_labels.get(strategy, strategy)
            linestyle = strategy_linestyles.get(strategy, '-')
            marker = strategy_markers.get(strategy, 'o')
            
            # Plot the line
            ax.plot(strategy_data['wsr_percent'], 
                   strategy_data['rebalanced_slabs'],
                   color=color, 
                   label=label,
                   linestyle=linestyle,
                   marker=marker,
                   markersize=12,
                   linewidth=2.5,
                   markerfacecolor=color,
                   markeredgecolor='white',
                   markeredgewidth=1)
        
        # Customize the plot
        ax.set_xlabel('Cache Size (% of Working Set)')
        ax.set_ylabel('Number of Rebalanced Slabs')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='best', frameon=True, fancybox=True, shadow=True)
        
        # Set reasonable axis limits
        ax.set_xlim(left=0)
        ax.set_ylim(y_min_padded, y_max_padded)
        
        # Set x-axis ticks with step size of 10
        ax.set_xticks(np.arange(0, 50, 10))
        
        # Tight layout
        plt.tight_layout()
        
        # Save the figure
        output_path = os.path.join(output_dir, f"rebalanced_slabs_{trace_name}_{allocator}.pdf")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        # Show the plot
        plt.show()
        
        print(f"Saved plot for {allocator_label}: {output_path}")

# Example usage
if __name__ == "__main__":
    # Example: plot for meta_202210_kv trace
    data_path = "../result/efficiency_result_processed.csv"
    output_dir = "figures/metaKV"
    
    plot_rebalanced_slabs("meta_202210_kv", data_path, output_dir)
    plot_rebalanced_slabs("meta_202401_kv", data_path, output_dir)
    plot_rebalanced_slabs("meta_memcache_2024_kv", data_path, output_dir)