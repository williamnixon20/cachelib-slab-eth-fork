import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from scipy import stats
import os
import sys

# Add parent directory to path to import const
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *

def plot_rebalancer_cycles(csv_path, output_dir='.'):
    """
    Plot rebalancer CPU cycles by allocator and strategy.
    Normalizes by 1e12 and creates bar plots with mean and error bars for each WSR value.
    
    Args:
        csv_path: Path to the CSV file containing overhead data
        output_dir: Directory to save the output plots
    """
    
    # Read the data
    print(f"Reading data from {csv_path}")
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows")
    
    # Filter out disabled strategy
    df = df[df['rebalance_strategy'] != 'disabled']
    print(f"After filtering out disabled strategy: {len(df)} rows")
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Create plots for both WSR values
    wsr_values = [0.1, 0.01]
    
    for wsr in wsr_values:
        # Filter data for current WSR
        wsr_data = df[df['wsr'] == wsr].copy()
        print(f"\nProcessing WSR = {wsr}")
        print(f"Data points for WSR {wsr}: {len(wsr_data)}")
        
        if len(wsr_data) == 0:
            print(f"No data found for WSR = {wsr}")
            continue
        
        # Create figure
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Calculate statistics for each allocator-strategy combination
        stats_data = []
        for allocator in allocator_order:
            for strategy in strategy_order:
                subset = wsr_data[
                    (wsr_data['allocator'] == allocator) & 
                    (wsr_data['rebalance_strategy'] == strategy)
                ]
                
                if len(subset) > 0:
                    # Use raw values without normalization
                    mean_val = subset['pool_rebalancer_cpu_cycles'].mean()
                    std_val = subset['pool_rebalancer_cpu_cycles'].std() if len(subset) > 1 else 0
                    sem_val = subset['pool_rebalancer_cpu_cycles'].sem() if len(subset) > 1 else 0
                    
                    # Only add if mean value is meaningful (not zero or near-zero)
                    if mean_val > 1000:  # Skip values less than 1000 cycles
                        stats_data.append({
                            'allocator': allocator,
                            'strategy': strategy,
                            'mean': mean_val,
                            'std': std_val,
                            'sem': sem_val,
                            'count': len(subset)
                        })
        
        stats_df = pd.DataFrame(stats_data)
        
        if len(stats_df) == 0:
            print(f"No meaningful data found for WSR = {wsr}")
            continue
        
        # Create bar plot with dynamic positioning per allocator
        x_positions = np.arange(len(allocator_order))
        bar_width = 0.13
        
        # Get all unique strategies that have data
        all_strategies_with_data = sorted(stats_df['strategy'].unique(), 
                                        key=lambda x: strategy_order.index(x))
        
        # Plot bars for each allocator separately to handle different strategy sets
        legend_added = set()
        
        for allocator_idx, allocator in enumerate(allocator_order):
            # Get strategies that have data for this specific allocator
            allocator_strategies = stats_df[stats_df['allocator'] == allocator]['strategy'].tolist()
            allocator_strategies = sorted(allocator_strategies, key=lambda x: strategy_order.index(x))
            
            if not allocator_strategies:
                continue
                
            # Calculate positions for this allocator's bars
            n_bars = len(allocator_strategies)
            start_offset = -(n_bars - 1) * bar_width / 2
            
            for i, strategy in enumerate(allocator_strategies):
                position = x_positions[allocator_idx] + start_offset + i * bar_width
                
                # Get data for this specific combination
                row = stats_df[(stats_df['allocator'] == allocator) & 
                              (stats_df['strategy'] == strategy)].iloc[0]
                
                # Add label only once per strategy for legend
                label = strategy_labels.get(strategy, strategy) if strategy not in legend_added else ""
                if strategy not in legend_added:
                    legend_added.add(strategy)
                
                ax.bar(position, row['mean'], bar_width,
                      label=label,
                      color=strategy_colors.get(strategy, '#808080'),
                      yerr=row['sem'], capsize=4,
                      edgecolor='black', linewidth=0.5)
        
        # Customize the plot
        ax.set_xlabel('Eviction Policy')
        ax.set_ylabel('CPU Cycles for Rebalancing')
        ax.set_xticks(x_positions)
        ax.set_xticklabels(allocator_labels)
        
        # Add dashed vertical lines to separate different eviction policies
        for i in range(len(allocator_order) - 1):
            separator_x = x_positions[i] + 0.5
            ax.axvline(x=separator_x, color='gray', linestyle='--', alpha=0.6, linewidth=1)
        
        # Add grid
        ax.grid(True, alpha=0.3, axis='y')
        ax.set_axisbelow(True)
        
        # Position legend at the top with more space
        ax.legend(bbox_to_anchor=(0.5, 1.08), loc='lower center', ncol=3, frameon=True, shadow=True, 
                              framealpha=0.9, edgecolor='black')
        
        # Tight layout with padding for legend
        plt.tight_layout()
        plt.subplots_adjust(top=0.85)
        
        # Save the plot
        wsr_str = f"{wsr:.2f}".replace('.', '_')
        output_file = os.path.join(output_dir, f'rebalancer_cycles_wsr_{wsr_str}.pdf')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_file}")
        
        # Print summary statistics
        print(f"\n=== Summary Statistics for WSR = {wsr} ===")
        print(f"Total combinations in stats_df: {len(stats_df)}")
        
        all_strategies_with_data = sorted(stats_df['strategy'].unique(), 
                                        key=lambda x: strategy_order.index(x))
        
        for strategy in all_strategies_with_data:
            strategy_stats = stats_df[stats_df['strategy'] == strategy]
            print(f"\n{strategy_labels.get(strategy, strategy)}:")
            for _, row in strategy_stats.iterrows():
                print(f"  {row['allocator']}: mean={row['mean']:.0f} cycles, std={row['std']:.0f} cycles, n={row['count']}")
                
        # Debug: Show which combinations have data
        print(f"\n=== Debug: Data combinations ===")
        for _, row in stats_df.iterrows():
            print(f"  {row['strategy']} + {row['allocator']}: {row['mean']:.0f} cycles")
        
        plt.show()


if __name__ == "__main__":
    csv_path = '../result_digested/meta_2022_overhead.csv'
    
    # Debug: Check what allocator values are actually in the data
    df_debug = pd.read_csv(csv_path)
    print("Unique allocator values in data:", df_debug['allocator'].unique())
    print("Unique rebalance_strategy values in data:", df_debug['rebalance_strategy'].unique())
    
    plot_rebalancer_cycles(csv_path)
