import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from const import (strategy_order, strategy_labels, strategy_colors, 
                   allocator_order, allocator_labels, rcParams)

def create_twitter_prod_worst_case_mr_barplots(csv_file, output_dir=None, ratio=False):
    """
    Create separate bar plots for each eviction policy showing worst-case miss ratio reduction from disabled.
    
    Args:
        csv_file (str): Path to the CSV file containing the data
        output_dir (str): Directory to save output plots. If None, uses current directory.
        ratio (bool): If True, use miss_ratio_percent_reduction_from_disabled instead 
                     of miss_ratio_reduction_from_disabled. Default False.
    """
    
    # Set output directory
    if output_dir is None:
        output_dir = os.getcwd()
    elif not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Choose the appropriate column based on ratio parameter
    metric_column = 'miss_ratio_percent_reduction_from_disabled' if ratio else 'miss_ratio_reduction_from_disabled'
    
    df = df[(df['trace_name'].str.startswith('twitter')) & (df['tag'] != 'warm-cold') & df[metric_column].notna()]
    df = df[df['rebalance_strategy'] != 'disabled']
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    # Create plots for both WSR values and each allocator
    wsr_values = [0.1, 0.01]
    
    for wsr in wsr_values:
        # Filter data for current WSR
        wsr_data = df[df['wsr'] == wsr].copy()
        
        if wsr_data.empty:
            print(f"No data found for WSR = {wsr}")
            continue
        
        # Create separate plot for each allocator (eviction policy)
        for i, allocator in enumerate(allocator_order):
            allocator_data = wsr_data[wsr_data['allocator'] == allocator]
            
            if allocator_data.empty:
                print(f"WSR {wsr}: No data for {allocator}")
                continue
            
            print(f"WSR {wsr}, {allocator}: {len(allocator_data)} rows, strategies: {list(allocator_data['rebalance_strategy'].unique())}")
            
            # Create figure for this allocator
            fig, ax = plt.subplots(figsize=(14, 10))
            
            # Prepare data for bar plots
            bar_data = []
            bar_labels = []
            bar_colors = []
            positions = []
            
            # Filter to only non-disabled strategies
            available_strategies = [s for s in strategy_order if s != 'disabled']
            
            pos = 0
            for strategy in available_strategies:
                strategy_data = allocator_data[allocator_data['rebalance_strategy'] == strategy]
                
                if not strategy_data.empty:
                    values = strategy_data[metric_column].values
                    
                    # Check for NaN values
                    if np.any(np.isnan(values)):
                        print(f"  WARNING: {strategy} has NaN values!")
                        continue
                    
                    # Calculate worst case (minimum value, which is most negative = worst performance)
                    worst_case = values.min()
                    
                    print(f"  {strategy}: worst case miss ratio reduction = {worst_case:.6f}")
                    
                    bar_data.append(worst_case)
                    bar_labels.append(strategy_labels[strategy])
                    bar_colors.append(strategy_colors[strategy])
                    positions.append(pos)
                    pos += 1
                    print(f"  Added bar for {strategy}")
                else:
                    print(f"  No data for {strategy}")
            
            if not bar_data:
                print(f"No data to plot for {allocator} at WSR {wsr}")
                plt.close(fig)
                continue
            
            # Create bar plots
            bars = ax.bar(positions, bar_data, color=bar_colors, alpha=1, 
                         edgecolor='black', linewidth=1, width=0.6)
            
            # Set x-axis labels
            ax.set_xticks(positions)
            ax.set_xticklabels(bar_labels, rotation=20, ha='left', fontsize=plt.rcParams['font.size']-1)
            
            # Move x-axis labels to the top since all values are negative
            ax.xaxis.tick_top()
            ax.xaxis.set_label_position('top')
            
            # Customize the plot
            ax.set_xlabel('Rebalance Strategy')
            ax.set_ylabel('Worst-Case Miss Ratio Reduction\nover Disabled')
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add a horizontal line at y=0 for reference
            ax.axhline(y=0, color='black', linestyle='-', alpha=0.3, linewidth=1)
            
            # Style the plot
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['left'].set_linewidth(1.2)
            ax.spines['bottom'].set_linewidth(1.2)
            
            # Adjust layout
            plt.tight_layout()
            
            # Save to PDF in the specified output directory
            suffix = "_percent" if ratio else ""
            allocator_name = allocator.lower().replace('/', '_')
            output_file = os.path.join(output_dir, f'twitter_prod_worst_case_{allocator_name}_wsr_{wsr:.2f}{suffix}.pdf')
            plt.savefig(output_file, format='pdf', dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            
            plt.show()
            print(f"Plot saved to: {output_file}")

# Example usage
if __name__ == "__main__":
    csv_file = "../result/efficiency_result_processed.csv"
    output_dir = "figures/twitter_worst_case"
    create_twitter_prod_worst_case_mr_barplots(csv_file, output_dir, ratio=False)
    create_twitter_prod_worst_case_mr_barplots(csv_file, output_dir, ratio=True)
