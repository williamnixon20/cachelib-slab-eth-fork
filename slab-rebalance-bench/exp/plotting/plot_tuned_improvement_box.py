"""
Plot boxplots showing the improvement from marginal-hits to marginal-hits-tuned strategy.
Three eviction policies on x-axis, tuning improvement on y-axis.
Creates subplots for WSR 0.01 and 0.1, exports to PDF with publication quality.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from const import (allocator_order, allocator_labels, rcParams)

def plot_tuned_improvement(df, output_dir, ratio=False):
    """
    Plot boxplots showing miss ratio improvement from marginal-hits to marginal-hits-tuned.
    Creates separate PDF files for each WSR value.
    
    Args:
        df: DataFrame with columns including 'allocator', 'wsr', 'tuned_improvement'
        output_dir: Directory to save the plots
        ratio (bool): If True, use tuned_percent_improvement instead of tuned_improvement. Default False.
    """
    
    # Filter Twitter production data and remove NaN values from tuned_improvement
    df = df[(df['trace_name'].str.startswith('twitter_cluster')) & (df['tag'] != 'warm-cold')]
    
    # Choose the appropriate column based on ratio parameter
    metric_column = 'tuned_percent_improvement' if ratio else 'tuned_improvement'
    
    df = df.dropna(subset=[metric_column])  # Remove rows where the metric is NaN
    
    # Debug: Print data info
    print(f"Total filtered data points: {len(df)}")
    print(f"Available columns: {df.columns.tolist()}")
    print(f"Unique WSR values: {df['wsr'].unique() if 'wsr' in df.columns else 'WSR column not found'}")
    print(f"Unique allocators: {df['allocator'].unique() if 'allocator' in df.columns else 'allocator column not found'}")
    if metric_column in df.columns:
        print(f"{metric_column} range: {df[metric_column].min()} to {df[metric_column].max()}")
        print(f"Non-null {metric_column} values: {df[metric_column].notna().sum()}")
    else:
        print(f"{metric_column} column not found")
    
    # Set up the plotting style for publication quality
    plt.rcParams.update(rcParams)
    
    # Create separate plots for each WSR value
    wsr_values = [0.01, 0.1]
    
    for wsr in wsr_values:
        # Create individual figure for this WSR
        fig, ax = plt.subplots(1, 1, figsize=(9, 6))
        
        df_wsr = df[df['wsr'] == wsr]
        print(f"\nWSR {wsr}: {len(df_wsr)} data points")
        
        # Prepare data for boxplot
        boxplot_data = []
        positions = []
        labels = []
        
        for j, allocator in enumerate(allocator_order):
            allocator_data = df_wsr[df_wsr['allocator'] == allocator]
            if len(allocator_data) > 0:
                data = allocator_data[metric_column].values
                # Double-check for NaN values
                data = data[~np.isnan(data)]
                if len(data) > 0:
                    print(f"  {allocator}: {len(data)} points, range {data.min():.6f} to {data.max():.6f}, mean: {data.mean():.6f}")
                    boxplot_data.append(data)
                    positions.append(j)
                    labels.append(allocator_labels[j])
                else:
                    print(f"  {allocator}: {len(allocator_data)} total points, but all NaN after filtering")
            else:
                print(f"  {allocator}: No data after WSR filtering")
        
        # Create boxplot
        if boxplot_data:
            bp = ax.boxplot(boxplot_data, positions=positions, patch_artist=True,
                          boxprops=dict(facecolor='lightblue', alpha=0.7),
                          medianprops=dict(color='red', linewidth=2),
                          meanprops=dict(marker='v', markerfacecolor='red', markeredgecolor='red', markersize=8),
                          showmeans=True,
                          showfliers=False,  # Don't show outlier points
                          whis=1.5)         # Whiskers extend to 1.5×IQR (traditional rule)
        else:
            print(f"No data available for WSR {wsr}")
            plt.close(fig)
            continue
        
        # Customize the subplot
        ax.set_xlabel('Eviction Policy')
        ax.set_ylabel('Miss Ratio Reduction\nover ' + r'$\mathit{Marginal\text{-}Hits}$')
        ax.set_xticks(range(len(allocator_order)))
        ax.set_xticklabels(allocator_labels)
        ax.grid(True, alpha=0.3)
        
        # Add horizontal line at y=0 for reference
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.5, linewidth=1)
        
        plt.tight_layout()
        
        # Save individual plot
        suffix = "_percent" if ratio else ""
        output_path = os.path.join(output_dir, f'twitter_prod_tuned_improvement_wsr_{wsr}{suffix}.pdf')
        plt.savefig(output_path, format='pdf', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"Plot saved to: {output_path}")

if __name__ == "__main__":
    # Load data (adjust path as needed)
    data_path = "../result/efficiency_result_processed.csv"
    df = pd.read_csv(data_path)
    
    # Set output directory
    output_dir = "figures/tuned_improvement"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create the plot
    plot_tuned_improvement(df, output_dir, ratio=False)
    plot_tuned_improvement(df, output_dir, ratio=True)