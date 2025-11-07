#!/usr/bin/env python3
"""
Script to generate demo figure
Takes trace name and input CSV file path as command line parameters.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import argparse

# Add parent directory to path to import const
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *

def create_meta_kv_demo(csv_file, trace_name, output_dir="."):
    """
    
    Args:
        csv_file: Path to the CSV file
        trace_name: Name of the trace to plot
        output_dir: Directory to save the output plots (default: current directory)
    """
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Filter for specified trace - include all strategies including LAMA
    df = df[df['trace_name'] == trace_name]
    
    if df.empty:
        print(f"Warning: No data found for trace '{trace_name}'")
        return
    
    # Calculate y-axis range from complete dataset
    y_min = df['miss_ratio'].min()
    y_max = df['miss_ratio'].max()
    y_range = y_max - y_min
    y_min_padded = max(0, y_min - 0.05 * y_range)
    y_max_padded = y_max + 0.05 * y_range
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    fig, ax = plt.subplots(figsize=(12, 9))
    
    # Strategy order for consistent legend ordering - include all strategies including LAMA
    current_strategy_order = strategy_order  # Include all strategies including LAMA
    
    # Get unique WSR values and sort them to preserve numerical order
    unique_wsr = sorted(df['wsr'].unique())
    wsr_labels = [f"{wsr*100:.1f}" for wsr in unique_wsr]  # Convert to percentage labels
    x_positions = range(len(unique_wsr))  # Categorical positions
    
    # Plot lines for each strategy-allocator combination
    for strategy in current_strategy_order:
        if strategy not in df['rebalance_strategy'].values:
            continue
            
        for allocator in allocator_order:
            if allocator not in df['allocator'].values:
                continue
                
            # Filter data for this strategy-allocator combination
            subset = df[(df['rebalance_strategy'] == strategy) & (df['allocator'] == allocator)]
            
            if subset.empty:
                continue
            
            # Sort by WSR for proper line connection
            subset = subset.sort_values('wsr')
            
            # Map WSR values to categorical positions
            x_values = [x_positions[unique_wsr.index(wsr)] for wsr in subset['wsr']]
            y_values = subset['miss_ratio']
            
            # Use standard marker and linestyle for all strategies
            marker = allocator_markers[allocator]
            linestyle = allocator_linestyles[allocator]
            
            # Plot line with markers
            ax.plot(x_values, y_values, 
                   color=strategy_colors[strategy],
                   marker=marker,
                   linestyle=linestyle,
                   linewidth=2.5,
                   markersize=8,
                   markeredgecolor='black',
                   markeredgewidth=0.8,
                   label=f"{strategy_labels[strategy]} + {allocator_labels[allocator_order.index(allocator)]}")
    
    # Customize the plot
    ax.set_xlabel('Cache Size (% of Working Set)')
    ax.set_ylabel('Miss Ratio')
    ax.grid(True, alpha=0.3)
    
    # Set y-axis limits
    ax.set_ylim(y_min_padded, y_max_padded)
    
    # Set categorical x-axis with proper labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels(wsr_labels)
    
    # Create legend elements
    # Strategy legend
    strategy_legend_elements = []
    strategies_in_data = set(df['rebalance_strategy'].unique())
    
    for strategy in current_strategy_order:
        if strategy in strategies_in_data:
            strategy_legend_elements.append(plt.Line2D([0], [0], 
                                           color=strategy_colors[strategy], 
                                           linewidth=3,
                                           label=strategy_labels[strategy]))
    
    # Allocator legend  
    allocator_legend_elements = []
    allocators_in_data = set(df['allocator'].unique())
    
    for allocator in allocator_order:
        if allocator in allocators_in_data:
            allocator_legend_elements.append(plt.Line2D([0], [0], 
                                            color='black',
                                            marker=allocator_markers[allocator],
                                            linestyle=allocator_linestyles[allocator],
                                            linewidth=2,
                                            markersize=8,
                                            markeredgecolor='black',
                                            markeredgewidth=0.8,
                                            label=allocator_labels[allocator_order.index(allocator)]))
    
    # Add strategy legend (left side)
    if strategy_legend_elements:
        strategy_legend = ax.legend(handles=strategy_legend_elements, 
                                  title="Rebalancing Strategy",
                                  bbox_to_anchor=(0.25, 1.35), loc='center',
                                  ncol=2, frameon=True, fancybox=True, shadow=True, 
                                  framealpha=0.9, edgecolor='black')
        strategy_legend.get_frame().set_facecolor('white')
        strategy_legend.set_title("Rebalancing Strategy", prop={'size': 22, 'weight': 'bold'})
    
    # Add allocator legend (right side)
    if allocator_legend_elements:
        allocator_legend = ax.legend(handles=allocator_legend_elements,
                                   title="Eviction Policy", 
                                   bbox_to_anchor=(0.90, 1.35), loc='center',
                                   ncol=1, labelspacing=1.0, frameon=True, fancybox=True, shadow=True, 
                                   framealpha=0.9, edgecolor='black')
        allocator_legend.get_frame().set_facecolor('white')
        allocator_legend.set_title("Eviction Policy", prop={'size': 22, 'weight': 'bold'})
        # Add the strategy legend back as an artist so both show
        if strategy_legend_elements:
            ax.add_artist(strategy_legend)
    
    # Style the plot
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)
    
    # Adjust layout to accommodate legends
    plt.tight_layout()
    plt.subplots_adjust(top=0.70)
    
    # Save to PDF
    trace_suffix = trace_name.replace('meta_', '').replace('_kv', '')
    output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}.pdf')
    
    plt.savefig(output_file, format='pdf', dpi=300, bbox_inches='tight',
               facecolor='white', edgecolor='none')
    
    plt.show()
    print(f"Plot saved to: {output_file}")

def create_rebalanced_slabs_demo(csv_file, trace_name, output_dir="."):
    """
    
    Args:
        csv_file: Path to the CSV file
        trace_name: Name of the trace to plot
        output_dir: Directory to save the output plots (default: current directory)
    """
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Filter for specified trace - include all strategies including LAMA
    df = df[df['trace_name'] == trace_name]
    
    if df.empty:
        print(f"Warning: No data found for trace '{trace_name}'")
        return
    
    # Calculate y-axis range from complete dataset
    y_min = df['n_rebalanced_slabs'].min()
    y_max = df['n_rebalanced_slabs'].max()
    y_range = y_max - y_min
    y_min_padded = max(0, y_min - 0.05 * y_range)
    y_max_padded = y_max + 0.05 * y_range
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    fig, ax = plt.subplots(figsize=(12, 9))
    
    # Strategy order for consistent legend ordering - include all strategies including LAMA
    current_strategy_order = strategy_order  # Include all strategies including LAMA
    
    # Get unique WSR values and sort them to preserve numerical order
    unique_wsr = sorted(df['wsr'].unique())
    wsr_labels = [f"{wsr*100:.1f}" for wsr in unique_wsr]  # Convert to percentage labels
    x_positions = range(len(unique_wsr))  # Categorical positions
    
    # Plot lines for each strategy-allocator combination
    for strategy in current_strategy_order:
        if strategy not in df['rebalance_strategy'].values:
            continue
            
        for allocator in allocator_order:
            if allocator not in df['allocator'].values:
                continue
                
            # Filter data for this strategy-allocator combination
            subset = df[(df['rebalance_strategy'] == strategy) & (df['allocator'] == allocator)]
            
            if subset.empty:
                continue
            
            # Sort by WSR for proper line connection
            subset = subset.sort_values('wsr')
            
            # Map WSR values to categorical positions
            x_values = [x_positions[unique_wsr.index(wsr)] for wsr in subset['wsr']]
            y_values = subset['n_rebalanced_slabs']
            
            # Use standard marker and linestyle for all strategies
            marker = allocator_markers[allocator]
            linestyle = allocator_linestyles[allocator]
            
            # Plot line with markers
            ax.plot(x_values, y_values, 
                   color=strategy_colors[strategy],
                   marker=marker,
                   linestyle=linestyle,
                   linewidth=2.5,
                   markersize=8,
                   markeredgecolor='black',
                   markeredgewidth=0.8,
                   label=f"{strategy_labels[strategy]} + {allocator_labels[allocator_order.index(allocator)]}")
    
    # Customize the plot
    ax.set_xlabel('Cache Size (% of Working Set)')
    ax.set_ylabel('Number of Rebalanced Slabs')
    ax.grid(True, alpha=0.3)
    
    # Set y-axis limits
    ax.set_ylim(y_min_padded, y_max_padded)
    
    # Set categorical x-axis with proper labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels(wsr_labels)
    
    # Strategy legend
    strategy_legend_elements = []
    strategies_in_data = set(df['rebalance_strategy'].unique())
    
    for strategy in current_strategy_order:
        if strategy in strategies_in_data:
            strategy_legend_elements.append(plt.Line2D([0], [0], 
                                           color=strategy_colors[strategy], 
                                           linewidth=3,
                                           label=strategy_labels[strategy]))
    
    # Allocator legend  
    allocator_legend_elements = []
    allocators_in_data = set(df['allocator'].unique())
    
    for allocator in allocator_order:
        if allocator in allocators_in_data:
            allocator_legend_elements.append(plt.Line2D([0], [0], 
                                            color='black',
                                            marker=allocator_markers[allocator],
                                            linestyle=allocator_linestyles[allocator],
                                            linewidth=2,
                                            markersize=8,
                                            markeredgecolor='black',
                                            markeredgewidth=0.8,
                                            label=allocator_labels[allocator_order.index(allocator)]))
    
    # Add strategy legend (left side)
    if strategy_legend_elements:
        strategy_legend = ax.legend(handles=strategy_legend_elements, 
                                  title="Rebalancing Strategy",
                                  bbox_to_anchor=(0.25, 1.35), loc='center',
                                  ncol=2, frameon=True, fancybox=True, shadow=True, 
                                  framealpha=0.9, edgecolor='black')
        strategy_legend.get_frame().set_facecolor('white')
        strategy_legend.set_title("Rebalancing Strategy", prop={'size': 22, 'weight': 'bold'})
    
    # Add allocator legend (right side)
    if allocator_legend_elements:
        allocator_legend = ax.legend(handles=allocator_legend_elements,
                                   title="Eviction Policy", 
                                   bbox_to_anchor=(0.90, 1.35), loc='center',
                                   ncol=1, labelspacing=1.0, frameon=True, fancybox=True, shadow=True, 
                                   framealpha=0.9, edgecolor='black')
        allocator_legend.get_frame().set_facecolor('white')
        allocator_legend.set_title("Eviction Policy", prop={'size': 22, 'weight': 'bold'})
        # Add the strategy legend back as an artist so both show
        if strategy_legend_elements:
            ax.add_artist(strategy_legend)
    
    # Style the plot
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.70)
    
    # Save to PDF
    trace_suffix = trace_name.replace('meta_', '').replace('_kv', '')
    output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_rebalanced_slabs.pdf')
    plt.savefig(output_file, format='pdf', dpi=300, bbox_inches='tight',
               facecolor='white', edgecolor='none')
    
    plt.show()
    print(f"Plot saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate miss ratio curves')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('trace_name', help='Name of the trace to plot (e.g., meta_202210_kv)')
    parser.add_argument('-o', '--output-dir', default='.', help='Output directory for plots (default: current directory)')
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        print(f"Created output directory: {args.output_dir}")
    
    print(f"Processing trace: {args.trace_name}")
    print(f"Input file: {args.input_file}")
    print(f"Output directory: {args.output_dir}")
    
    print("\nGenerating miss ratio plot ...")
    create_meta_kv_demo(args.input_file, args.trace_name, args.output_dir)
    
    print("\nGenerating rebalanced slabs plot...")
    create_rebalanced_slabs_demo(args.input_file, args.trace_name, args.output_dir)

    print(f"\nDone! Generated plots for {args.trace_name}")

if __name__ == "__main__":
    main()
