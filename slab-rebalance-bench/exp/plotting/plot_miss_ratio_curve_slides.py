import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os

# Add parent directory to path to import const
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *

def create_meta_kv_plot(csv_file, version="complete", trace_name="meta_202210_kv", output_dir="."):
    """
    Create line plot for Meta KV data showing miss ratio vs cache size.
    
    Args:
        csv_file: Path to the CSV file
        version: "disabled_only" for first slide, "complete" for second slide, "full_complete" for third slide, "with_tuned" for fourth slide, "with_lama" for fifth slide
        trace_name: Name of the trace to plot (meta_202210_kv, meta_202401_kv, or meta_memcache_2024_kv)
        output_dir: Directory to save the output plots (default: current directory)
    """
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Filter for specified trace and remove LAMA (but keep marginal-hits-tuned for slide 4)
    if version == "with_tuned":
        # For slide 4: include marginal-hits-tuned, exclude LAMA
        df = df[(df['trace_name'] == trace_name) & 
                (df['rebalance_strategy'] != 'lama')]
    elif version == "with_lama":
        # For slide 5: include all strategies including LAMA and marginal-hits-tuned
        df = df[df['trace_name'] == trace_name]
    else:
        # For other slides: exclude both LAMA and marginal-hits-tuned
        df = df[(df['trace_name'] == trace_name) & 
                (df['rebalance_strategy'] != 'lama') &
                (df['rebalance_strategy'] != 'marginal-hits-tuned')]
    
    # Calculate y-axis range from complete dataset (before any version filtering)
    # This ensures both versions have the same y-axis range
    y_min = df['miss_ratio'].min()
    y_max = df['miss_ratio'].max()
    y_range = y_max - y_min
    y_min_padded = max(0, y_min - 0.05 * y_range)
    y_max_padded = y_max + 0.05 * y_range
    
    # For disabled_only version, filter to only show disabled strategy
    if version == "disabled_only":
        df = df[df['rebalance_strategy'] == 'disabled']
    elif version == "complete":
        # For complete version: show only LRU with all strategies
        df = df[df['allocator'] == 'LRU']
    elif version == "with_tuned":
        # For slide 4: show all data (no additional filtering)
        pass
    elif version == "with_lama":
        # For slide 5: show all data including LAMA (no additional filtering)
        pass
    # For full_complete version, keep all data (no additional filtering)
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    # Create figure - use slightly taller height for slide 5 legends
    if version == "with_lama":
        fig, ax = plt.subplots(figsize=(12, 9))
    else:
        fig, ax = plt.subplots(figsize=(12, 8))
    
    # Strategy order for consistent legend ordering
    if version == "disabled_only":
        current_strategy_order = ["disabled"]
    elif version == "with_tuned":
        current_strategy_order = strategy_order
    elif version == "with_lama":
        current_strategy_order = strategy_order  # Include all strategies including LAMA
    else:
        # Filter out marginal-hits-tuned for other versions
        current_strategy_order = [s for s in strategy_order if s != "marginal-hits-tuned"]
    
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
            
            # Use standard marker and linestyle for all strategies (including marginal-hits-tuned)
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
    
    # Set y-axis limits to be consistent across both versions
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
    
    if version == "complete":
        # For slide 2: Only show LRU but maintain legend box size for 3 items
        allocator_legend_elements.append(plt.Line2D([0], [0], 
                                        color='black',
                                        marker=allocator_markers['LRU'],
                                        linestyle=allocator_linestyles['LRU'],
                                        linewidth=2,
                                        markersize=8,
                                        markeredgecolor='black',
                                        markeredgewidth=0.8,
                                        label=allocator_labels[0]))  # LRU
        # Add invisible placeholders to maintain legend box size
        for i in range(2):
            allocator_legend_elements.append(plt.Line2D([0], [0], 
                                            color='white',
                                            alpha=0,
                                            label=''))
    else:
        # For other versions: show all allocators that exist in data
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
    
    # Create separate legends based on version
    if version == "disabled_only":
        # Only show eviction policy legend for disabled_only version (aligned to right)
        if allocator_legend_elements:
            allocator_legend = ax.legend(handles=allocator_legend_elements,
                                       title="Eviction Policy", 
                                       bbox_to_anchor=(1.0, 1.20), loc='center right',
                                       ncol=1, frameon=True, fancybox=True, shadow=True, 
                                       framealpha=0.9, edgecolor='black')
            allocator_legend.get_frame().set_facecolor('white')
            allocator_legend.set_title("Eviction Policy", prop={'size': 22, 'weight': 'bold'})  # Increased from 16
    else:
        # Show both legends for complete, full_complete, and with_tuned versions
        # For all versions: use horizontal layout with legends at top
        # Add strategy legend (left side)
        if strategy_legend_elements:
            if version == "with_tuned":
                # For slide 4 with 6 strategies, use slide 3 positioning approach with more separation
                strategy_legend = ax.legend(handles=strategy_legend_elements, 
                                          title="Rebalancing Strategy",
                                          bbox_to_anchor=(-0.1, 1.25), loc='center left',
                                          ncol=2, frameon=True, fancybox=True, shadow=True, 
                                          framealpha=0.9, edgecolor='black')
            elif version == "with_lama":
                # For slide 5 with 7 strategies, use side-by-side layout like slide 4
                # Position strategy legend on left with 2 columns (4 rows), moved slightly higher
                strategy_legend = ax.legend(handles=strategy_legend_elements, 
                                          title="Rebalancing Strategy",
                                          bbox_to_anchor=(0.25, 1.35), loc='center',
                                          ncol=2, frameon=True, fancybox=True, shadow=True, 
                                          framealpha=0.9, edgecolor='black')
            else:
                # For other versions with 5 strategies, use 2 columns
                strategy_legend = ax.legend(handles=strategy_legend_elements, 
                                          title="Rebalancing Strategy",
                                          bbox_to_anchor=(0.0, 1.20), loc='center left',
                                          ncol=2, frameon=True, fancybox=True, shadow=True, 
                                          framealpha=0.9, edgecolor='black')
            strategy_legend.get_frame().set_facecolor('white')
            strategy_legend.set_title("Rebalancing Strategy", prop={'size': 22, 'weight': 'bold'})
        
        # Add allocator legend (right side)
        if allocator_legend_elements:
            if version == "with_tuned":
                # For slide 4: use slide 3 positioning approach with more separation
                allocator_legend = ax.legend(handles=allocator_legend_elements,
                                           title="Eviction Policy", 
                                           bbox_to_anchor=(1.1, 1.25), loc='center right',
                                           ncol=1, frameon=True, fancybox=True, shadow=True, 
                                           framealpha=0.9, edgecolor='black')
            elif version == "with_lama":
                # For slide 5: position eviction policy legend on right with smaller spacing
                # Keep good horizontal spacing, move slightly higher
                allocator_legend = ax.legend(handles=allocator_legend_elements,
                                           title="Eviction Policy", 
                                           bbox_to_anchor=(0.90, 1.35), loc='center',
                                           ncol=1, labelspacing=1.0, frameon=True, fancybox=True, shadow=True, 
                                           framealpha=0.9, edgecolor='black')
            else:
                # For other versions: use standard positioning
                allocator_legend = ax.legend(handles=allocator_legend_elements,
                                           title="Eviction Policy", 
                                           bbox_to_anchor=(1.0, 1.20), loc='center right',
                                           ncol=1, frameon=True, fancybox=True, shadow=True, 
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
    
    # Adjust layout to accommodate legends outside plot area
    plt.tight_layout()
    # For slide 4: adjust for higher legends like slide 3
    if version == "with_tuned":
        plt.subplots_adjust(top=0.80)
    elif version == "with_lama":
        # For slide 5: adjust for stacked legends in center with more space
        plt.subplots_adjust(top=0.70)
    else:
        # For other slides: standard adjustment for horizontal legends at top
        plt.subplots_adjust(top=0.80)
    
    # Save to PDF
    trace_suffix = trace_name.replace('meta_', '').replace('_kv', '')
    if version == "disabled_only":
        output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_slide1.pdf')
    elif version == "complete":
        output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_slide2.pdf')
    elif version == "with_tuned":
        output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_slide4.pdf')
    elif version == "with_lama":
        output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_slide5.pdf')
    else:  # full_complete
        output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_slide3.pdf')
    
    plt.savefig(output_file, format='pdf', dpi=300, bbox_inches='tight',
               facecolor='white', edgecolor='none')
    
    plt.show()
    print(f"Plot saved to: {output_file}")

def create_rebalanced_slabs_plot(csv_file, trace_name="meta_202210_kv", output_dir=".", include_tuned=False):
    """
    Create line plot for Meta KV data showing number of rebalanced slabs vs cache size.
    Uses the same layout as meta_kv_slide3.pdf (full_complete version).
    
    Args:
        csv_file: Path to the CSV file
        trace_name: Name of the trace to plot (meta_202210_kv, meta_202401_kv, or meta_memcache_2024_kv)
        output_dir: Directory to save the output plots (default: current directory)
        include_tuned: Whether to include marginal-hits-tuned strategy (default: False)
    """
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Filter for specified trace and remove LAMA (conditionally remove marginal-hits-tuned)
    if include_tuned:
        # Include marginal-hits-tuned, exclude only LAMA
        df = df[(df['trace_name'] == trace_name) & 
                (df['rebalance_strategy'] != 'lama')]
    else:
        # Exclude both LAMA and marginal-hits-tuned
        df = df[(df['trace_name'] == trace_name) & 
                (df['rebalance_strategy'] != 'lama') &
                (df['rebalance_strategy'] != 'marginal-hits-tuned')]
    
    # Calculate y-axis range from complete dataset
    y_min = df['n_rebalanced_slabs'].min()
    y_max = df['n_rebalanced_slabs'].max()
    y_range = y_max - y_min
    y_min_padded = max(0, y_min - 0.05 * y_range)
    y_max_padded = y_max + 0.05 * y_range
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Strategy order for consistent legend ordering (conditionally filter marginal-hits-tuned)
    if include_tuned:
        current_strategy_order = [s for s in strategy_order if s != "lama"]
    else:
        current_strategy_order = [s for s in strategy_order if s not in ["marginal-hits-tuned", "lama"]]
    
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
            
            # Plot line with markers
            ax.plot(x_values, y_values, 
                   color=strategy_colors[strategy],
                   marker=allocator_markers[allocator],
                   linestyle=allocator_linestyles[allocator],
                   linewidth=2.5,
                   markersize=8,
                   markeredgecolor='black',
                   markeredgewidth=0.8,
                   label=f"{strategy_labels[strategy]} + {allocator_labels[allocator_order.index(allocator)]}")
    
    # Customize the plot
    ax.set_xlabel('Cache Size (% of Working Set)')
    ax.set_ylabel('Number of Rebalanced Slabs')  # Changed y-axis label
    ax.grid(True, alpha=0.3)
    
    # Set y-axis limits to be consistent
    ax.set_ylim(y_min_padded, y_max_padded)
    
    # Set categorical x-axis with proper labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels(wsr_labels)
    
    # Create legend elements (same as original)
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
    
    # Show both legends (adjusted positioning to prevent overlap)
    # Add strategy legend (left side)
    if strategy_legend_elements:
        strategy_legend = ax.legend(handles=strategy_legend_elements, 
                                  title="Rebalancing Strategy",
                                  bbox_to_anchor=(-0.1, 1.25), loc='center left',
                                  ncol=2, frameon=True, fancybox=True, shadow=True, 
                                  framealpha=0.9, edgecolor='black')
        strategy_legend.get_frame().set_facecolor('white')
        strategy_legend.set_title("Rebalancing Strategy", prop={'size': 22, 'weight': 'bold'})
    
    # Add allocator legend (right side)
    if allocator_legend_elements:
        allocator_legend = ax.legend(handles=allocator_legend_elements,
                                   title="Eviction Policy", 
                                   bbox_to_anchor=(1.1, 1.25), loc='center right',
                                   ncol=1, frameon=True, fancybox=True, shadow=True, 
                                   framealpha=0.9, edgecolor='black')
        allocator_legend.get_frame().set_facecolor('white')
        allocator_legend.set_title("Eviction Policy", prop={'size': 22, 'weight': 'bold'})
        # Add the strategy legend back as an artist so both show
        if strategy_legend_elements:
            ax.add_artist(strategy_legend)
    
    # Style the plot (same as original)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)
    
    # Adjust layout to accommodate legends outside plot area
    plt.tight_layout()
    plt.subplots_adjust(top=0.80)
    
    # Save to PDF
    trace_suffix = trace_name.replace('meta_', '').replace('_kv', '')
    if include_tuned:
        output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_rebalanced_slabs_with_tuned.pdf')
    else:
        output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_rebalanced_slabs.pdf')
    plt.savefig(output_file, format='pdf', dpi=300, bbox_inches='tight',
               facecolor='white', edgecolor='none')
    
    plt.show()
    print(f"Plot saved to: {output_file}")

def create_rebalanced_slabs_all_plot(csv_file, trace_name="meta_202210_kv", output_dir="."):
    """
    Create line plot for Meta KV data showing number of rebalanced slabs vs cache size.
    Uses the same layout as slide 5 (with_lama version) - includes all strategies including LAMA.
    
    Args:
        csv_file: Path to the CSV file
        trace_name: Name of the trace to plot (meta_202210_kv, meta_202401_kv, or meta_memcache_2024_kv)
        output_dir: Directory to save the output plots (default: current directory)
    """
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Filter for specified trace - include all strategies including LAMA (like slide 5)
    df = df[df['trace_name'] == trace_name]
    
    # Calculate y-axis range from complete dataset
    y_min = df['n_rebalanced_slabs'].min()
    y_max = df['n_rebalanced_slabs'].max()
    y_range = y_max - y_min
    y_min_padded = max(0, y_min - 0.05 * y_range)
    y_max_padded = y_max + 0.05 * y_range
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    # Create figure - use same size as slide 5
    fig, ax = plt.subplots(figsize=(12, 9))
    
    # Strategy order for consistent legend ordering - include all strategies like slide 5
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
    
    # Create legend elements - same as slide 5
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
    
    # Create legends using same layout as slide 5 (with_lama)
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
    
    # Adjust layout to accommodate legends - same as slide 5
    plt.tight_layout()
    plt.subplots_adjust(top=0.70)
    
    # Save to PDF
    trace_suffix = trace_name.replace('meta_', '').replace('_kv', '')
    output_file = os.path.join(output_dir, f'meta_kv_{trace_suffix}_rebalanced_slabs_all.pdf')
    plt.savefig(output_file, format='pdf', dpi=300, bbox_inches='tight',
               facecolor='white', edgecolor='none')
    
    plt.show()
    print(f"Plot saved to: {output_file}")

# Example usage
if __name__ == "__main__":
    # Configuration
    data_path = "../result/efficiency_result_processed.csv"
    output_dir = "figures/metaKV_slides"
    
    # List of traces to process
    trace_names = ['meta_202210_kv', 'meta_202401_kv', 'meta_memcache_2024_kv']
    
    for trace_name in trace_names:
        print(f"\n=== PROCESSING TRACE: {trace_name} ===")
        
        # Data analysis: Find best LRU performance for this trace
        print(f"=== DATA ANALYSIS: LRU Miss Ratio Reduction for {trace_name} ===")
        
        # Read and filter data for analysis
        df_analysis = pd.read_csv(data_path)
        df_analysis = df_analysis[(df_analysis['trace_name'] == trace_name) & 
                                 (df_analysis['rebalance_strategy'] != 'lama') &
                                 (df_analysis['rebalance_strategy'] != 'marginal-hits-tuned') &
                                 (df_analysis['allocator'] == 'LRU') &
                                 (df_analysis['miss_ratio_reduction_from_disabled'].notna())]
        
        if not df_analysis.empty:
            # Find the row with maximum miss ratio reduction
            best_row = df_analysis.loc[df_analysis['miss_ratio_reduction_from_disabled'].idxmax()]
            
            print(f"Best LRU configuration for {trace_name}:")
            print(f"  Strategy: {best_row['rebalance_strategy']}")
            print(f"  WSR: {best_row['wsr']:.4f} ({best_row['wsr']*100:.1f}% of working set)")
            print(f"  Miss Ratio Reduction: {best_row['miss_ratio_reduction_from_disabled']:.6f}")
            print(f"  Absolute Miss Ratio: {best_row['miss_ratio']:.6f}")
            
            # Show breakdown by strategy
            print(f"\n=== Miss Ratio Reduction by Strategy (all WSR values) for {trace_name} ===")
            strategy_summary = df_analysis.groupby('rebalance_strategy')['miss_ratio_reduction_from_disabled'].agg(['max', 'mean', 'min']).round(6)
            print(strategy_summary)
            
            # Show top 3 configurations for this trace
            print(f"\n=== Top 3 LRU Configurations for {trace_name} ===")
            top3 = df_analysis.nlargest(3, 'miss_ratio_reduction_from_disabled')[['rebalance_strategy', 'wsr', 'miss_ratio_reduction_from_disabled', 'miss_ratio']]
            top3['wsr_percent'] = top3['wsr'] * 100
            print(top3[['rebalance_strategy', 'wsr_percent', 'miss_ratio_reduction_from_disabled', 'miss_ratio']].round(6))
        else:
            print(f"No valid LRU data found for {trace_name}!")
        
        print(f"\n=== GENERATING PLOTS for {trace_name} ===")
        
        # Generate all four versions for presentation
        print(f"Generating disabled-only version (slide 1) for {trace_name}...")
        create_meta_kv_plot(data_path, version="disabled_only", trace_name=trace_name, output_dir=output_dir)
        
        print(f"Generating LRU-focused version (slide 2) for {trace_name}...")
        create_meta_kv_plot(data_path, version="complete", trace_name=trace_name, output_dir=output_dir)
        
        print(f"Generating full complete version (slide 3) for {trace_name}...")
        create_meta_kv_plot(data_path, version="full_complete", trace_name=trace_name, output_dir=output_dir)
        
        print(f"Generating version with marginal-hits-tuned (slide 4) for {trace_name}...")
        create_meta_kv_plot(data_path, version="with_tuned", trace_name=trace_name, output_dir=output_dir)
        
        print(f"Generating version with LAMA (slide 5) for {trace_name}...")
        create_meta_kv_plot(data_path, version="with_lama", trace_name=trace_name, output_dir=output_dir)
        
        print(f"Generating rebalanced slabs plot for {trace_name}...")
        create_rebalanced_slabs_plot(data_path, trace_name=trace_name, output_dir=output_dir)
        
        print(f"Generating rebalanced slabs plot with tuned for {trace_name}...")
        create_rebalanced_slabs_plot(data_path, trace_name=trace_name, output_dir=output_dir, include_tuned=True)
        
        print(f"Generating rebalanced slabs plot with all strategies (slide 5 layout) for {trace_name}...")
        create_rebalanced_slabs_all_plot(data_path, trace_name=trace_name, output_dir=output_dir)