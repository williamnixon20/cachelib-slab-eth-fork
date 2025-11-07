import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os

# Set publication-quality matplotlib parameters
plt.rcParams.update({
    'font.size': 22,
    'axes.labelsize': 26,
    'axes.titlesize': 28,
    'xtick.labelsize': 22,
    'ytick.labelsize': 22,
    'legend.fontsize': 22,
    'figure.titlesize': 30,
    'lines.linewidth': 2.5,
    'lines.markersize': 8,
    'figure.figsize': (12, 8),
    'axes.linewidth': 1.5,
    'xtick.major.width': 1.5,
    'ytick.major.width': 1.5,
    'xtick.minor.width': 1,
    'ytick.minor.width': 1,
    'legend.frameon': True,
    'legend.fancybox': True,
    'legend.shadow': True,
    'legend.edgecolor': 'black',
    'legend.facecolor': 'white',
    'legend.framealpha': 0.9
})

def plot_lama_analysis(trace_name, csv_path='../data/end-to-end/report_lama_detailed_processed.csv', output_dir='.', categorical_x=False):
    """
    Plot LAMA analysis comparing marginal-hits-tuned and lama strategies
    with different footprintBufferSize configurations.
    
    Args:
        trace_name: Name of the trace to analyze (e.g., 'meta_202210_kv')
        csv_path: Path to the CSV file containing the data
        output_dir: Directory to save the plots
        categorical_x: If True, treat x-axis as categorical with numeric ordering (default: False)
    """
    
    # Read the data
    print(f"Reading data from {csv_path}")
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows")
    
    # Filter the data
    # Step 1: Filter by allocator and rebalance_strategy
    filtered_df = df[
        (df['allocator'] == 'LRU') & 
        (df['rebalance_strategy'].isin(['lama', 'marginal-hits-tuned']))
    ].copy()
    
    print(f"After filtering by allocator=LRU and rebalance_strategy: {len(filtered_df)} rows")
    
    # Step 2: Handle null footprintBufferSize (fill with 20,000,000)
    filtered_df['footprintBufferSize'] = filtered_df['footprintBufferSize'].fillna(20000000)
    
    # Step 3: Filter by trace_name
    filtered_df = filtered_df[filtered_df['trace_name'] == trace_name]
    print(f"After filtering by trace_name={trace_name}: {len(filtered_df)} rows")
    
    if len(filtered_df) == 0:
        print(f"No data found for trace_name={trace_name}")
        return
    
    # Step 4: Create strategy labels with buffer size information
    def create_strategy_label(row):
        if row['rebalance_strategy'] == 'lama':
            buffer_size = int(row['footprintBufferSize'])
            if buffer_size == 20000000:
                return 'lama-20m-buffer'
            elif buffer_size == 40000000:
                return 'lama-40m-buffer'
            elif buffer_size == 80000000:
                return 'lama-80m-buffer'
            elif buffer_size == 200000000:
                return None  # Filter out 200m buffer size
            else:
                return f'lama-{buffer_size//1000000}m-buffer'
        else:
            return row['rebalance_strategy']
    
    # Create display labels for legend
    def create_display_label(strategy_label):
        if strategy_label == 'marginal-hits-tuned':
            return r'$\mathit{Marginal\text{-}Hits\text{-}Tuned}$'
        elif strategy_label.startswith('lama-'):
            # Extract buffer size from strategy label
            parts = strategy_label.split('-')
            if len(parts) >= 2:
                buffer_size = parts[1].upper()  # Convert to uppercase (20M, 40M, etc.)
                return rf'$\mathit{{LAMA\text{{-}}{buffer_size}\text{{-}}Buffer}}$'
        return strategy_label
    
    filtered_df['strategy_label'] = filtered_df.apply(create_strategy_label, axis=1)
    
    # Filter out rows with None strategy_label (200m buffer size)
    filtered_df = filtered_df[filtered_df['strategy_label'].notna()]
    
    # Convert WSR to percentage
    filtered_df['wsr_percentage'] = filtered_df['wsr'] * 100
    
    # Filter to keep only WSR values that have data for all strategies
    unique_strategies = sorted(filtered_df['strategy_label'].unique())
    print(f"Found strategies: {unique_strategies}")
    
    # Define desired legend order
    desired_order = ['lama-20m-buffer', 'lama-40m-buffer', 'lama-80m-buffer', 'marginal-hits-tuned']
    # Reorder strategies to match desired legend order
    ordered_strategies = [s for s in desired_order if s in unique_strategies]
    # Add any remaining strategies not in the desired order at the end
    ordered_strategies.extend([s for s in unique_strategies if s not in desired_order])
    unique_strategies = ordered_strategies
    
    if len(unique_strategies) > 1:
        # Find WSR values that have data for all strategies
        wsr_strategy_counts = filtered_df.groupby('wsr_percentage')['strategy_label'].nunique()
        complete_wsr_values = wsr_strategy_counts[wsr_strategy_counts == len(unique_strategies)].index
        
        print(f"WSR values with data for all {len(unique_strategies)} strategies: {len(complete_wsr_values)} out of {len(wsr_strategy_counts)} total WSR values")
        
        # Filter to keep only complete WSR values
        filtered_df = filtered_df[filtered_df['wsr_percentage'].isin(complete_wsr_values)]
        print(f"After filtering for complete WSR coverage: {len(filtered_df)} rows")
        
        if len(filtered_df) == 0:
            print(f"No WSR values found with data for all strategies")
            return
    
    # Create the plot
    os.makedirs(output_dir, exist_ok=True)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Define colors and styles for different strategies
    strategy_styles = {
        'marginal-hits-tuned': {'color': '#1f77b4', 'linestyle': '-', 'marker': 'o'},  # Blue, solid line, circle
        'lama-20m-buffer': {'color': '#ff7f0e', 'linestyle': '--', 'marker': 's'},  # Orange, dashed line, square
        'lama-40m-buffer': {'color': '#2ca02c', 'linestyle': '-.', 'marker': '^'},  # Green, dash-dot line, triangle
        'lama-80m-buffer': {'color': '#d62728', 'linestyle': ':', 'marker': 'D'},  # Red, dotted line, diamond
    }
    
    # Plot each strategy
    print(f"Plotting {len(unique_strategies)} strategies with complete WSR coverage")
    
    # Get unique WSR values and sort them for categorical x-axis
    unique_wsr_values = sorted(filtered_df['wsr_percentage'].unique())
    wsr_labels = [f"{wsr:.1f}" for wsr in unique_wsr_values]
    
    if categorical_x:
        # Use categorical positions for x-axis
        x_positions = range(len(unique_wsr_values))
        wsr_to_position = {wsr: pos for pos, wsr in enumerate(unique_wsr_values)}
    else:
        # Use actual WSR values for x-axis (continuous)
        x_positions = unique_wsr_values
        wsr_to_position = {wsr: wsr for wsr in unique_wsr_values}
    
    for i, strategy in enumerate(unique_strategies):
        strategy_data = filtered_df[filtered_df['strategy_label'] == strategy]
        
        # Sort by WSR for better line plotting
        strategy_data = strategy_data.sort_values('wsr_percentage')
        
        # Get style for this strategy, or use default if not defined
        if strategy in strategy_styles:
            style = strategy_styles[strategy]
        else:
            # Default style for undefined strategies
            default_colors = ['#17becf', '#bcbd22', '#7f7f7f', '#e377c2', '#8c564b']
            default_markers = ['*', 'P', 'X', 'h', '+']
            default_linestyles = ['-', '--', '-.', ':', '-']
            style = {
                'color': default_colors[i % len(default_colors)],
                'linestyle': default_linestyles[i % len(default_linestyles)],
                'marker': default_markers[i % len(default_markers)]
            }
        
        # Map WSR values to x-axis positions
        x_values = [wsr_to_position[wsr] for wsr in strategy_data['wsr_percentage']]
        
        ax.plot(x_values, 
                strategy_data['miss_ratio'],
                marker=style['marker'], 
                linestyle=style['linestyle'],
                label=create_display_label(strategy),
                color=style['color'],
                linewidth=2.5,
                markersize=8)
    
    # Customize the plot
    ax.set_xlabel('Cache Size (% of Working Set)')
    ax.set_ylabel('Miss Ratio')
    # ax.set_title(f'LAMA Analysis: {trace_name}')  # No title for publication plots
    
    # Set x-axis ticks and labels
    if categorical_x:
        # Set categorical x-axis with proper labels
        ax.set_xticks(x_positions)
        ax.set_xticklabels(wsr_labels)
    else:
        # For continuous x-axis, matplotlib will handle ticks automatically
        pass
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Position legend
    ax.legend(loc='upper right', bbox_to_anchor=(0.98, 0.98))
    
    # Tight layout
    plt.tight_layout()
    
    # Save the plot
    if categorical_x:
        output_file = os.path.join(output_dir, f'lama_analysis_{trace_name}_categorical.pdf')
    else:
        output_file = os.path.join(output_dir, f'lama_analysis_{trace_name}.pdf')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to {output_file}")
    
    # Show summary statistics
    print("\n=== Summary Statistics ===")
    for strategy in unique_strategies:
        strategy_data = filtered_df[filtered_df['strategy_label'] == strategy]
        print(f"\n{strategy}:")
        print(f"  Number of data points: {len(strategy_data)}")
        print(f"  WSR range: {strategy_data['wsr_percentage'].min():.2f}% - {strategy_data['wsr_percentage'].max():.2f}%")
        print(f"  Miss ratio range: {strategy_data['miss_ratio'].min():.4f} - {strategy_data['miss_ratio'].max():.4f}")
        print(f"  Mean miss ratio: {strategy_data['miss_ratio'].mean():.4f}")
    
    plt.show()

if __name__ == "__main__":
    # Configuration
    csv_path = '../result/lama_buffer_size.csv'
    output_dir = 'figures/lama_buffer_analysis'
    categorical_x = False  # Set to True to use categorical x-axis
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Plot for different traces
    traces = ['meta_202210_kv', 'meta_202401_kv', 'meta_memcache_2024_kv', 'twitter_cluster53']
    
    for trace in traces:
        print(f"Plotting trace: {trace}")
        plot_lama_analysis(trace, csv_path, output_dir, categorical_x)
        plot_lama_analysis(trace, csv_path, output_dir, True)