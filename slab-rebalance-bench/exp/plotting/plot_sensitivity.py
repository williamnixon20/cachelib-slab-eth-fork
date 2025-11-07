import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import sys

# Add parent directory to path to import const
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from const import *

# Set up matplotlib for publication quality
plt.rcParams.update(rcParams)

def load_sensitivity_data(csv_path):
    """Load sensitivity analysis data"""
    df = pd.read_csv(csv_path)
    # Filter for meta_202210_kv trace only
    df_filtered = df[df['trace_name'] == 'meta_202210_kv'].copy()
    # Convert wsr to percentage like in meta_kv script
    df_filtered['wsr_percent'] = df_filtered['wsr'] * 100
    return df_filtered

def get_parameter_configs():
    """Define parameter configurations for sensitivity analysis"""
    # Default values - these should match the defaults used in prepare_configs_sensitivity.py
    defaults = {
        'monitor_interval': 50000,
        'mhMovingAverageParam': 0.3,  # Changed back to 0.3 to match the original config
        'mhMinDiff': 2,
        'thresholdAIADStep': 2,
        'emrLow': 0.5,
        'emrHigh': 0.95
    }
    
    # Parameter variations
    param_variations = {
        'monitor_interval': [10000, 20000, 50000, 100000, 200000, 500000],
        'mhMovingAverageParam': [0.1, 0.3, 0.5],
        'mhMinDiff': [1, 2, 4, 8],
        'thresholdAIADStep': [1, 2, 4, 8],
        'emrLow': [0.3, 0.4, 0.5],
        'emrHigh': [0.85, 0.9, 0.95]
    }
    
    # Nice parameter names for plots
    param_names = {
        'monitor_interval': 'Rebalance Interval',
        'mhMovingAverageParam': 'Decay Factor',
        'mhMinDiff': r'Initial Threshold ($\theta_0$)',
        'thresholdAIADStep': r'Additive Step ($\delta_\theta$)',
        'emrLow': r'EMR Low ($\mathrm{EMR}_{\min}$)',
        'emrHigh': r'EMR High ($\mathrm{EMR}_{\max}$)'
    }
    
    return defaults, param_variations, param_names

def filter_data_for_parameter(df, param_name, defaults):
    """Filter data to only include rows where all parameters except param_name are at default values"""
    filtered_df = df.copy()
    
    for param, default_val in defaults.items():
        if param != param_name:  # Keep all values for the parameter we're studying
            filtered_df = filtered_df[filtered_df[param] == default_val]
    
    # Don't filter the parameter we're studying - we want to see all its values
    return filtered_df

def create_sensitivity_plots(csv_path, output_dir='.'):
    """Create sensitivity analysis plots for each parameter"""
    df = load_sensitivity_data(csv_path)
    defaults, param_variations, param_names = get_parameter_configs()
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Get unique allocators
    allocators = sorted(df['allocator'].unique())
    allocator_labels = {'LRU': 'LRU', 'LRU2Q': 'TwoQ', 'TINYLFU': 'TinyLFU'}
    
    # Color palette for different parameter values
    colors = ['#E74C3C', '#3498DB', '#2ECC71', '#F39C12', '#9B59B6', '#1ABC9C', '#34495E', '#E67E22']
    
    # Marker styles for variety
    markers = ['o', 's', '^', 'D', 'v', 'p', 'H', '*', '<']
    
    # Line styles for variety
    linestyles = ['-', '--', '-.', ':', (0, (3, 1, 1, 1)), (0, (5, 1)), (0, (3, 5, 1, 5)), (0, (1, 1))]
    
    for param_name, param_values in param_variations.items():
        # Filter data for this parameter analysis
        param_data = filter_data_for_parameter(df, param_name, defaults)
        
        if param_data.empty:
            print(f"No data found for parameter: {param_name}")
            continue
        
        # Create separate figures for each allocator
        for allocator in allocators:
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Filter data for this allocator
            alloc_data = param_data[param_data['allocator'] == allocator]
            
            if alloc_data.empty:
                print(f"No data found for allocator: {allocator} and parameter: {param_name}")
                continue
            
            # Plot line for each parameter value
            for i, param_val in enumerate(param_values):
                val_data = alloc_data[alloc_data[param_name] == param_val]
                
                if not val_data.empty:
                    # Sort by wsr_percent for proper line plotting
                    val_data_sorted = val_data.sort_values('wsr_percent')
                    
                    color = colors[i % len(colors)]
                    marker = markers[i % len(markers)]
                    linestyle = linestyles[i % len(linestyles)]
                    
                    # Add '*' to label if this is the default value
                    default_val = defaults.get(param_name)
                    if param_val == default_val:
                        label = f'{param_val}*'
                    else:
                        label = f'{param_val}'
                    
                    ax.plot(val_data_sorted['wsr_percent'], val_data_sorted['miss_ratio'],
                           color=color, 
                           label=label,
                           linestyle=linestyle,
                           marker=marker,
                           markersize=14,  # Increased from 12
                           linewidth=2.5,
                           markerfacecolor=color,
                           markeredgecolor='white',
                           markeredgewidth=1)
            
            # Formatting
            ax.set_xlabel('Cache Size (% of Working Set)')
            ax.set_ylabel('Miss Ratio')
            ax.grid(True, alpha=0.3)
            
            # Put legend inside the plot for better space utilization
            ax.legend(title=param_names[param_name], loc='best', frameon=True, 
                     fancybox=True, shadow=True)
            
            # Set reasonable axis limits
            ax.set_xlim(left=0)
            
            # Set x-axis ticks with step size of 10 like in meta_kv
            if not alloc_data.empty:
                wsr_max_percent = alloc_data['wsr_percent'].max()
                ax.set_xticks(np.arange(0, int(wsr_max_percent) + 10, 10))
            
            # Tight layout
            plt.tight_layout()
            
            # Save individual plot
            output_path = os.path.join(output_dir, f'sensitivity_{param_name}_{allocator}.pdf')
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"Saved: {output_path}")
    
    print(f"\nCreated sensitivity plots for {len(param_variations)} parameters and {len(allocators)} allocators")
    
    # Print data summary
    print(f"\n=== Data Summary ===")
    print(f"Total data points: {len(df)}")
    print(f"Allocators: {allocators}")
    print(f"WSR range: {df['wsr'].min():.4f} - {df['wsr'].max():.4f}")
    print(f"Miss ratio range: {df['miss_ratio'].min():.4f} - {df['miss_ratio'].max():.4f}")
    
    # Check data availability for each parameter
    for param_name in param_variations.keys():
        param_data = filter_data_for_parameter(df, param_name, defaults)
        print(f"{param_name}: {len(param_data)} data points")

if __name__ == "__main__":
    # Default paths
    default_csv_path = '../result/sensitivity_result_processed.csv'
    default_output_dir = 'figures/sensitivity'
    
    # You can modify these paths as needed
    csv_path = default_csv_path
    output_dir = default_output_dir
    
    print(f"Input CSV: {csv_path}")
    print(f"Output directory: {output_dir}")
    
    create_sensitivity_plots(csv_path, output_dir)