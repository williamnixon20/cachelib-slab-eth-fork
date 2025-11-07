"""
define a plotting function:
x axis is Number of Slabs, y axis is Miss Ratio
title if Class xx, xx is parameter, 
what i want here is a illustrative miss ratio curve, 
note that when x=0 y=1, let's characterize the curve 
by total working set by the number of slabs
and a skewness parameter if you know what is zipfian distribution
basically i want to plot different miss ratio curves with similar working set sizes but different skewness,
each time we plot a pair, class 0 and class 1 
make class 0 red and class 1 blue
plot them separately put togheter horizontally side-by-side
"""

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams

# Set up publication-quality plotting parameters
rcParams.update({
    'font.size': 16,
    'font.family': 'sans-serif',
    'axes.linewidth': 1.2,
    'axes.labelsize': 18,
    'xtick.labelsize': 16,
    'ytick.labelsize': 16,
    'figure.dpi': 100,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight'
})

def generate_miss_ratio_curve(max_slabs, working_set_size, skewness):
    """
    Generate a miss ratio curve based on Zipfian distribution.
    
    Parameters:
    max_slabs (int): Maximum number of slabs to plot
    working_set_size (float): Total working set size (controls where curve reaches ~0)
    skewness (float): Zipfian skewness parameter (higher = more skewed/steeper curve, lower = flatter)
    
    Returns:
    x, y: Arrays of slab counts and corresponding miss ratios
    """
    x = np.linspace(0, max_slabs, 200)
    
    # Zipfian-like miss ratio curve
    # When x=0, y=1 (complete miss)
    # As x approaches working_set_size, y approaches 0
    
    # Normalize x by working set size
    x_norm = x / working_set_size
    
    # Corrected Zipfian-inspired formula: 
    # Higher skewness = steeper curve (more skewed)
    # Lower skewness = flatter curve (less skewed, more uniform)
    # Special case: skewness = -1 means flat line at miss ratio = 1
    if skewness == -1:
        # Flat line at miss ratio = 1 (always miss)
        y = np.ones_like(x)
    elif skewness > 0:
        # Use exponential decay with skewness controlling steepness
        y = np.exp(-skewness * x_norm)
    else:
        # Handle edge case of zero skewness (linear decay)
        y = np.maximum(0, 1 - x_norm)
    
    # Ensure y starts at 1 when x=0 and approaches 0 asymptotically
    y = np.clip(y, 0, 1)
    
    return x, y

def plot_miss_ratio_curves_comparison(scenarios, output_file=None):
    """
    Plot miss ratio curves for different scenarios side by side.
    
    Parameters:
    scenarios: List of tuples (working_set_size, skewness_pairs)
        Each scenario contains a working set size and list of (class_id, skewness) pairs
    output_file: Optional output file path
    """
    
    n_scenarios = len(scenarios)
    
    # Check if we have exactly one scenario with two classes for side-by-side class plots
    if n_scenarios == 1 and len(scenarios[0][1]) == 2:
        # Special case: plot each class in its own subplot
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        
        working_set_size, skewness_pairs = scenarios[0]
        colors = {0: 'red', 1: 'blue'}
        max_slabs = int(working_set_size * 1.5)
        
        for i, (class_id, skewness) in enumerate(skewness_pairs):
            ax = axes[i]
            x, y = generate_miss_ratio_curve(max_slabs, working_set_size, skewness)
            
            ax.plot(x, y, color=colors[class_id], linewidth=2.5)
            
            # Customize each subplot
            ax.set_xlabel('Number of Slabs')
            ax.set_ylabel('Miss Ratio')
            ax.set_title(f'Class {class_id}')
            ax.grid(True, alpha=0.3)
            ax.set_ylim(0, 1.05)
            ax.set_xlim(0, max_slabs)
            
            # Add reference lines
            ax.axhline(y=0, color='black', linestyle='--', alpha=0.3)
            ax.axvline(x=working_set_size, color='gray', linestyle=':', alpha=0.5)
            

    
    else:
        # Original behavior for multiple scenarios or different configurations
        fig, axes = plt.subplots(1, n_scenarios, figsize=(6*n_scenarios, 5))
        
        # If only one scenario, make axes a list for consistency
        if n_scenarios == 1:
            axes = [axes]
        
        colors = {0: 'red', 1: 'blue'}
        
        for i, (working_set_size, skewness_pairs) in enumerate(scenarios):
            ax = axes[i]
            
            max_slabs = int(working_set_size * 1.5)  # Plot a bit beyond working set
            
            for class_id, skewness in skewness_pairs:
                x, y = generate_miss_ratio_curve(max_slabs, working_set_size, skewness)
                
                ax.plot(x, y, color=colors[class_id], linewidth=2.5)
            
            # Customize the subplot
            ax.set_xlabel('Number of Slabs')
            ax.set_ylabel('Miss Ratio')
            ax.set_title(f'Working Set Size: {working_set_size} slabs')
            ax.grid(True, alpha=0.3)
            ax.set_ylim(0, 1.05)
            ax.set_xlim(0, max_slabs)
            
            # Add reference lines
            ax.axhline(y=0, color='black', linestyle='--', alpha=0.3)
            ax.axvline(x=working_set_size, color='gray', linestyle=':', alpha=0.5)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_file}")
    
    plt.show()

# Example usage and demonstration
if __name__ == "__main__":

    
    # Example of single scenario
    single_scenario = [(100, [(0, 2), (1, -1)])]
    plot_miss_ratio_curves_comparison(single_scenario, "single_miss_ratio_curve_diff_skew2.pdf")