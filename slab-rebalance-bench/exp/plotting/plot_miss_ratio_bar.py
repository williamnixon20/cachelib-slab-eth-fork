import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from pathlib import Path
from const import (strategy_order, strategy_labels, strategy_colors, 
                   allocator_order, allocator_labels, rcParams)

def add_metric_compared_to_worst(df, trace_name, metric):
    """
    Compute the metric compared to the worst strategy amongst all strategies.
    Adds a new column to the dataframe with the suffix '_percent_reduction_from_worst'.
    
    Parameters:
    df (pd.DataFrame): The dataframe containing the data
    trace_name (str): The trace name to filter the data
    metric (str): The metric column to compare (e.g., 'miss_ratio')
    """
    df_trace = df[df['trace_name'] == trace_name]
    worst_value = df_trace["miss_ratio"].max()
    print(f"Worst {metric} for trace {trace_name}: {worst_value}")
    print("Row with worst value:")
    # print all no '...'
    # set option
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # print(df_trace[df_trace["miss_ratio"] == worst_value])
    new_column = f"miss_ratio_percent_reduction_from_worst"
    df.loc[df['trace_name'] == trace_name, new_column] = (
        (worst_value - df_trace["miss_ratio"]) / worst_value * 100
    )
    df.loc[df['trace_name'] == trace_name, new_column] = df.loc[df['trace_name'] == trace_name, new_column].fillna(0)
    
    return df
    


def plot_cdn_bars(trace_name, csv_file, output_dir=None, metric="miss_ratio_percent_reduction_from_disabled", wsr=None):
    """
    Plot bar chart of miss ratios for different allocators and rebalance strategies.
    Creates one bar plot per trace_name with allocators on x-axis and strategies as colored bars.
    
    Parameters:
    trace_name (str): The trace name to filter the data
    csv_file (str): Path to the CSV file containing the data
    output_dir (str): Directory to save output plots. If None, uses current directory.
    """
    
    # Set output directory
    if output_dir is None:
        output_dir = os.getcwd()
    elif not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Read the data
    df = pd.read_csv(csv_file)
    
    # Filter by trace_name
    df_filtered = df[df['trace_name'] == trace_name].copy()
    valid_strategies = [s for s in strategy_order if s in ['hits', 'disabled']]
    df_filtered = df_filtered[df_filtered['rebalance_strategy'].isin(valid_strategies)]

    if wsr is not None:
        df_filtered = df_filtered[df_filtered['wsr'] == wsr]
        
    add_metric_compared_to_worst(df_filtered, trace_name, metric)


    if df_filtered.empty:
        print(f"No data found for trace_name: {trace_name}")
        return
    
    # Set up matplotlib for publication quality
    plt.rcParams.update(rcParams)
    
    # Create the bar plot
    fig, ax = plt.subplots(figsize=(12, 8), constrained_layout=True)
    
    # Get unique strategies in this data and order them
    available_strategies = df_filtered['rebalance_strategy'].unique()
    strategies = [s for s in strategy_order if s in available_strategies]
    
    # Set up bar positions
    x_positions = np.arange(len(allocator_order))
    
    # For each allocator, determine which strategies have data
    allocator_strategies = {}
    for allocator in allocator_order:
        allocator_strategies[allocator] = []
        for strategy in strategies:
            strategy_data = df_filtered[df_filtered['rebalance_strategy'] == strategy]
            allocator_data = strategy_data[strategy_data['allocator'] == allocator]
            if not allocator_data.empty:
                allocator_strategies[allocator].append(strategy)
    
    # Plot bars for each strategy
    legend_elements = []  # Track legend elements to avoid duplicates
    
    for strategy in strategies:
        strategy_data = df_filtered[df_filtered['rebalance_strategy'] == strategy]
        
        if strategy_data.empty:
            continue
        
        # Get styling
        color = strategy_colors.get(strategy, '#000000')
        label = strategy_labels.get(strategy, strategy)
        
        # Track if we've added this strategy to legend
        strategy_added_to_legend = False
        
        # Plot bars for each allocator where this strategy has data
        for j, allocator in enumerate(allocator_order):
            allocator_data = strategy_data[strategy_data['allocator'] == allocator]
            
            if not allocator_data.empty:
                miss_ratio = allocator_data[metric].mean()
                
                # Calculate bar position based on strategies available for this allocator
                strategies_for_allocator = allocator_strategies[allocator]
                n_strategies = len(strategies_for_allocator)
                bar_width = 0.8 / n_strategies
                
                # Find the index of this strategy within the available strategies for this allocator
                strategy_index = strategies_for_allocator.index(strategy)
                bar_position = x_positions[j] + (strategy_index - n_strategies/2 + 0.5) * bar_width
                
                # Plot the bar
                bar = ax.bar(bar_position, miss_ratio, bar_width,
                           color=color,
                           edgecolor='black', linewidth=1)
                
                # Add to legend elements only once per strategy
                if not strategy_added_to_legend:
                    legend_elements.append(plt.Rectangle((0,0),1,1, facecolor=color, edgecolor='black', linewidth=1, label=label))
                    strategy_added_to_legend = True
    
    # Customize the plot
    ax.set_xlabel('Eviction Policy')
    ax.set_ylabel('Miss Ratio' if metric == 'miss_ratio' else 'Miss Ratio Reduction')
    ax.set_xticks(x_positions)
    ax.set_xticklabels(allocator_labels)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add vertical dashed lines between different eviction policies
    for i in range(len(allocator_order) - 1):
        # Position the line between allocator groups
        line_x = x_positions[i] + 0.5
        ax.axvline(x=line_x, color='gray', linestyle='--', alpha=0.7, linewidth=1)
    
    # Create custom legend
    if legend_elements:
        # Create legend with 2 rows, 3 columns outside the plot area at the top
        legend = ax.legend(handles=legend_elements, 
                          bbox_to_anchor=(0.5, 1.20), loc='center',
                          ncol=3, frameon=True, fancybox=True, shadow=True, 
                          framealpha=0.9, edgecolor='black')
        legend.get_frame().set_facecolor('white')
    
    # # Set reasonable y-axis limits
    # ax.set_ylim(bottom=0)
    
    # plt.tight_layout()
    ax.set_title(f"Paper Trace: {trace_name.split('.')[0]}, WSR: {wsr}")
    # Tight layout
    
    # Save the figure in the specified output directory
    csv_name = Path(csv_file).stem
    output_dir = os.path.join(output_dir, csv_name, str(wsr), metric)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"cdn_bars_{str(wsr)}_{metric}_{trace_name}.pdf")
    plt.savefig(output_path, bbox_inches='tight')
    plt.savefig(output_path.replace('.pdf', '.png'), bbox_inches='tight')
    # save to png too
    # save to png
    plt.close()
    
    print(f"Saved plot for {trace_name}: {output_path}")


def plot_wsr_line(trace_name, csv_file, output_dir=None, metric="miss_ratio"):
    """
    Plot line chart of miss ratios for different rebalance strategies across WSRs,
    separated per allocator.
    Each (trace_name, allocator) pair will get its own plot.

    Parameters:
    trace_name (str): The trace name to filter the data
    csv_file (str): Path to the CSV file containing the data
    output_dir (str): Directory to save output plots. If None, uses current directory.
    """

    # Set output directory
    if output_dir is None:
        output_dir = os.getcwd()
    elif not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Read data
    df = pd.read_csv(csv_file)
    #     I have this other file 
    # file,algorithm,cache_size,wsr,total_requests,miss_ratio,throughput_MQPS
    # this is original df columns trace_name,allocator,rebalance_strategy,miss_ratio,n_rebalanced_slabs,allocFactor,base_dir,cacheSizeMB,compressed,cpu_requirement,enableLookaside,generator,ignoreLargeReq,lru2qColdPct,lru2qHotPct,lruRefreshSec,maxAllocSize,memory_requirement,minAllocSize,miss_ratio_percent_reduction_from_disabled,miss_ratio_percent_reduction_from_lru_disabled,miss_ratio_reduction_from_disabled,miss_ratio_reduction_from_lru_disabled,monitor_interval,moveOnSlabRelease,n_alloc_failures,numOps,numThreads,onlySetIfMiss,poolRebalanceIntervalSec,poolResizeIntervalSec,prepopulateCache,purpose,rebalanceDiffRatio,rebalanceStrategy,rebalanced_slabs,repeatOpCount,repeatTraceReplay,replayGeneratorConfig,slab_cnt,slab_size,tag,throughput,timestampFactor,traceFileName,trace_file,tuned_improvement,tuned_percent_improvement,useTraceTimer,uuid,wsr,wss_mb,zstdTrac
    df_other = pd.read_csv("/home/cc/CacheLib/slab-rebalance-bench/exp/report/lib_cache_optim.csv")
    # Concat the other file please, file is tracename in original column.
    # I want the df to be new rows.
    # df_other rebalance_strategy is None
    df_other['rebalance_strategy'] = 'disabled'
    df_other["slab_cnt"] = 0
    df = pd.concat([df, df_other.rename(columns={
        "file": "trace_name",
        "algorithm": "allocator"
        })], ignore_index=True)
    
    
    # Filter by trace_name
    df_filtered = df[df['trace_name'] == trace_name].copy()
    if df_filtered.empty:
        print(f"No data found for trace_name: {trace_name}")
        return

    # Define strategies to keep
    available_strategies = df_filtered['rebalance_strategy'].unique()
    strategies = [s for s in ['hits', 'disabled'] if s in available_strategies]

    # Define output directory
    csv_name = Path(csv_file).stem
    base_output_dir = os.path.join(output_dir, csv_name, "wsr_lines_hit")

    # Loop per allocator
    for allocator in sorted(df_filtered['allocator'].unique()):
        df_alloc = df_filtered[df_filtered['allocator'] == allocator]

        if df_alloc.empty:
            continue

        plt.rcParams.update({
            "font.size": 14,
            "axes.labelsize": 16,
            "axes.titlesize": 18,
            "legend.fontsize": 12,
            "figure.titlesize": 20,
        })

        fig, ax = plt.subplots(figsize=(12, 8), constrained_layout=True)

        # Plot each strategy line
        for strategy in strategies:
            strategy_data = df_alloc[df_alloc['rebalance_strategy'] == strategy].sort_values(by='wsr')

            if strategy_data.empty:
                continue

            color = strategy_colors.get(strategy, '#000000')
            label = f"{strategy_labels.get(strategy, strategy)}"

            ax.plot(strategy_data['wsr'], strategy_data[metric], marker='o',
                    color=color, label=label, linewidth=2)

        # Customize the plot
        ax.set_title(f"{trace_name} — {allocator}")
        ax.set_xlabel('WSR')
        ax.set_ylabel('{}'.format("Miss Ratio" if metric == "miss_ratio" else metric.title()))
        ax.grid(True, alpha=0.3, axis='y')
        ax.legend(frameon=True, fancybox=True, shadow=True, framealpha=0.9, edgecolor='black')
        ax.set_ylim(bottom=0)

        # Save the plot
        allocator_dir = os.path.join(base_output_dir, allocator)
        os.makedirs(allocator_dir, exist_ok=True)
        output_path = os.path.join(allocator_dir, f"{trace_name}_{allocator}.pdf")

        plt.savefig(output_path, bbox_inches='tight')
        plt.savefig(output_path.replace('.pdf', '.png'), bbox_inches='tight')
        plt.close()

        print(f"Saved plot for {trace_name} ({allocator}): {output_path}")

import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from matplotlib.lines import Line2D

def plot_wsr_line_combi(trace_name, csv_file, output_dir=None, metric="miss_ratio"):
    """
    Plot line chart of miss ratios for different rebalance strategies across WSRs,
    showing all allocators together in one plot.
    Colors = strategy, markers/linestyles = allocator.
    Separate legends for strategies and allocators.
    """

    # Set output directory
    if output_dir is None:
        output_dir = os.getcwd()
    elif not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Read data
    df = pd.read_csv(csv_file)
    df_other = pd.read_csv("/home/cc/CacheLib/slab-rebalance-bench/exp/report/lib_cache_optim.csv")
    df_other['rebalance_strategy'] = 'disabled'
    # if the trace is lru-sim, then rebalance_stratey = 'lru-sim'
    df_other.loc[df_other['algorithm'] == 'LRU-sim', 'rebalance_strategy'] = 'lru-sim'
    # Concat the other file please, file is tracename in original column.
    # I want the df to be new rows.
    df = pd.concat([df, df_other.rename(columns={
        "file": "trace_name",
        "algorithm": "allocator"
        })], ignore_index=True)
    df_filtered = df[df['trace_name'] == trace_name].copy()
    if df_filtered.empty:
        print(f"No data found for trace_name: {trace_name}")
        return

    # Strategies and allocators
    strategies = sorted(df_filtered['rebalance_strategy'].unique())
    allocators = sorted(df_filtered['allocator'].unique())
    
    # # print the dict of wsr and slab_cnt for trace_name
    # print(f"WSR and slab_cnt for trace {trace_name}:")
    # wsr_slabcnt = df_filtered[['trace_name', 'wsr', 'slab_cnt', 'cacheSizeMB','cache_size']].drop_duplicates().sort_values(by='wsr')
    # # drop nan slab_cnt
    # # wsr_slabcnt = wsr_slabcnt[wsr_slabcnt['slab_cnt'].notna()]
    # print("Trace: {}".format(trace_name))
    # # dont print trace_name
    # print(wsr_slabcnt[['wsr', 'slab_cnt', 'cacheSizeMB', "cacheSizeMB"]].to_json(orient='records', indent=2))
        

    # Markers and line styles for allocators
    markers = ['o', 's', '^', 'D', 'v', '*', 'X', 'P']
    linestyles = ['-', '--', '-.', ':']
    marker_map = {alloc: markers[i % len(markers)] for i, alloc in enumerate(allocators)}
    linestyle_map = {alloc: linestyles[i % len(linestyles)] for i, alloc in enumerate(allocators)}

    # Matplotlib setup
    plt.rcParams.update({
        "font.size": 14,
        "axes.labelsize": 16,
        "axes.titlesize": 18,
        "legend.fontsize": 12,
        "figure.titlesize": 20,
    })

    fig, ax = plt.subplots(figsize=(12, 8), constrained_layout=True)

    # Plot lines
    for allocator in allocators:
        for strategy in strategies:
            subset = df_filtered[
                (df_filtered['allocator'] == allocator) &
                (df_filtered['rebalance_strategy'] == strategy)
            ].sort_values(by='wsr')

            if subset.empty:
                continue

            color = strategy_colors.get(strategy, 'red')
            marker = marker_map[allocator]
            linestyle = linestyle_map[allocator]

            ax.plot(subset['wsr'], subset[metric],
                    marker=marker, linestyle=linestyle, color=color,
                    linewidth=2, markersize=6,
                    label=f"{strategy_labels.get(strategy, strategy)}")
        
    # Cap at 100

    # Create custom legends
    # 1️⃣ Legend for strategies (color)
    strategy_legend = [
        Line2D([0], [0], color=strategy_colors.get(s, 'red'),
               lw=3, label=strategy_labels.get(s, s))
        for s in strategies
    ]

    # 2️⃣ Legend for allocators (marker + linestyle)
    allocator_legend = [
        Line2D([0], [0], color='black', lw=2,
               marker=marker_map[a], linestyle=linestyle_map[a],
               markersize=6, label=a)
        for a in allocators
    ]

    # Customize plot
    ax.set_title(f"{trace_name}: {metric.title()} vs WSR (All Allocators)")
    ax.set_xlabel("WSR")
    ax.set_ylabel("{}".format("Miss Ratio" if metric == "miss_ratio" else metric.title()))
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_ylim(bottom=0)

    # Add legends separately
    legend1 = ax.legend(handles=strategy_legend, title="Strategy",
                        loc='upper right', frameon=True, fancybox=True, shadow=True)
    ax.add_artist(legend1)
    ax.legend(handles=allocator_legend, title="Allocator",
              loc='upper center', frameon=True, fancybox=True, shadow=True)

    # Save
    csv_name = Path(csv_file).stem
    output_dir = os.path.join(output_dir, csv_name, "wsr_lines_hit")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{trace_name}_allocators_combined.pdf")

    plt.savefig(output_path, bbox_inches='tight')
    plt.savefig(output_path.replace('.pdf', '.png'), bbox_inches='tight')
    plt.close()

    print(f"Saved combined allocator plot for {trace_name}: {output_path}")


def plot_all_cdn_traces(csv_file, output_dir=None, metric="miss_ratio"):
    """Plot bar charts for all CDN trace names
    
    Parameters:
    csv_file (str): Path to the CSV file containing the data
    output_dir (str): Directory to save output plots. If None, uses current directory.
    """
    # read csv file, get all the unique trace names
    df = pd.read_csv(csv_file)
    trace_names = df['trace_name'].unique()
    unique_wsr = df['wsr'].unique()
    # Filter out traces not 'kv' and not 'cluster'
    # trace_names = [t for t in trace_names if 'kv' in t or 'cluster52' in t]
    
    
    for trace_name in trace_names:
        plot_wsr_line(trace_name, csv_file, output_dir, metric="miss_ratio")
        plot_wsr_line_combi(trace_name, csv_file, output_dir, metric="miss_ratio")
        # for wsr in unique_wsr:
        #     for metric in ["miss_ratio", "miss_ratio_percent_reduction_from_worst"]:
        #         plot_cdn_bars(trace_name, csv_file, output_dir, metric=metric, wsr=wsr)

    print(f"\nCreated bar plots for {len(trace_names)} CDN traces")

if __name__ == "__main__":
    # csv_file = "/home/cc/CacheLib/slab-rebalance-bench/exp/report/new_reportt.csv"
    csv_file = "/home/cc/CacheLib/slab-rebalance-bench/exp/result/efficiency_result_processed.csv"
    output_dir = "figures/cdn_bars_new"
    plot_all_cdn_traces(csv_file, output_dir)