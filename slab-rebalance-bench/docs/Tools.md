# Tools

This document describes the tools available in the `tools` directory.

## create_synthetic_trace

Contains a script to help generate synthetic Zipfian traces. For example, each class has a Zipfian skewness, and we mix the requests from different classes based on the configured request frequency ratio to merge into one big trace.

We can also create dynamic workloads with periodic workload changes. There is a `demo_config.json` file you can reference. The `zip_gen.py` script can create synthetic traces based on the config file.

## read_binary_trace

A C++ class that can read zstd compressed binary trace files.

## trace_analysis

Can be used to compute the optimal allocation for synthetic traces. The entry point is `optimal_allocation` - you can pass in the trace file path, and it will compute the optimal allocation for different cache capacities.