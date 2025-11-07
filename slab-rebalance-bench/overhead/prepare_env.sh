#!/bin/bash

#!/bin/bash

# Environment Preparation for Micro-benchmarking
# Target: CloudLab c6320 nodes with Intel Xeon E5-2683 v3 CPUs @ 2.00 GHz
# 
# For accurate micro-benchmarks we need to:
# 1. Disable turbo boost - ensures consistent CPU frequency 
# 2. Enable perf events - allows hardware performance counter access

set -e

echo "=== Preparing Environment for Micro-benchmarking ==="

# Disable CPU Turbo Boost
echo "Disabling turbo boost..."
sudo ./disable_turbo.sh disable

# Enable Performance Events  
echo "Enabling performance events..."
sudo ./enable_perf.sh

echo "✓ Environment ready for micro-benchmarking"
echo "Note: Re-enable turbo boost later with: sudo ./disable_turbo.sh enable" 