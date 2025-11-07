#!/bin/bash

# Build script for OracleGeneralBinaryZstdTraceReader
# This script compiles the C++ code with zstd library dependency

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building OracleGeneralBinaryZstdTraceReader...${NC}"

# Check if zstd development libraries are available
echo -e "${YELLOW}Checking for zstd library...${NC}"
if ! pkg-config --exists libzstd 2>/dev/null; then
    echo -e "${RED}Error: libzstd development package not found!${NC}"
    echo "Please install zstd development libraries:"
    echo "  Ubuntu/Debian: sudo apt-get install libzstd-dev"
    echo "  CentOS/RHEL:   sudo yum install libzstd-devel"
    echo "  Or build from source: https://github.com/facebook/zstd"
    exit 1
fi

# Compiler settings
CXX=${CXX:-g++}
CXXFLAGS="-std=c++11 -O2 -Wall -Wextra"
LDFLAGS=""

# Get zstd flags using pkg-config if available
if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libzstd; then
    ZSTD_CFLAGS=$(pkg-config --cflags libzstd)
    ZSTD_LIBS=$(pkg-config --libs libzstd)
    echo -e "${GREEN}Using pkg-config for zstd: ${ZSTD_LIBS}${NC}"
else
    # Fallback to standard library flags
    ZSTD_CFLAGS=""
    ZSTD_LIBS="-lzstd"
    echo -e "${YELLOW}Using fallback zstd linking: ${ZSTD_LIBS}${NC}"
fi

# Source and output files
SOURCE_FILE="OracleGeneralBinaryZstdTraceReader.cpp"
HEADER_FILE="OracleGeneralBinaryZstdTraceReader.h"
OUTPUT_BINARY="oracle_trace_reader"

# Check if source files exist
if [[ ! -f "$SOURCE_FILE" ]]; then
    echo -e "${RED}Error: Source file $SOURCE_FILE not found!${NC}"
    exit 1
fi

if [[ ! -f "$HEADER_FILE" ]]; then
    echo -e "${RED}Error: Header file $HEADER_FILE not found!${NC}"
    exit 1
fi

# Build command
BUILD_CMD="$CXX $CXXFLAGS $ZSTD_CFLAGS -o $OUTPUT_BINARY $SOURCE_FILE $ZSTD_LIBS $LDFLAGS"

echo -e "${YELLOW}Compiling with command:${NC}"
echo "$BUILD_CMD"
echo

# Execute build
if $BUILD_CMD; then
    echo -e "${GREEN}✓ Build successful!${NC}"
    echo -e "${GREEN}✓ Binary created: $OUTPUT_BINARY${NC}"
    
    # Make executable
    chmod +x "$OUTPUT_BINARY"
    
    # Show binary info
    if command -v file >/dev/null 2>&1; then
        echo -e "${YELLOW}Binary info:${NC}"
        file "$OUTPUT_BINARY"
    fi
    
    # Show usage
    echo
    echo -e "${YELLOW}Usage:${NC}"
    echo "  ./$OUTPUT_BINARY <input_file.zst> [output_file.csv] [max_records] [print_min_max_size]"
    echo
    echo -e "${YELLOW}Examples:${NC}"
    echo "  ./$OUTPUT_BINARY trace.zst                           # Print first record"
    echo "  ./$OUTPUT_BINARY trace.zst output.csv               # Convert to CSV"
    echo "  ./$OUTPUT_BINARY trace.zst output.csv 1000          # Convert first 1000 records"
    echo "  ./$OUTPUT_BINARY trace.zst print_min_max_size       # Show statistics"
    
else
    echo -e "${RED}✗ Build failed!${NC}"
    exit 1
fi