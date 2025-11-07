#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Read username from file
if [[ ! -f "$SCRIPT_DIR/../hosts/username.txt" ]]; then
    echo "Error: username.txt not found in $SCRIPT_DIR/../hosts/"
    exit 1
fi
USERNAME=$(cat "$SCRIPT_DIR/../hosts/username.txt" | tr -d '\n\r')

# Read machines from file
if [[ ! -f "$SCRIPT_DIR/../hosts/hosts.txt" ]]; then
    echo "Error: hosts.txt not found in $SCRIPT_DIR/../hosts/"
    exit 1
fi

# Read hosts robustly, filtering out empty lines and trimming whitespace
MACHINES=()
while IFS= read -r line || [[ -n "$line" ]]; do
    # Trim leading and trailing whitespace
    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    # Skip empty lines
    if [[ -n "$line" ]]; then
        MACHINES+=("$line")
    fi
done < "$SCRIPT_DIR/../hosts/hosts.txt"

# Debug: Show what was read
echo "Read ${#MACHINES[@]} hosts from hosts.txt:"
for i in "${!MACHINES[@]}"; do
    echo "  [$i]: '${MACHINES[$i]}'"
done

# Validate we have at least one host
if [ ${#MACHINES[@]} -eq 0 ]; then
    echo "Error: No valid hosts found in hosts.txt"
    exit 1
fi

UPDATE_CMDS=$(cat <<END_CMDS
echo "Starting update and rebuild process..."

cd /users/$USERNAME

# Update cachelib_v1 (4mb slab)
echo "Updating cachelib_v1..."
cd cachelib_v1
if [ ! -d ".git" ]; then
    echo "Error: cachelib_v1 is not a git repository"
    exit 1
fi

echo "Fetching latest changes from remote..."
git fetch origin

echo "Checking out benchmark-4mb-slab branch..."
git checkout benchmark-4mb-slab

echo "Merging latest changes..."
git merge origin/benchmark-4mb-slab

echo "Rebuilding cachelib_v1..."
cd build-cachelib
sudo make install
if [ \$? -eq 0 ]; then
    echo "✓ cachelib_v1 rebuild successful"
else
    echo "✗ cachelib_v1 rebuild failed"
    exit 1
fi

# Update cachelib_v2 (1mb slab)
echo "Updating cachelib_v2..."
cd /users/$USERNAME/cachelib_v2
if [ ! -d ".git" ]; then
    echo "Error: cachelib_v2 is not a git repository"
    exit 1
fi

echo "Fetching latest changes from remote..."
git fetch origin

echo "Checking out benchmark-1mb-slab branch..."
git checkout benchmark-1mb-slab

echo "Merging latest changes..."
git merge origin/benchmark-1mb-slab

echo "Rebuilding cachelib_v2..."
cd build-cachelib
sudo make install
if [ \$? -eq 0 ]; then
    echo "✓ cachelib_v2 rebuild successful"
else
    echo "✗ cachelib_v2 rebuild failed"
    exit 1
fi

echo "Update and rebuild process completed successfully!"
END_CMDS
)

# Execute update commands on all machines in parallel
echo "Starting update and rebuild on all hosts..."
for MACHINE in "${MACHINES[@]}"; do
    echo "Updating $MACHINE ..."
    ssh "$MACHINE" "$UPDATE_CMDS" &
done

wait
echo "All updates and rebuilds finished."

# Verification
echo "Running verification..."
VERIFICATION_CMDS=$(cat <<'EOF'
echo "=== VERIFICATION ==="

# Check cachelib_v1
echo "Checking cachelib_v1 cachebench..."
if [ -f /users/USERNAME_PLACEHOLDER/cachelib_v1/opt/cachelib/bin/cachebench ]; then
    echo "✓ cachelib_v1 cachebench exists"
else
    echo "✗ cachelib_v1 cachebench not found"
fi

# Check cachelib_v2
echo "Checking cachelib_v2 cachebench..."
if [ -f /users/USERNAME_PLACEHOLDER/cachelib_v2/opt/cachelib/bin/cachebench ]; then
    echo "✓ cachelib_v2 cachebench exists"
else
    echo "✗ cachelib_v2 cachebench not found"
fi

# Check git status
echo "Git status for cachelib_v1:"
cd /users/USERNAME_PLACEHOLDER/cachelib_v1
git branch --show-current
git log --oneline -1

echo "Git status for cachelib_v2:"
cd /users/USERNAME_PLACEHOLDER/cachelib_v2
git branch --show-current
git log --oneline -1
EOF
)

# Replace username placeholder in verification commands
VERIFICATION_CMDS=$(echo "$VERIFICATION_CMDS" | sed "s/USERNAME_PLACEHOLDER/$USERNAME/g")

echo "Running verification on first host..."
ssh "${MACHINES[0]}" "$VERIFICATION_CMDS"

echo "Update and rebuild script completed!"
