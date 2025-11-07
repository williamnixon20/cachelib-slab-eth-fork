#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Read username from file
if [[ ! -f "$SCRIPT_DIR/../hosts/username.txt" ]]; then
    echo "Error: username.txt not found in $SCRIPT_DIR/../hosts/"
    exit 1
fi
USERNAME=$(cat "$SCRIPT_DIR/../hosts/username.txt" | tr -d '\n\r')

# Repository URL
REPO_URL="https://github.com/eth-easl/CacheLib.git"

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

# Get master host (first in the list)
MASTER_HOST="${MACHINES[0]}"
echo "Master host: '$MASTER_HOST'"

# Validate master host is not empty
if [ -z "$MASTER_HOST" ]; then
    echo "Error: Master host is empty after processing"
    exit 1
fi

# Function to setup SSH keys
setup_ssh_keys() {
    echo "Setting up SSH keys..."
    
    # Generate SSH key pair on master host
    echo "Generating SSH key pair on master host: $MASTER_HOST"
    ssh -o StrictHostKeyChecking=accept-new "$MASTER_HOST" "
        if [ ! -f ~/.ssh/id_rsa ]; then
            ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ''
        fi
        chmod 600 ~/.ssh/id_rsa
        chmod 644 ~/.ssh/id_rsa.pub
    "
    
    # Copy public key back to local machine
    echo "Copying public key from master host to local machine..."
    scp "$MASTER_HOST:~/.ssh/id_rsa.pub" "$SCRIPT_DIR/master_id_rsa.pub"
    
    # Distribute public key to all hosts (including master itself)
    echo "Distributing public key to all hosts..."
    for MACHINE in "${MACHINES[@]}"; do
        echo "Adding public key to $MACHINE..."
        ssh -o StrictHostKeyChecking=accept-new "$MACHINE" "
            mkdir -p ~/.ssh
            chmod 700 ~/.ssh
            touch ~/.ssh/authorized_keys
            chmod 600 ~/.ssh/authorized_keys
        "
        cat "$SCRIPT_DIR/master_id_rsa.pub" | ssh "$MACHINE" "cat >> ~/.ssh/authorized_keys"
        
        # Remove duplicate entries
        ssh "$MACHINE" "
            sort ~/.ssh/authorized_keys | uniq > ~/.ssh/authorized_keys.tmp
            mv ~/.ssh/authorized_keys.tmp ~/.ssh/authorized_keys
            chmod 600 ~/.ssh/authorized_keys
        "
    done
    
    # Test SSH connections from master to all hosts
    echo "Testing SSH connections from master host..."
    for MACHINE in "${MACHINES[@]}"; do
        HOST_PART=$(echo "$MACHINE" | cut -d'@' -f2)
        echo "Testing connection from master to $HOST_PART..."
        ssh "$MASTER_HOST" "ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 $HOST_PART 'echo SSH connection to $HOST_PART successful'"
    done
    
    echo "SSH key setup completed!"
}

# Setup SSH keys before running the main setup
setup_ssh_keys

# Copy libmock_time.cpp to all hosts
echo "Copying libmock_time.cpp to all hosts..."
if [[ -f "$SCRIPT_DIR/hook_time/libmock_time.cpp" ]]; then
    for MACHINE in "${MACHINES[@]}"; do
        echo "Copying libmock_time.cpp to $MACHINE..."
        ssh "$MACHINE" "mkdir -p /users/$USERNAME/hook_time"
        scp "$SCRIPT_DIR/hook_time/libmock_time.cpp" "$MACHINE:/users/$USERNAME/hook_time/"
    done
    echo "libmock_time.cpp copied to all hosts."
else
    echo "Warning: libmock_time.cpp not found at $SCRIPT_DIR/hook_time/libmock_time.cpp"
    echo "Please ensure the file exists before running the script."
fi

SETUP_CMDS=$(cat <<END_CMDS
sudo apt-get update -y
sudo apt-get install python3-pip libglib2.0-dev parallel pssh build-essential -y
pip3 install pandas plotly matplotlib seaborn requests 
pip3 install nbformat --upgrade

cd /users/$USERNAME

# Setup cachelib_v1 (4mb slab)
mkdir -p cachelib_v1
cd cachelib_v1
if [ ! -d ".git" ]; then
    git clone $REPO_URL .
fi
git fetch origin
current_branch=\$(git rev-parse --abbrev-ref HEAD)
if [ "\$current_branch" != "benchmark-4mb-slab" ]; then
    git checkout benchmark-4mb-slab
fi
git pull origin benchmark-4mb-slab
sudo ./contrib/build.sh -j -T

# Setup cachelib_v2 (1mb slab)
cd /users/$USERNAME
mkdir -p cachelib_v2
cd cachelib_v2
if [ ! -d ".git" ]; then
    git clone $REPO_URL .
fi
git fetch origin
current_branch=\$(git rev-parse --abbrev-ref HEAD)
if [ "\$current_branch" != "benchmark-1mb-slab" ]; then
    git checkout benchmark-1mb-slab
fi
git pull origin benchmark-1mb-slab
sudo ./contrib/build.sh -j -T

# Copy and compile libmock_time.cpp (after cachelib builds)
cd /users/$USERNAME
# Check if libmock_time.cpp was copied to hook_time directory
if [ -f "hook_time/libmock_time.cpp" ]; then
    echo "Found libmock_time.cpp in hook_time directory. Compiling..."
    cd hook_time
    g++ -shared -fPIC -o libmock_time.so libmock_time.cpp -ldl
    # Copy the compiled artifact to the user directory
    cp libmock_time.so /users/$USERNAME/
    echo "libmock_time.so compiled and copied to /users/$USERNAME/"
else
    echo "Warning: libmock_time.cpp not found in hook_time directory"
    echo "Expected location: /users/$USERNAME/hook_time/libmock_time.cpp"
    ls -la /users/$USERNAME/hook_time/ || echo "hook_time directory does not exist"
fi
END_CMDS
)

for MACHINE in "${MACHINES[@]}"; do
    echo "Setting up $MACHINE ..."
    ssh "$MACHINE" "$SETUP_CMDS" &
done

wait
echo "All setups finished."

# Verification and connectivity tests
echo "Running verification tests from master host: $MASTER_HOST..."

# Create verification commands to run on master host
VERIFICATION_CMDS=$(cat <<'EOF'
# Create a temporary hosts file for parallel-ssh (without username prefix)
TEMP_HOSTS_FILE="/tmp/temp_hosts_pssh.txt"
cat > "$TEMP_HOSTS_FILE" << HOSTS_EOF
HOST_LIST_PLACEHOLDER
HOSTS_EOF

echo "Testing connectivity to all hosts with parallel-ssh..."

# Test whoami on all hosts
echo "Running 'whoami' on all hosts:"
parallel-ssh -h "$TEMP_HOSTS_FILE" -i "whoami"
PSSH_EXIT_CODE=$?
if [ $PSSH_EXIT_CODE -eq 0 ]; then
    echo "✓ All hosts are reachable via parallel-ssh"
else
    echo "✗ Some hosts failed connectivity test (exit code: $PSSH_EXIT_CODE)"
fi

# Verify cachebench installations
echo "Verifying cachebench installations..."

# Check cachelib_v1
echo "Checking cachelib_v1 cachebench..."
parallel-ssh -h "$TEMP_HOSTS_FILE" -i "[ -f /users/USERNAME_PLACEHOLDER/cachelib_v1/opt/cachelib/bin/cachebench ] && echo 'v1 OK' || echo 'v1 FAILED'"
V1_EXIT_CODE=$?

# Check cachelib_v2  
echo "Checking cachelib_v2 cachebench..."
parallel-ssh -h "$TEMP_HOSTS_FILE" -i "[ -f /users/USERNAME_PLACEHOLDER/cachelib_v2/opt/cachelib/bin/cachebench ] && echo 'v2 OK' || echo 'v2 FAILED'"
V2_EXIT_CODE=$?

# Summary
echo "=== VERIFICATION SUMMARY ==="
if [ $PSSH_EXIT_CODE -eq 0 ]; then
    echo "✓ Connectivity: PASSED"
else
    echo "✗ Connectivity: FAILED"
fi

if [ $V1_EXIT_CODE -eq 0 ]; then
    echo "✓ CacheLib v1 (4mb slab): PASSED"
else
    echo "✗ CacheLib v1 (4mb slab): FAILED"
fi

if [ $V2_EXIT_CODE -eq 0 ]; then
    echo "✓ CacheLib v2 (1mb slab): PASSED"
else
    echo "✗ CacheLib v2 (1mb slab): FAILED"
fi

# Cleanup temporary files
rm -f "$TEMP_HOSTS_FILE"
EOF
)

# Create the host list for the verification script
HOST_LIST=""
for MACHINE in "${MACHINES[@]}"; do
    HOST_LIST="${HOST_LIST}${MACHINE}\n"
done

# Replace placeholders in verification commands
VERIFICATION_CMDS=$(echo "$VERIFICATION_CMDS" | sed "s/HOST_LIST_PLACEHOLDER/$HOST_LIST/g" | sed "s/USERNAME_PLACEHOLDER/$USERNAME/g")

# Execute verification commands on master host
ssh "$MASTER_HOST" "$VERIFICATION_CMDS"

# Cleanup temporary files
if [ -f "$SCRIPT_DIR/master_id_rsa.pub" ]; then
    rm "$SCRIPT_DIR/master_id_rsa.pub"
    echo "Cleaned up temporary SSH key file."
fi

echo "Verification complete. All temporary files cleaned up."