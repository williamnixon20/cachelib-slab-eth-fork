import os
import json

def _read_configs():
    """Read configuration from configs.json file."""
    try:
        # Get the directory where this const.py file is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # configs.json is in the same directory
        config_file = os.path.join(script_dir, "configs.json")
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Check that required fields exist
        if 'wget_trace_path' not in config:
            raise ValueError(f"Missing required field 'wget_trace_path' in {config_file}")
        if 'local_trace_file_dir' not in config:
            raise ValueError(f"Missing required field 'local_trace_file_dir' in {config_file}")
        
        return config['wget_trace_path'], config['local_trace_file_dir']
    except (FileNotFoundError, IOError) as e:
        raise RuntimeError(f"Could not read configs.json ({e}). Please ensure {config_file} exists and is readable.")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in configs.json ({e}). Please check the file format.")

# Read configuration paths dynamically
WGET_PATH, TRACE_FILE_PATH = _read_configs()

CACHEBENCH_BINARY_PATH = "/users/Hongshu/cachelib_v1/opt/cachelib/bin/cachebench"
CACHEBENCH_BINARY_PATH2 = "/users/Hongshu/cachelib_v2/opt/cachelib/bin/cachebench"

VALID_ALLOCATOR_REBALANCE_COMBINATIONS = {
    "SIMPLE2Q": set(["marginal-hits-old", "marginal-hits-new", "free-mem", "disabled", "hits", "tail-age", "lama", 'eviction-rate']),
    "LRU2Q": set(["marginal-hits-old", "marginal-hits-new", "free-mem", "disabled", "hits", "tail-age", "lama", 'eviction-rate']),
    "TINYLFU": set(["free-mem", "disabled", "hits", "tail-age", 'eviction-rate', "lama"]),
    "TINYLFUTail": set(["marginal-hits-old", "marginal-hits-new"]),
}