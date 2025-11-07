"""
Script to clean up failed experiment directories.

Scans multiple work directories for subdirs (optionally with specific prefix),
finds those with rc.txt content != 0 (failed experiments),
and cleans up all files except config.json and meta.json.

Configuration:
- work_dirs: List of directories to scan
- prefix_filter: Optional prefix filter (e.g., 'meta_memcache'), set to None to process all subdirs
"""

import os
import glob
import shutil
from pathlib import Path


def read_rc_file(subdir_path):
    """Read the content of rc.txt file and return as integer."""
    rc_file = os.path.join(subdir_path, "rc.txt")
    try:
        with open(rc_file, 'r') as f:
            content = f.read().strip()
            return int(content)
    except (FileNotFoundError, ValueError, IOError):
        # If rc.txt doesn't exist or can't be parsed, assume running or success
        return 0


def get_failed_subdirs(work_dirs, prefix_filter=None):
    """Find all subdirs with specified prefix that have rc.txt != 0."""
    failed_subdirs = []
    
    for work_dir in work_dirs:
        if not os.path.exists(work_dir):
            print(f"Warning: Directory {work_dir} does not exist, skipping...")
            continue
            
        # Use prefix filter if provided, otherwise process all subdirs
        if prefix_filter:
            pattern = os.path.join(work_dir, f"{prefix_filter}*")
            subdirs = glob.glob(pattern)
        else:
            # Get all subdirectories
            subdirs = [os.path.join(work_dir, d) for d in os.listdir(work_dir) 
                      if os.path.isdir(os.path.join(work_dir, d))]
        
        for subdir in subdirs:
            if os.path.isdir(subdir):
                rc_value = read_rc_file(subdir)
                if rc_value != 0:
                    failed_subdirs.append(subdir)
    
    return failed_subdirs


def list_files_to_keep(subdir_path):
    """List files that should be kept (config.json and meta.json)."""
    keep_files = ["config.json", "meta.json"]
    existing_keep_files = []
    
    for filename in keep_files:
        file_path = os.path.join(subdir_path, filename)
        if os.path.exists(file_path):
            existing_keep_files.append(filename)
    
    return existing_keep_files


def list_files_to_delete(subdir_path):
    """List all files/dirs to be deleted (everything except config.json and meta.json)."""
    keep_files = {"config.json", "meta.json"}
    to_delete = []
    
    try:
        for item in os.listdir(subdir_path):
            if item not in keep_files:
                item_path = os.path.join(subdir_path, item)
                to_delete.append(item_path)
    except OSError as e:
        print(f"Error listing directory {subdir_path}: {e}")
    
    return to_delete


def clean_subdir(subdir_path, dry_run=True):
    """Clean up a subdirectory, keeping only config.json and meta.json."""
    files_to_delete = list_files_to_delete(subdir_path)
    files_to_keep = list_files_to_keep(subdir_path)
    
    if dry_run:
        print(f"\nSubdir: {subdir_path}")
        print(f"  Files to keep: {files_to_keep}")
        print(f"  Items to delete: {len(files_to_delete)}")
        for item in files_to_delete:
            item_name = os.path.basename(item)
            item_type = "DIR" if os.path.isdir(item) else "FILE"
            print(f"    {item_type}: {item_name}")
    else:
        print(f"Cleaning {subdir_path}...")
        deleted_count = 0
        for item in files_to_delete:
            try:
                if os.path.isdir(item):
                    shutil.rmtree(item)
                else:
                    os.remove(item)
                deleted_count += 1
            except OSError as e:
                print(f"  Error deleting {item}: {e}")
        
        print(f"  Deleted {deleted_count} items, kept {len(files_to_keep)} files")


def main():
    # Configuration - modify these as needed
    work_dirs = ["../work_dir_meta"]  # List of directories to scan
    prefix_filter = "meta_memcache"  # Set to None to process all subdirs, or specify prefix like "meta_memcache"
    
    # Check if any work_dirs exist
    existing_dirs = [d for d in work_dirs if os.path.exists(d)]
    if not existing_dirs:
        print(f"Error: None of the specified directories exist: {work_dirs}")
        return
    
    filter_msg = f" with prefix '{prefix_filter}'" if prefix_filter else " (all subdirectories)"
    print(f"Scanning {len(existing_dirs)} directories for failed experiments{filter_msg}...")
    for work_dir in existing_dirs:
        print(f"  - {work_dir}")
    
    # Find failed subdirs
    failed_subdirs = get_failed_subdirs(work_dirs, prefix_filter)
    
    print(f"\nFound {len(failed_subdirs)} failed experiment directories to clean:")
    for subdir in failed_subdirs:
        rc_value = read_rc_file(subdir)
        parent_dir = os.path.basename(os.path.dirname(subdir))
        subdir_name = os.path.basename(subdir)
        print(f"  {parent_dir}/{subdir_name} (rc.txt = {rc_value})")
    
    if not failed_subdirs:
        print("No failed experiments found. Nothing to clean.")
        return
    
    # First pass: dry run to show what will be deleted
    print(f"\n{'='*60}")
    print("DRY RUN - Preview of files to be cleaned:")
    print(f"{'='*60}")
    
    for subdir in failed_subdirs:
        clean_subdir(subdir, dry_run=True)
    
    # Ask for confirmation
    print(f"\n{'='*60}")
    response = input(f"Do you want to proceed with cleaning {len(failed_subdirs)} directories? (y/N): ")
    
    if response.lower() in ['y', 'yes']:
        print(f"\n{'='*60}")
        print("ACTUAL CLEANUP - Deleting files:")
        print(f"{'='*60}")
        
        for subdir in failed_subdirs:
            clean_subdir(subdir, dry_run=False)
        
        print(f"\nCleanup completed for {len(failed_subdirs)} directories!")
    else:
        print("Cleanup cancelled.")


if __name__ == "__main__":
    main()