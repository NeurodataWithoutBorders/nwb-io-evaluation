#!/usr/bin/env python3
"""Check which NWB files in a directory can be opened with PyNWB."""

import sys
from pathlib import Path

import pynwb


def parse_range(range_str: str) -> tuple[int, int]:
    """Parse a range string like '1-126' into start and end integers."""
    parts = range_str.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid range format: {range_str}. Expected format: start-end (e.g., 1-126)")
    return int(parts[0]), int(parts[1])


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python exp12_check_nwb_files.py <directory> <range>")
        print("Example: python exp12_check_nwb_files.py /path/to/files 1-126")
        sys.exit(1)

    directory = Path(sys.argv[1])
    try:
        range_start, range_end = parse_range(sys.argv[2])
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    if not directory.is_dir():
        print(f"Error: {directory} is not a directory")
        sys.exit(1)

    nwb_files = sorted(directory.glob("*.nwb"))
    if not nwb_files:
        print(f"No .nwb files found in {directory}")
        sys.exit(0)

    # Errors that indicate a corrupted/invalid file that should be deleted
    delete_errors = (
        "Missing NWB version in file. The file is not a valid NWB file.",
        "bad object header version number",
    )

    failed_files = []
    deleted_files = []
    for filepath in nwb_files:
        try:
            with pynwb.NWBHDF5IO(filepath, "r") as io:
                io.read()
        except Exception as e:
            error_msg = str(e)
            failed_files.append((filepath, error_msg))
            print(f"FAILED: {filepath.name} - {e}")

            # Delete files with specific corruption errors
            if any(err in error_msg for err in delete_errors):
                filepath.unlink()
                deleted_files.append(filepath)
                print(f"DELETED: {filepath.name}")

    print()
    print(f"Checked {len(nwb_files)} files")
    if deleted_files:
        print(f"Deleted {len(deleted_files)} corrupted files:")
        for filepath in deleted_files:
            print(f"  {filepath.name}")
    if failed_files:
        remaining_failures = [(f, e) for f, e in failed_files if f not in deleted_files]
        if remaining_failures:
            print(f"Failed to open {len(remaining_failures)} files (not deleted):")
            for filepath, error in remaining_failures:
                print(f"  {filepath.name}: {error}")
    if not failed_files:
        print("All files opened successfully")

    # Check for missing files in the specified range for each ophys experiment
    print()
    print(f"Checking for missing files in range {range_start}-{range_end}...")
    experiments = ["ophys1", "ophys2", "ophys3"]
    suffixes = [".nwb", ".txt"]

    for experiment in experiments:
        print(f"\n{experiment}:")
        for suffix in suffixes:
            missing = []
            for config_num in range(range_start, range_end + 1):
                padded = f"{config_num:03d}"
                pattern = f"*_{experiment}_Config{padded}{suffix}"
                matches = list(directory.glob(pattern))
                if not matches:
                    missing.append(config_num)
            if missing:
                print(f"  Missing {len(missing)} {suffix} files: {missing}")
            else:
                print(f"  No missing {suffix} files")


if __name__ == "__main__":
    main()
