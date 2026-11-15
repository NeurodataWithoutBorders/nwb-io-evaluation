#!/usr/bin/env python3
"""
Check which NWB files in a directory can be opened with PyNWB, which write outputs are missing
for the given datasets, and which ".partial" files incomplete writes left in the directory.

For each dataset label, the expected outputs are <label>_<config_id>.nwb and
stats_<label>_<config_id>.tsv for every configuration in scripts/exp12_configs_<label>.tsv,
plus <label>_unchunked.nwb and stats_<label>_unchunked.tsv.
"""

import sys
from pathlib import Path

import pynwb

from exp12_configs import config_table_path, load_configs


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python exp12_check_nwb_files.py <directory> <label> [<label> ...]")
        print("Example: python exp12_check_nwb_files.py write_output ophys1 ophys2 ophys3")
        sys.exit(1)

    directory = Path(sys.argv[1])
    labels = sys.argv[2:]
    if not directory.is_dir():
        print(f"Error: {directory} is not a directory")
        sys.exit(1)

    # A write that was interrupted or whose data did not match the source leaves a .partial file
    partial_files = sorted(directory.glob("*.partial"))
    if partial_files:
        total_gb = sum(p.stat().st_size for p in partial_files) / 1e9
        print(f"{len(partial_files)} .partial files from incomplete writes ({total_gb:.1f} GB):")
        for filepath in partial_files:
            print(f"  {filepath.name}")
        print()

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

    # Check for missing write outputs for each dataset
    print()
    print("Checking for missing write outputs...")
    for label in labels:
        config_ids = [c.config_id for c in load_configs(config_table_path(label))] + ["unchunked"]
        print(f"\n{label}:")
        expected = (("NWB", "{label}_{config_id}.nwb"), ("stats", "stats_{label}_{config_id}.tsv"))
        for kind, name_format in expected:
            missing = [
                config_id for config_id in config_ids
                if not (directory / name_format.format(label=label, config_id=config_id)).exists()
            ]
            if missing:
                print(f"  Missing {len(missing)} {kind} files: {missing}")
            else:
                print(f"  No missing {kind} files")


if __name__ == "__main__":
    main()
