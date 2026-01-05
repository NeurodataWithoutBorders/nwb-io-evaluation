#!/usr/bin/env python3
"""Check which NWB files in a directory can be opened with PyNWB."""

import sys
from pathlib import Path

import pynwb


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python exp12_check_nwb_files.py <directory>")
        sys.exit(1)

    directory = Path(sys.argv[1])
    if not directory.is_dir():
        print(f"Error: {directory} is not a directory")
        sys.exit(1)

    nwb_files = sorted(directory.glob("*.nwb"))
    if not nwb_files:
        print(f"No .nwb files found in {directory}")
        sys.exit(0)

    failed_files = []
    for filepath in nwb_files:
        try:
            with pynwb.NWBHDF5IO(filepath, "r") as io:
                io.read()
        except Exception as e:
            failed_files.append((filepath, str(e)))
            print(f"FAILED: {filepath.name} - {e}")

    print()
    print(f"Checked {len(nwb_files)} files")
    if failed_files:
        print(f"Failed to open {len(failed_files)} files:")
        for filepath, error in failed_files:
            print(f"  {filepath.name}: {error}")
    else:
        print("All files opened successfully")


if __name__ == "__main__":
    main()
