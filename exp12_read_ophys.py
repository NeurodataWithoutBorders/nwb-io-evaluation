"""
Benchmark read patterns from ophys NWB files.

Implements access patterns from the chunking/compression evaluation plan:
- O1: Sequential full-frame batches (500 frames at a time)

Runs all registered use cases for a given config file.

Usage:
    python exp12_read_ophys.py <config_number> <input_dir> <series_name> <output_label> <output_dir>

Arguments:
    config_number: Config number (1-indexed, corresponds to line+1 in config file)
    input_dir: Path to input NWB files
    series_name: Name of the TwoPhotonSeries in the NWB file to read
    output_label: Label for this input NWB file experiment (e.g., "ophys1")
    output_dir: Path to directory to write output HDF5 files

Example:
    python exp12_read_ophys.py 1 /path/to/nwb TwoPhotonSeries ophys1 /path/to/results
"""

import math
import os
from pathlib import Path
import sys
import time
from typing import Any

import h5py
import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
import numpy as np
import pynwb


# =============================================================================
# Cache management
# =============================================================================

def drop_cache(filepath):
    """Drop the OS file cache for the given file."""
    fd = os.open(filepath, os.O_RDONLY)
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    os.close(fd)


# =============================================================================
# Use case implementations
# =============================================================================

def benchmark_o1(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    O1: Sequential full-frame batch reads through entire movie.

    Reads data[t:t+500, :, :] sequentially from start to end.
    Derived from Suite2p registration/detection pattern.

    Returns dict with:
        - batch_times: per-batch read times (seconds)
        - batch_sizes: actual frames per batch
    """
    batch_size = 500

    drop_cache(input_filepath)

    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        series = nwbfile.acquisition[series_name]
        data = series.data

        n_frames = data.shape[0]
        n_batches = math.ceil(n_frames / batch_size)

        batch_read_times = []
        batch_sizes = []

        for i in range(n_batches):
            t0 = i * batch_size
            t1 = min(t0 + batch_size, n_frames)

            start_time = time.perf_counter_ns()
            _ = data[t0:t1, :, :]
            end_time = time.perf_counter_ns()

            batch_read_times.append((end_time - start_time) / 1e9)
            batch_sizes.append(t1 - t0)

    return {
        "batch_times": np.array(batch_read_times, dtype=np.float64),
        "batch_sizes": np.array(batch_sizes, dtype=np.int32),
    }


# =============================================================================
# Use case registry
# =============================================================================

USE_CASES = {
    "o1": {
        "func": benchmark_o1,
        "description": "Sequential full-frame batches (500 frames)",
        "group_name": "o1_sequential_batches",
    },
    # Add future use cases here:
    # "p2": {
    #     "func": benchmark_p2,
    #     "description": "Evenly spaced frame sampling (~300 frames)",
    #     "group_name": "p2_evenly_spaced",
    # },
    # "p3": {
    #     "func": benchmark_p3,
    #     "description": "Spatial patch, all time (96x96)",
    #     "group_name": "p3_spatial_patch",
    # },
    # "p4": {
    #     "func": benchmark_p4,
    #     "description": "Single pixel timeseries (128 pixels)",
    #     "group_name": "p4_pixel_timeseries",
    # },
}


# =============================================================================
# Output writing
# =============================================================================

def write_results(output_file: h5py.File, group_name: str, results: dict[str, Any]):
    """Write benchmark results to an open HDF5 file."""
    grp = output_file.create_group(group_name)

    # Write arrays as datasets, scalars as attributes
    for key, value in results.items():
        if isinstance(value, np.ndarray):
            grp.create_dataset(key, data=value)
        else:
            grp.attrs[key] = value

    # Add computed summary statistics
    if "batch_times" in results:
        batch_times = results["batch_times"]
        grp.attrs["total_time_s"] = float(np.sum(batch_times))
        grp.attrs["mean_time_s"] = float(np.mean(batch_times))
        grp.attrs["std_time_s"] = float(np.std(batch_times))
        grp.attrs["min_time_s"] = float(np.min(batch_times))
        grp.attrs["max_time_s"] = float(np.max(batch_times))


def print_summary(results: dict[str, Any]):
    """Print summary of benchmark results."""
    if "batch_times" in results:
        batch_times = results["batch_times"]
        total = np.sum(batch_times)
        mean = np.mean(batch_times)
        std = np.std(batch_times)
        print(f"  Iterations: {len(batch_times)}")
        print(f"  Total time: {total:.3f}s")
        print(f"  Mean time: {mean:.4f}s (std: {std:.4f}s)")


# =============================================================================
# Main
# =============================================================================

def main():
    if len(sys.argv) < 6:
        print(__doc__)
        print("\nRegistered use cases:")
        for name, info in USE_CASES.items():
            print(f"  {name}: {info['description']}")
        sys.exit(1)

    config_number = int(sys.argv[1])
    input_dir = Path(sys.argv[2])
    series_name = sys.argv[3]
    output_label = sys.argv[4]
    output_dir = Path(sys.argv[5])

    input_filepath = input_dir / f"{output_label}_Config{config_number:03d}.nwb"
    output_filepath = output_dir / f"read_{output_label}_Config{config_number:03d}.h5"

    if not input_filepath.exists():
        print(f"Input file not found: {input_filepath}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Benchmarking {len(USE_CASES)} use case(s)")
    print(f"  Input: {input_filepath}")
    print(f"  Output: {output_filepath}")

    with h5py.File(output_filepath, "w") as output_file:
        for use_case, uc_info in USE_CASES.items():
            print(f"\n[{use_case.upper()}] {uc_info['description']}")

            # Run benchmark
            results = uc_info["func"](input_filepath, series_name)

            # Print summary
            print_summary(results)

            # Write results
            write_results(output_file, uc_info["group_name"], results)

    print(f"\nResults written to {output_filepath}")


if __name__ == "__main__":
    main()
