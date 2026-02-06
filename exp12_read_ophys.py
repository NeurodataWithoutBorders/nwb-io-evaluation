"""
Benchmark read patterns from ophys NWB files.

Implements access patterns from the chunking/compression evaluation plan:
- O1: Sequential full-frame batches (500 frames), 5 repeats
- O2: Sequential full-frame batches (5000 frames), 5 repeats
- O3: Single random frame, full spatial (50 samples)
- O4: Sequential 500-frame 16x16 spatial patch (50 random x,y locations)

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


RANDOM_SEED = 42


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

def benchmark_sequential_batches(
    input_filepath: Path, series_name: str, batch_size: int, n_repeats: int = 5
) -> dict[str, Any]:
    """
    Sequential full-frame batch reads through entire movie, repeated n_repeats times.

    Each repeat: drop cache, open file, read data[t:t+batch_size, :, :] sequentially.

    Returns dict with:
        - repeat_total_times: total read time per repeat (seconds), shape (n_repeats,)
        - batch_times: per-batch read times for all repeats, shape (n_repeats, n_batches)
        - batch_sizes: actual frames per batch, shape (n_batches,)
    """
    all_batch_times = []
    repeat_total_times = []

    for repeat in range(n_repeats):
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

        all_batch_times.append(batch_read_times)
        repeat_total_times.append(sum(batch_read_times))

    return {
        "repeat_total_times": np.array(repeat_total_times, dtype=np.float64),
        "batch_times": np.array(all_batch_times, dtype=np.float64),
        "batch_sizes": np.array(batch_sizes, dtype=np.int32),
    }


def benchmark_o1(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    O1: Sequential 500-frame full-frame batches through whole movie, 5 repeats.

    Derived from Suite2p registration/detection and NWB-to-binary conversion.
    """
    return benchmark_sequential_batches(input_filepath, series_name, batch_size=500, n_repeats=5)


def benchmark_o2(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    O2: Sequential 5000-frame full-frame batches through whole movie, 5 repeats.

    Larger batch variant of O1 for systems with large RAM.
    """
    return benchmark_sequential_batches(input_filepath, series_name, batch_size=5000, n_repeats=5)


def benchmark_o3(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    O3: Single random frame, full spatial extent, 50 samples.

    Each sample: drop cache, open file, read data[t, :, :] for a random t.
    Derived from Suite2p initial reference image construction and visualization.

    Returns dict with:
        - read_times: per-sample read times (seconds), shape (n_samples,)
        - frame_indices: which frame was read per sample, shape (n_samples,)
    """
    n_samples = 50

    # Get dimensions
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        n_frames = nwbfile.acquisition[series_name].data.shape[0]

    rng = np.random.default_rng(RANDOM_SEED)
    frame_indices = rng.integers(0, n_frames, size=n_samples)

    read_times = []

    for sample in range(n_samples):
        t = int(frame_indices[sample])

        drop_cache(input_filepath)

        with pynwb.NWBHDF5IO(input_filepath, "r") as io:
            nwbfile = io.read()
            series = nwbfile.acquisition[series_name]
            data = series.data

            start_time = time.perf_counter_ns()
            _ = data[t, :, :]
            end_time = time.perf_counter_ns()

        read_times.append((end_time - start_time) / 1e9)

    return {
        "read_times": np.array(read_times, dtype=np.float64),
        "frame_indices": np.array(frame_indices, dtype=np.int64),
    }


def benchmark_o4(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    O4: Sequential 500-frame batches of a 16x16 spatial patch through the whole movie,
    at 50 random (x, y) locations.

    Each location: drop cache, open file, read data[t:t+500, y:y+16, x:x+16] sequentially.
    Tests spatial sub-selection performance.

    Returns dict with:
        - location_total_times: total read time per location (seconds), shape (n_locations,)
        - batch_times: per-batch times for all locations, shape (n_locations, n_batches)
        - batch_sizes: actual frames per batch, shape (n_batches,)
        - xy_locations: (x, y) start coordinates, shape (n_locations, 2)
    """
    n_locations = 50
    batch_size = 500
    patch_size = 16

    # Get dimensions
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        data_shape = nwbfile.acquisition[series_name].data.shape
        n_frames = data_shape[0]
        dim_y = data_shape[1]
        dim_x = data_shape[2]

    rng = np.random.default_rng(RANDOM_SEED)
    x_starts = rng.integers(0, dim_x - patch_size + 1, size=n_locations)
    y_starts = rng.integers(0, dim_y - patch_size + 1, size=n_locations)

    n_batches = math.ceil(n_frames / batch_size)
    all_batch_times = []
    location_total_times = []

    for loc in range(n_locations):
        x0 = int(x_starts[loc])
        y0 = int(y_starts[loc])
        x1 = x0 + patch_size
        y1 = y0 + patch_size

        drop_cache(input_filepath)

        with pynwb.NWBHDF5IO(input_filepath, "r") as io:
            nwbfile = io.read()
            series = nwbfile.acquisition[series_name]
            data = series.data

            batch_read_times = []
            batch_sizes = []

            for i in range(n_batches):
                t0 = i * batch_size
                t1 = min(t0 + batch_size, n_frames)

                start_time = time.perf_counter_ns()
                _ = data[t0:t1, y0:y1, x0:x1]
                end_time = time.perf_counter_ns()

                batch_read_times.append((end_time - start_time) / 1e9)
                batch_sizes.append(t1 - t0)

        all_batch_times.append(batch_read_times)
        location_total_times.append(sum(batch_read_times))

    return {
        "location_total_times": np.array(location_total_times, dtype=np.float64),
        "batch_times": np.array(all_batch_times, dtype=np.float64),
        "batch_sizes": np.array(batch_sizes, dtype=np.int32),
        "xy_locations": np.column_stack([x_starts, y_starts]),
    }


# =============================================================================
# Use case registry
# =============================================================================

USE_CASES = {
    "o1": {
        "func": benchmark_o1,
        "description": "Sequential full-frame batches (500 frames, 5 repeats)",
        "group_name": "o1_sequential_500",
    },
    "o2": {
        "func": benchmark_o2,
        "description": "Sequential full-frame batches (5000 frames, 5 repeats)",
        "group_name": "o2_sequential_5000",
    },
    "o3": {
        "func": benchmark_o3,
        "description": "Single random frame, full spatial (50 samples)",
        "group_name": "o3_random_frame",
    },
    "o4": {
        "func": benchmark_o4,
        "description": "Sequential 500-frame 16x16 patch (50 random locations)",
        "group_name": "o4_spatial_patch",
    },
}


# =============================================================================
# Output writing
# =============================================================================

def write_results(output_file: h5py.File, group_name: str, results: dict[str, Any]):
    """Write benchmark results to an open HDF5 file."""
    grp = output_file.create_group(group_name)

    for key, value in results.items():
        if isinstance(value, np.ndarray):
            grp.create_dataset(key, data=value)
        else:
            grp.attrs[key] = value


def print_summary(use_case: str, results: dict[str, Any]):
    """Print summary of benchmark results."""
    if "repeat_total_times" in results:
        # O1, O2
        times = results["repeat_total_times"]
        print(f"  Repeats: {len(times)}")
        print(f"  Total time per repeat: mean={np.mean(times):.3f}s std={np.std(times):.3f}s")
        print(f"  Min={np.min(times):.3f}s Max={np.max(times):.3f}s")
    elif "read_times" in results:
        # O3
        times = results["read_times"]
        print(f"  Samples: {len(times)}")
        print(f"  Per-read: mean={np.mean(times):.4f}s std={np.std(times):.4f}s")
        print(f"  Total: {np.sum(times):.3f}s")
    elif "location_total_times" in results:
        # O4
        times = results["location_total_times"]
        print(f"  Locations: {len(times)}")
        print(f"  Total time per location: mean={np.mean(times):.3f}s std={np.std(times):.3f}s")
        print(f"  Min={np.min(times):.3f}s Max={np.max(times):.3f}s")


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

            results = uc_info["func"](input_filepath, series_name)

            print_summary(use_case, results)

            write_results(output_file, uc_info["group_name"], results)

    print(f"\nResults written to {output_filepath}")


if __name__ == "__main__":
    main()
