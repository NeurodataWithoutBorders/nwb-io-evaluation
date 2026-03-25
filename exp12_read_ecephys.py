"""
Benchmark read patterns from ecephys NWB files.

Implements access patterns from the chunking/compression evaluation plan:
- E1: Sequential 30,000-sample all-channel batches, 5 repeats
- E2: Sequential 3,000,000-sample all-channel batches, 5 repeats
- E3: 20 random batches of 15,000 samples, all channels (50 samples)
- E4: 30,000 samples x 32 channels at random (time, channel) position (50 samples)

Runs all registered use cases for a given config file.

Usage:
    python exp12_read_ecephys.py <config_number> <input_dir> <series_name> <output_label> <output_dir>

Arguments:
    config_number: Config number (1-indexed, corresponds to line+1 in config file)
    input_dir: Path to input NWB files
    series_name: Name of the ElectricalSeries in the NWB file to read
    output_label: Label for this input NWB file experiment (e.g., "ecephys1")
    output_dir: Path to directory to write output HDF5 files

Example:
    python exp12_read_ecephys.py 1 /path/to/nwb ElectricalSeries ecephys1 /path/to/results
"""

import math
import os
from pathlib import Path
import sys
import time
from typing import Any

import b2h5py.auto  # noqa: F401 - auto-use fast slicing for blosc2-compressed datasets
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
    Sequential all-channel batch reads through entire recording, repeated n_repeats times.

    Each repeat: drop cache, open file, read data[t:t+batch_size, :] sequentially.

    Returns dict with:
        - repeat_total_times: total read time per repeat (seconds), shape (n_repeats,)
        - batch_times: per-batch read times for all repeats, shape (n_repeats, n_batches)
        - batch_sizes: actual samples per batch, shape (n_batches,)
    """
    all_batch_times = []
    repeat_total_times = []

    for repeat in range(n_repeats):
        drop_cache(input_filepath)

        with pynwb.NWBHDF5IO(input_filepath, "r") as io:
            nwbfile = io.read()
            series = nwbfile.acquisition[series_name]
            data = series.data

            n_samples = data.shape[0]
            n_batches = math.ceil(n_samples / batch_size)

            batch_read_times = []
            batch_sizes = []

            for i in range(n_batches):
                t0 = i * batch_size
                t1 = min(t0 + batch_size, n_samples)

                start_time = time.perf_counter_ns()
                _ = data[t0:t1, :]
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


def benchmark_e1(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    E1: Sequential 30,000-sample all-channel batches through whole recording, 5 repeats.

    Derived from SpikeInterface chunk-based processing (default chunk_duration="1s" at 30kHz).
    """
    return benchmark_sequential_batches(input_filepath, series_name, batch_size=30000, n_repeats=5)


def benchmark_e2(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    E2: Sequential 3,000,000-sample all-channel batches through whole recording, 5 repeats.

    Larger batch variant of E1 for systems with large RAM.
    """
    return benchmark_sequential_batches(input_filepath, series_name, batch_size=3000000, n_repeats=5)


def benchmark_e3(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    E3: 20 random batches of 15,000 samples, all channels, 50 samples.

    Each sample: drop cache, open file, read 20 random 15,000-sample batches.
    Derived from SpikeInterface noise estimation and Kilosort whitening.

    Returns dict with:
        - sample_total_times: total read time per sample (seconds), shape (n_samples,)
        - batch_times: per-batch read times for all samples, shape (n_samples, 20)
        - batch_starts: start indices for all batches, shape (n_samples, 20)
    """
    n_samples = 50
    n_batches_per_sample = 20
    batch_size = 15000

    # Get dimensions
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        n_total_samples = nwbfile.acquisition[series_name].data.shape[0]

    rng = np.random.default_rng(RANDOM_SEED)
    all_batch_starts = rng.integers(
        0, n_total_samples - batch_size, size=(n_samples, n_batches_per_sample)
    )

    all_batch_times = []
    sample_total_times = []

    for sample in range(n_samples):
        drop_cache(input_filepath)

        with pynwb.NWBHDF5IO(input_filepath, "r") as io:
            nwbfile = io.read()
            series = nwbfile.acquisition[series_name]
            data = series.data

            batch_read_times = []

            for b in range(n_batches_per_sample):
                t0 = int(all_batch_starts[sample, b])
                t1 = t0 + batch_size

                start_time = time.perf_counter_ns()
                _ = data[t0:t1, :]
                end_time = time.perf_counter_ns()

                batch_read_times.append((end_time - start_time) / 1e9)

        all_batch_times.append(batch_read_times)
        sample_total_times.append(sum(batch_read_times))

    return {
        "sample_total_times": np.array(sample_total_times, dtype=np.float64),
        "batch_times": np.array(all_batch_times, dtype=np.float64),
        "batch_starts": all_batch_starts,
    }


def benchmark_e4(input_filepath: Path, series_name: str) -> dict[str, Any]:
    """
    E4: 30,000 samples x 32 channels at random (time, channel) position, 50 samples.

    Each sample: drop cache, open file, read data[t:t+30000, c:c+32].
    Tests channel sub-selection performance.

    Returns dict with:
        - read_times: per-sample read times (seconds), shape (n_samples,)
        - time_starts: start sample index per sample, shape (n_samples,)
        - channel_starts: start channel index per sample, shape (n_samples,)
    """
    n_samples = 50
    time_span = 30000
    channel_span = 32

    # Get dimensions
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        data_shape = nwbfile.acquisition[series_name].data.shape
        n_total_samples = data_shape[0]
        n_channels = data_shape[1]

    rng = np.random.default_rng(RANDOM_SEED)
    time_starts = rng.integers(0, n_total_samples - time_span + 1, size=n_samples)
    channel_starts = rng.integers(0, n_channels - channel_span + 1, size=n_samples)

    read_times = []

    for sample in range(n_samples):
        t0 = int(time_starts[sample])
        t1 = t0 + time_span
        c0 = int(channel_starts[sample])
        c1 = c0 + channel_span

        drop_cache(input_filepath)

        with pynwb.NWBHDF5IO(input_filepath, "r") as io:
            nwbfile = io.read()
            series = nwbfile.acquisition[series_name]
            data = series.data

            start_time = time.perf_counter_ns()
            _ = data[t0:t1, c0:c1]
            end_time = time.perf_counter_ns()

        read_times.append((end_time - start_time) / 1e9)

    return {
        "read_times": np.array(read_times, dtype=np.float64),
        "time_starts": np.array(time_starts, dtype=np.int64),
        "channel_starts": np.array(channel_starts, dtype=np.int64),
    }


# =============================================================================
# Use case registry
# =============================================================================

USE_CASES = {
    "e1": {
        "func": benchmark_e1,
        "description": "Sequential all-channel batches (30,000 samples, 5 repeats)",
        "group_name": "e1_sequential_30k",
    },
    "e2": {
        "func": benchmark_e2,
        "description": "Sequential all-channel batches (3,000,000 samples, 5 repeats)",
        "group_name": "e2_sequential_3M",
    },
    "e3": {
        "func": benchmark_e3,
        "description": "20 random 15,000-sample all-channel batches (50 samples)",
        "group_name": "e3_random_batches",
    },
    "e4": {
        "func": benchmark_e4,
        "description": "30,000 samples x 32 channels at random position (50 samples)",
        "group_name": "e4_channel_subset",
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
        # E1, E2
        times = results["repeat_total_times"]
        print(f"  Repeats: {len(times)}")
        print(f"  Total time per repeat: mean={np.mean(times):.3f}s std={np.std(times):.3f}s")
        print(f"  Min={np.min(times):.3f}s Max={np.max(times):.3f}s")
    elif "sample_total_times" in results:
        # E3
        times = results["sample_total_times"]
        print(f"  Samples: {len(times)}")
        print(f"  Total time per sample: mean={np.mean(times):.3f}s std={np.std(times):.3f}s")
        print(f"  Min={np.min(times):.3f}s Max={np.max(times):.3f}s")
    elif "read_times" in results:
        # E4
        times = results["read_times"]
        print(f"  Samples: {len(times)}")
        print(f"  Per-read: mean={np.mean(times):.4f}s std={np.std(times):.4f}s")
        print(f"  Total: {np.sum(times):.3f}s")


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
            sys.stdout.flush()

            write_results(output_file, uc_info["group_name"], results)

    print(f"\nResults written to {output_filepath}")


if __name__ == "__main__":
    main()
