"""Tests for cache clearing effectiveness using raw file reads and PyNWB reads.

Results from one run on Lawrencium:

=== Raw file read ===
Cold read:   0.972s (1.1 GB/s)
Warm read:   0.420s (2.6 GB/s)
After drop:  0.953s (1.1 GB/s)
Cache speedup: 2.31x
After drop vs cold: 2.0% difference
Cache clearing works: True

=== PyNWB read (500 frames) ===
Data shape: (500, 796, 512), size: 407.6 MB
Cold read:   0.620s (0.66 GB/s)
Warm read:   0.407s (1.00 GB/s)
After drop:  0.607s (0.67 GB/s)
Cache speedup: 1.52x
After drop vs cold: 2.1% difference
Cache clearing works: True

"""
import os
import time

from pynwb import NWBHDF5IO


def drop_cache(filepath):
    fd = os.open(filepath, os.O_RDONLY)
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    os.close(fd)


def test_cache_clearing_raw(filepath, read_bytes=1024**3):
    """Test cache clearing effectiveness on first 1 GB using raw file read."""

    print("=== Raw file read ===")

    # Cold read
    drop_cache(filepath)
    t0 = time.perf_counter_ns()
    with open(filepath, 'rb') as f:
        _ = f.read(read_bytes)
    cold = (time.perf_counter_ns() - t0) / 1e9

    # Warm read (cached)
    t0 = time.perf_counter_ns()
    with open(filepath, 'rb') as f:
        _ = f.read(read_bytes)
    warm = (time.perf_counter_ns() - t0) / 1e9

    # Clear and read again
    drop_cache(filepath)
    t0 = time.perf_counter_ns()
    with open(filepath, 'rb') as f:
        _ = f.read(read_bytes)
    after_drop = (time.perf_counter_ns() - t0) / 1e9

    print(f"Cold read:   {cold:.3f}s ({read_bytes/1e9/cold:.1f} GB/s)")
    print(f"Warm read:   {warm:.3f}s ({read_bytes/1e9/warm:.1f} GB/s)")
    print(f"After drop:  {after_drop:.3f}s ({read_bytes/1e9/after_drop:.1f} GB/s)")
    print(f"Cache speedup: {cold/warm:.2f}x")
    print(f"After drop vs cold: {abs(after_drop - cold)/cold * 100:.1f}% difference")
    print(f"Cache clearing works: {after_drop > (cold + warm) / 2}")
    print()


def test_cache_clearing_pynwb(filepath, series_name="TwoPhotonSeries", num_frames=500):
    """Test cache clearing effectiveness reading TwoPhotonSeries via PyNWB."""

    print(f"=== PyNWB read ({num_frames} frames) ===")

    # Cold read
    drop_cache(filepath)
    t0 = time.perf_counter_ns()
    with NWBHDF5IO(filepath, "r") as io:
        nwbfile = io.read()
        data = nwbfile.acquisition[series_name].data[:num_frames]
    cold = (time.perf_counter_ns() - t0) / 1e9
    data_bytes = data.nbytes

    # Warm read (cached)
    t0 = time.perf_counter_ns()
    with NWBHDF5IO(filepath, "r") as io:
        nwbfile = io.read()
        data = nwbfile.acquisition[series_name].data[:num_frames]
    warm = (time.perf_counter_ns() - t0) / 1e9

    # Clear and read again
    drop_cache(filepath)
    t0 = time.perf_counter_ns()
    with NWBHDF5IO(filepath, "r") as io:
        nwbfile = io.read()
        data = nwbfile.acquisition[series_name].data[:num_frames]
    after_drop = (time.perf_counter_ns() - t0) / 1e9

    print(f"Data shape: {data.shape}, size: {data_bytes/1e6:.1f} MB")
    print(f"Cold read:   {cold:.3f}s ({data_bytes/1e9/cold:.2f} GB/s)")
    print(f"Warm read:   {warm:.3f}s ({data_bytes/1e9/warm:.2f} GB/s)")
    print(f"After drop:  {after_drop:.3f}s ({data_bytes/1e9/after_drop:.2f} GB/s)")
    print(f"Cache speedup: {cold/warm:.2f}x")
    print(f"After drop vs cold: {abs(after_drop - cold)/cold * 100:.1f}% difference")
    print(f"Cache clearing works: {after_drop > (cold + warm) / 2}")
    print()


if __name__ == "__main__":
    filepath = "write_output/ophys1_Config001.nwb"

    test_cache_clearing_raw(filepath)
    test_cache_clearing_pynwb(filepath)