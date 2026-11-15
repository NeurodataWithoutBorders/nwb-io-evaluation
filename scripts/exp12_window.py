"""
Contiguous time windows taken from the middle of each source recording.

The write scripts that build test NWB files from the raw binary and TIFF data all take their
data from these windows, so the unchunked baseline and every chunking and compression
configuration of a dataset hold the same samples. Each window length is a whole number of chunk
time extents for the chunk shapes EC1 to EC5 and C1 to C5 and for the hdmf default ophys shapes.
The h5py chunk guesses and the hdmf default ecephys shape do not divide the window, so those
datasets end in a chunk that is partly filled along time.
"""

from typing import Any

# 60 min at 30 kHz (59.998 min for ecephys3, which records at 30001.065 Hz). Divisible by the
# time extent of every ecephys chunk shape (1,500 to 180,000 samples).
ECEPHYS_WINDOW_SAMPLES = 108_000_000

# 2 x 20,480 frames: 45.5 min for ophys1 at 15 Hz, 22.7 min for ophys2 and ophys3 at 30.075 Hz.
# Divisible by the time extent of every ophys chunk shape (2 to 20,480 frames).
OPHYS_WINDOW_FRAMES = 40_960


def centered_window(n_total: int, n_window: int) -> tuple[int, int]:
    """Return (start, stop) of the n_window samples centered in a series of n_total samples."""
    if n_window > n_total:
        raise ValueError(f"Window of {n_window} samples is longer than the series ({n_total} samples)")
    start = (n_total - n_window) // 2
    return start, start + n_window


def windowed_timing(series: Any, start: int, stop: int) -> dict[str, Any]:
    """Timing arguments for a TimeSeries holding samples start:stop of the given series.

    A series with timestamps keeps the timestamps of the window. A series with a starting time
    and rate keeps its rate, and its starting time moves to the first sample of the window, so
    the window stays aligned with the rest of the file (trials, behavior, other series).
    """
    if series.timestamps is not None:
        return {"starting_time": None, "rate": None, "timestamps": series.timestamps[start:stop]}
    return {
        "starting_time": series.starting_time + start / series.rate,
        "rate": series.rate,
        "timestamps": None,
    }
