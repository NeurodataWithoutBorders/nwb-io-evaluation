"""
Read patterns, read paths, and chunk cache settings of the read benchmark.

Each read task (exp12_read_schedule.py) reads one test file with one pattern, one read path, and
one chunk cache setting. A pattern runs as units: each unit opens the file, makes one or more
timed reads, and closes it.

    e1  ecephys  10 repeats, each reading the window in 30,000 sample batches, all channels
    e2  ecephys  10 repeats, each reading the window in 3,000,000 sample batches, all channels
    e3  ecephys  50 samples, each reading 20 batches of 15,000 samples at random times
    e4  ecephys  50 samples, each reading 30,000 samples of 32 channels at a random time and channel
    o1  ophys    10 repeats, each reading the window in 500 frame batches, whole frames
    o2  ophys    10 repeats, each reading the window in 5,000 frame batches, whole frames
    o3  ophys    50 samples, each reading one whole frame at a random time
    o4  ophys    50 samples, each reading a 16 x 16 pixel patch at a random position through the
                 window in 500 frame batches

Random positions come from a generator seeded with RANDOM_SEED and the pattern's name, so every
configuration of a dataset is read at the same positions, and the positions of one pattern are
independent of those of another.

The sequential patterns (e1, e2, o1, o2, o4) read through the whole window in each unit. A task of
one of these stops after MAX_SLOW_UNITS units each longer than SLOW_UNIT_THRESHOLD_S, and records
that it was cut short. A unit over the threshold reads its window at under 2.7 MB/s (ecephys,
27.6 GB in 170 minutes) or 3.6 to 5.6 MB/s (ophys, 21 to 33 GB in 100 minutes), and three such
units fit in the job's time limit (see exp12_read.py).

Read paths:

    h5py    h5py and the HDF5 filter pipeline (hdf5plugin), for every configuration
    b2h5py  b2h5py's fast slicing, for Blosc2 configurations only. It reads the needed blocks of
            each chunk with blosc2 directly from the file, so the HDF5 chunk cache does not apply

Chunk cache settings, set when the file is opened:

    1mib    1 MiB and 521 slots, the HDF5 1.x defaults, which h5py.File gives with HDF5 1.x
    8mib    8 MiB and 8191 slots, the HDF5 2.x defaults, which h5py.File gives with HDF5 2.x
    32mib   32 MiB and 8191 slots, which pynwb gives when it opens a file by path: hdmf sets
            32 MiB (hdmf.backends.hdf5.h5tools.RDCC_NBYTES) and leaves the slots at HDF5 2.x's
            default
    fitted  enough for one row of chunks across the full width of the data (the chunks of one
            chunk time extent), plus one chunk, with a prime number of slots at least 100 times
            the chunks it holds. A sequential read then keeps every chunk it reads in part until
            the next read call reads the rest, and decompresses each chunk once

A chunk larger than the cache is not cached, so a read call decompresses it again. The h5py path
of a sequential pattern (e1, e2, o1, o2, o4) on a chunked file whose chunk time extent does not
divide the batch reads part of some chunks in one call and the rest in the next, so it runs with
each of the four settings. Every other task runs with 32mib, the cache of a pynwb reader.
"""

from dataclasses import dataclass
import math

import numpy as np

RANDOM_SEED = 42
N_REPEATS = 10
N_SAMPLES = 50

# A sequential pattern stops after MAX_SLOW_UNITS units each longer than this many seconds.
SLOW_UNIT_THRESHOLD_S = {"ecephys": 170 * 60, "ophys": 100 * 60}
MAX_SLOW_UNITS = 3

READ_PATHS = ("h5py", "b2h5py")
CACHE_LABELS = ("1mib", "8mib", "32mib", "fitted")
DEFAULT_CACHE = "32mib"
CACHE_W0 = 0.75
FIXED_CACHES = {"1mib": (2**20, 521), "8mib": (8 * 2**20, 8191), "32mib": (32 * 2**20, 8191)}


@dataclass(frozen=True)
class Pattern:
    name: str
    modality: str
    kind: str  # sequential, random_batches, subregion, frame, or patch
    batch: int  # time steps per read
    width: int = 0  # channels (subregion) or pixels per side (patch)
    reads_per_sample: int = 1  # reads per unit of a random_batches pattern

    @property
    def sequential(self) -> bool:
        """Whether the pattern reads through the window in consecutive read calls."""
        return self.kind in ("sequential", "patch")

    @property
    def n_units(self) -> int:
        return N_REPEATS if self.kind == "sequential" else N_SAMPLES


PATTERNS = {
    "e1": Pattern("e1", "ecephys", "sequential", 30_000),
    "e2": Pattern("e2", "ecephys", "sequential", 3_000_000),
    "e3": Pattern("e3", "ecephys", "random_batches", 15_000, reads_per_sample=20),
    "e4": Pattern("e4", "ecephys", "subregion", 30_000, width=32),
    "o1": Pattern("o1", "ophys", "sequential", 500),
    "o2": Pattern("o2", "ophys", "sequential", 5_000),
    "o3": Pattern("o3", "ophys", "frame", 1),
    "o4": Pattern("o4", "ophys", "patch", 500, width=16),
}


def modality_patterns(modality: str) -> list[Pattern]:
    return [p for p in PATTERNS.values() if p.modality == modality]


def chunks_across(shape: tuple[int, ...], chunks: tuple[int, ...]) -> int:
    """Chunks in one chunk time extent across the full width of the data."""
    return math.prod(math.ceil(n / c) for n, c in zip(shape[1:], chunks[1:]))


def next_prime(n: int) -> int:
    """The smallest prime at least n."""
    def is_prime(k: int) -> bool:
        return k >= 2 and all(k % d for d in range(2, math.isqrt(k) + 1))

    while not is_prime(n):
        n += 1
    return n


def cache_setting(label: str, shape: tuple[int, ...], chunks: tuple[int, ...] | None,
                  itemsize: int) -> tuple[int, int]:
    """(rdcc_nbytes, rdcc_nslots) of a cache setting for a dataset of this shape and chunks."""
    if label in FIXED_CACHES:
        return FIXED_CACHES[label]
    if label != "fitted" or chunks is None:
        raise ValueError(f"No cache setting {label!r} for chunks {chunks}")
    held = chunks_across(shape, chunks) + 1
    return held * math.prod(chunks) * itemsize, next_prime(100 * held)


def caches_for(pattern: Pattern, path: str, chunks: tuple[int, ...] | None) -> tuple[str, ...]:
    """The cache settings a task of this pattern, read path, and chunk shape runs with."""
    if path == "h5py" and chunks is not None and pattern.sequential and pattern.batch % chunks[0]:
        return CACHE_LABELS
    return (DEFAULT_CACHE,)


Selection = list[int | list[int]]  # per dimension, an index or [start, stop]


def unit_positions(pattern: Pattern, shape: tuple[int, ...]) -> np.ndarray:
    """Random positions of each unit, as rows; empty rows for the sequential repeats."""
    rng = np.random.default_rng([RANDOM_SEED, *pattern.name.encode()])
    n = shape[0]
    if pattern.kind == "sequential":
        return np.zeros((pattern.n_units, 0), dtype=np.int64)
    if pattern.kind == "random_batches":
        return rng.integers(0, n - pattern.batch + 1, size=(N_SAMPLES, pattern.reads_per_sample))
    if pattern.kind == "subregion":
        t = rng.integers(0, n - pattern.batch + 1, size=N_SAMPLES)
        c = rng.integers(0, shape[1] - pattern.width + 1, size=N_SAMPLES)
        return np.column_stack([t, c])
    if pattern.kind == "frame":
        return rng.integers(0, n, size=(N_SAMPLES, 1))
    if pattern.kind == "patch":
        x = rng.integers(0, shape[2] - pattern.width + 1, size=N_SAMPLES)
        y = rng.integers(0, shape[1] - pattern.width + 1, size=N_SAMPLES)
        return np.column_stack([x, y])
    raise ValueError(f"Unknown pattern kind {pattern.kind!r}")


def unit_selections(pattern: Pattern, shape: tuple[int, ...], position: list[int]) -> list[Selection]:
    """The selections one unit reads, in order."""
    n = shape[0]
    rest = [[0, s] for s in shape[1:]]
    if pattern.kind == "sequential":
        return [[[t, min(t + pattern.batch, n)], *rest] for t in range(0, n, pattern.batch)]
    if pattern.kind == "random_batches":
        return [[[t, t + pattern.batch], *rest] for t in position]
    if pattern.kind == "subregion":
        t, c = position
        return [[[t, t + pattern.batch], [c, c + pattern.width]]]
    if pattern.kind == "frame":
        return [[position[0], *rest]]
    if pattern.kind == "patch":
        x, y = position
        w = pattern.width
        return [[[t, min(t + pattern.batch, n)], [y, y + w], [x, x + w]] for t in range(0, n, pattern.batch)]
    raise ValueError(f"Unknown pattern kind {pattern.kind!r}")


def to_index(selection: Selection) -> tuple[int | slice, ...]:
    """The numpy index of a selection."""
    return tuple(s if isinstance(s, int) else slice(s[0], s[1]) for s in selection)
