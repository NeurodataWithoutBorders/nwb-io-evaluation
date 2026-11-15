"""
Chunking and compression configurations for the write and read benchmarks.

Each dataset has a tab-separated table, scripts/exp12_configs_<label>.tsv, with one configuration
per row and these columns:

    config_id  readable name used in output file names, for example EC384M-zstd3-shuf
    stage      1 or 2 (see below)
    chunks     chunk shape, for example 10000,384
    codec      none, gzip, lzf, lz4, zstd, blosc2-blosclz, blosc2-lz4, blosc2-lz4hc, or blosc2-zstd
    level      compression level, or - for codecs without one (none, lzf, lz4)
    shuffle    none, shuffle, or bitshuffle

For gzip, lzf, lz4, and zstd, "shuffle" adds the HDF5 shuffle filter in front of the codec. For
the Blosc2 codecs, "shuffle" and "bitshuffle" select Blosc2's own filter, and the HDF5 shuffle
filter is never added: b2h5py's fast slicing decompresses Blosc2 chunks itself, skips any other
HDF5 filter, and so returns wrong data for a dataset that also has HDF5 shuffle.

A Blosc2 level sets both the codec level and the Blosc2 block size. For blosc2-zstd, level c
runs zstd at level 2c - 1 for c < 9 and at zstd's maximum (22) for c = 9, so blosc2-zstd levels
2, 6, and 9 run zstd at the same levels as the standalone zstd 3, 11, and 22 configurations.

Chunk shapes cross three aspects with three sizes. The aspects split time from the other
dimensions differently:

    EC384, EC64, EC32       all 384 channels, groups of 64 channels, and groups of 32 channels
    OCframe                 whole frames
    OCtile128, OCtile16     128 x 128 and 16 x 16 pixel tiles (16 x 16 is the patch the O4 read
                            pattern reads)

and the sizes, in uncompressed bytes, are

    S  at most 1 MiB, the default chunk cache of HDF5 1.x
    M  at most 8 MiB, the default chunk cache of HDF5 2.x
    L  at most 24 MiB. At the compression ratios measured for these data in an earlier run (about
       2.2 for ecephys and 1.8 for ophys with the best codecs) and at 1.5, these chunks make byte
       range requests of about 8 to 15 MiB, within the 8 to 16 MB part size AWS suggests for
       downloading large objects from S3

The chunk cache holds decompressed chunks no larger than itself, so a read that touches a chunk
again, in the same read call or a later one, decompresses it again when the chunk does not fit.
Each shape's time extent divides the window (exp12_window.py) and is at most half of it, so every
dataset has at least two chunks along time, and the ecephys time extents divide or are multiples
of 30,000 samples, one second of data. For OCtile16 these limits hold the sizes below their
labels: M is 5 MiB and L is 10 MiB, since 8 MiB of 16 x 16 tiles is 16,384 frames, which does not
divide the 40,960 frame window, and 24 MiB would exceed it.

Three more labels hold library default chunk shapes, computed for each full-length recording
(the shape, dtype, and label of every recording are in DATASETS) and written with gzip 4:

    ECh5py, OCh5py  h5py's chunk guess for chunks=True (h5py._hl.filters.guess_chunk), which hdmf
                    used for chunks=True and iterator writes before hdmf computed its own shape.
                    Many existing DANDI files have these chunks.
    EChdmf, OChdmf  hdmf's default chunk shape (HDF5IO.compute_default_chunk_shape), which hdmf
                    uses for chunks=True and iterator writes.
    ECneuroconv,    NeuroConv's default chunk shape and compression (see neuroconv_chunks), the most
    OCneuroconv     common setting for raw Neuropixels data in a scan of large DANDI dandisets.

The computations depend on the dataset shape, so they use the full-length shape, which is what
users writing the full recording get. The tables record the shapes for the installed h5py and hdmf.
Regenerating the tables and comparing them with the committed ones checks that the shapes still
hold.

The configurations run in two stages:

    1  every codec setting on the M size of two aspects (CODEC_SWEEP_CHUNKS), which ranks the
       codecs; no compression and gzip 4 on every other shape; and the library defaults
    2  the codec settings chosen from the stage 1 results (STAGE2_SETTINGS) on every other shape

Stage 2 rows follow the stage 1 rows, so adding them leaves the row of every stage 1
configuration unchanged.

The write scripts take a configuration by its row in the table (1-based, after the header), and
the write schedule (exp12_write_schedule.py) records the row of each configuration.

Write the tables with:
    python scripts/exp12_configs.py
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from h5py._hl.filters import guess_chunk
import hdf5plugin
from hdmf.backends.hdf5.h5tools import HDF5IO
import numpy as np

from exp12_window import ECEPHYS_WINDOW_SAMPLES, OPHYS_WINDOW_FRAMES

CONFIG_DIR = Path(__file__).resolve().parent
COLUMNS = ("config_id", "stage", "chunks", "codec", "level", "shuffle")

STANDARD_CODECS = ("gzip", "lzf", "lz4", "zstd")
BLOSC2_CNAMES = {
    "blosc2-blosclz": "blosclz",
    "blosc2-lz4": "lz4",
    "blosc2-lz4hc": "lz4hc",
    "blosc2-zstd": "zstd",
}
BLOSC2_FILTERS = {
    "none": hdf5plugin.Blosc2.NOFILTER,
    "shuffle": hdf5plugin.Blosc2.SHUFFLE,
    "bitshuffle": hdf5plugin.Blosc2.BITSHUFFLE,
}
CODEC_TOKENS = {
    "none": "none", "gzip": "gzip", "lzf": "lzf", "lz4": "lz4", "zstd": "zstd",
    "blosc2-blosclz": "b2blosclz", "blosc2-lz4": "b2lz4", "blosc2-lz4hc": "b2lz4hc",
    "blosc2-zstd": "b2zstd",
}
SHUFFLE_TOKENS = {"none": "", "shuffle": "shuf", "bitshuffle": "bitshuf"}

# Each standard codec is written with and without HDF5 shuffle.
STANDARD_SETTINGS = [
    ("gzip", 1), ("gzip", 4), ("gzip", 6), ("gzip", 9),
    ("lzf", None), ("lz4", None),
    ("zstd", 3), ("zstd", 11), ("zstd", 22),
]
# Each Blosc2 setting is written with Blosc2's shuffle and with its bitshuffle.
BLOSC2_SETTINGS = [
    ("blosc2-blosclz", 1), ("blosc2-blosclz", 9),
    ("blosc2-lz4", 1), ("blosc2-lz4", 9),
    ("blosc2-lz4hc", 1), ("blosc2-lz4hc", 5), ("blosc2-lz4hc", 9),
    ("blosc2-zstd", 2), ("blosc2-zstd", 6), ("blosc2-zstd", 9),
]
# The library default chunk shapes are written with gzip 4 (h5py's default gzip level) and no
# shuffle, so the h5py, hdmf, and NeuroConv rows differ only in chunk shape. gzip 4 without shuffle
# is also NeuroConv's default compression for these datasets.
DEFAULT_CHUNKS_SETTING = ("gzip", 4)

# Largest uncompressed chunk size, in bytes, of each size label.
SIZE_LIMITS = {"S": 2**20, "M": 8 * 2**20, "L": 24 * 2**20}

# Time extents divide the window and divide or are multiples of 30,000 samples (one second). The
# S, M, and L shapes of an aspect are 0.96, 7.68, and 23.04 MB.
ECEPHYS_CHUNKS = {
    "EC384S": (1250, 384),
    "EC384M": (10000, 384),
    "EC384L": (30000, 384),
    "EC64S": (7500, 64),
    "EC64M": (60000, 64),
    "EC64L": (180000, 64),
    "EC32S": (15000, 32),
    "EC32M": (120000, 32),
    "EC32L": (360000, 32),
}


def largest_time_extent(slice_bytes: int, limit: int, window: int) -> int:
    """The largest divisor of window, at most half of it, whose chunks of slice_bytes per time step
    fit in limit."""
    extent = max(1, min(limit // slice_bytes, window // 2))
    while window % extent:
        extent -= 1
    return extent


def ophys_chunks(width: int, height: int) -> dict[str, tuple[int, ...]]:
    """Chunk shapes for frames of width x height uint16 pixels, by chunk label.

    Each shape holds the most frames that divide the window, number at most half of it, and keep
    the chunk within its size limit.
    """
    aspects = {"OCframe": (width, height), "OCtile128": (128, 128), "OCtile16": (16, 16)}
    chunks = {}
    for aspect, (w, h) in aspects.items():
        for size, limit in SIZE_LIMITS.items():
            frames = largest_time_extent(w * h * np.dtype(np.uint16).itemsize, limit, OPHYS_WINDOW_FRAMES)
            chunks[f"{aspect}{size}"] = (frames, w, h)
    return chunks


# The M shapes of two aspects that differ most in how they split the data, on which stage 1 writes
# every codec setting.
CODEC_SWEEP_CHUNKS = {"EC": ("EC384M", "EC32M"), "OC": ("OCframeM", "OCtile16M")}

# Codec settings (codec, level, shuffle) that stage 2 writes on every shape outside
# CODEC_SWEEP_CHUNKS. They are chosen per modality by a fixed rule from the stage 1 writes and
# reads of the codec sweep shapes, 2 shapes x 3 datasets = 6 cells per setting. Per cell:
#
#     R  compression ratio (write stats)
#     D  read CPU time of the full-width sequential pattern (E1 or O1) without b2h5py, with the
#        largest chunk cache the cell was read with, median over repeats
#     C  write CPU time of the series, cpu_series_s - cpu_source_read_s
#
# Each is divided by the gzip 4 (no shuffle) value of the same cell, and the settings are compared
# on the geometric mean over the 6 cells. The ophys cells include ophys1, whose compression ratio
# is close to 1 for every setting, so the ophys means of R differ less than those of ophys2 and
# ophys3 alone. Ratios within 1% of each other count as equal, and the
# setting with the lower D ranks first among them. The picks:
#
#     1  fastest to read: lowest D among settings with R >= 1
#     2  smallest that reads and writes at an acceptable cost: highest R among settings with D <= 1
#        and C no higher than gzip 9's
#     3  smallest in the HDF5 library: the gzip setting picked by rule 2 among gzip settings only
#     4  Blosc2, so the b2h5py read factor covers every shape: lowest D among Blosc2 settings with
#        R >= 1. When pick 1 is a Blosc2 setting, this pick is the lowest D among lz4 and zstd
#        settings with R >= 1, so both plugin families are covered.
#
# STAGE2_SETTINGS holds the distinct picks other than gzip 4, which every shape already has.
# TODO: fill in by this rule once the stage 1 reads finish, then regenerate the tables and the
# write schedule.
STAGE2_SETTINGS: dict[str, list[tuple[str, int | None, str]]] = {"EC": [], "OC": []}


# Full-length shape and dtype of each benchmarked series in its source NWB file.
DATASETS = {
    "ecephys1": {"prefix": "EC", "chunks": ECEPHYS_CHUNKS, "full_shape": (349975807, 384), "dtype": np.int16},
    "ecephys2": {"prefix": "EC", "chunks": ECEPHYS_CHUNKS, "full_shape": (114798408, 384), "dtype": np.int16},
    "ecephys3": {"prefix": "EC", "chunks": ECEPHYS_CHUNKS, "full_shape": (326714943, 384), "dtype": np.int16},
    "ophys1": {"prefix": "OC", "chunks": ophys_chunks(796, 512), "full_shape": (41673, 796, 512),
               "dtype": np.uint16},
    "ophys2": {"prefix": "OC", "chunks": ophys_chunks(512, 512), "full_shape": (126741, 512, 512),
               "dtype": np.uint16},
    "ophys3": {"prefix": "OC", "chunks": ophys_chunks(512, 512), "full_shape": (113855, 512, 512),
               "dtype": np.uint16},
}


def neuroconv_chunks(shape: tuple[int, ...], dtype: Any) -> tuple[int, ...]:
    """NeuroConv's default chunk shape for an ElectricalSeries (2-D) or TwoPhotonSeries (3-D) dataset.

    This follows NeuroConv 0.10.2, which aims for chunks of at most 10 MB:
    get_electrical_series_chunk_shape keeps min(64, channels) channels and as many frames as fit,
    and get_image_series_chunk_shape keeps whole frames, as many as fit and at least one. Its
    default dataset configuration compresses both with gzip at h5py's default level 4, without
    shuffle. For 384 channel int16 data the chunks are (78125, 64). In a scan of the largest NWB
    file in each DANDI dandiset of 200 GB or more, 7 of the 10 files holding a raw 384 channel int16
    AP series used (78125, 64) with gzip 4, including every one created from August 2023 to March
    2026 and the source of ecephys3.
    """
    itemsize = np.dtype(dtype).itemsize
    chunk_bytes = 10_000_000
    if len(shape) == 2:
        channels = min(64, shape[1])
        return (min(chunk_bytes // (channels * itemsize), shape[0]), channels)
    frames = int(chunk_bytes / (shape[1] * shape[2] * itemsize))
    return (max(1, min(frames, shape[0])), shape[1], shape[2])


def default_chunks(label: str) -> dict[str, tuple[int, ...]]:
    """Library default chunk shapes for the full-length recording of a dataset, by chunk label."""
    spec = DATASETS[label]
    shape, dtype = spec["full_shape"], spec["dtype"]
    return {
        f"{spec['prefix']}h5py": tuple(guess_chunk(shape, shape, np.dtype(dtype).itemsize)),
        f"{spec['prefix']}hdmf": tuple(HDF5IO.compute_default_chunk_shape(shape, dtype)),
        f"{spec['prefix']}neuroconv": neuroconv_chunks(shape, dtype),
    }


@dataclass(frozen=True)
class Config:
    config_id: str
    stage: int
    chunks: tuple[int, ...]
    codec: str
    level: int | None
    shuffle: str


def make_config(stage: int, chunk_label: str, chunks: tuple[int, ...], codec: str, level: int | None,
                shuffle: str) -> Config:
    level_token = "" if level is None else str(level)
    parts = [chunk_label, f"{CODEC_TOKENS[codec]}{level_token}"]
    if SHUFFLE_TOKENS[shuffle]:
        parts.append(SHUFFLE_TOKENS[shuffle])
    return Config("-".join(parts), stage, chunks, codec, level, shuffle)


def all_codec_settings() -> list[tuple[str, int | None, str]]:
    """Every codec setting, as (codec, level, shuffle)."""
    settings: list[tuple[str, int | None, str]] = [("none", None, "none")]
    for codec, level in STANDARD_SETTINGS:
        for shuffle in ("none", "shuffle"):
            settings.append((codec, level, shuffle))
    for codec, level in BLOSC2_SETTINGS:
        for shuffle in ("shuffle", "bitshuffle"):
            settings.append((codec, level, shuffle))
    return settings


def build_configs(label: str) -> list[Config]:
    spec = DATASETS[label]
    sweep = CODEC_SWEEP_CHUNKS[spec["prefix"]]
    codec, level = DEFAULT_CHUNKS_SETTING
    baseline_settings = [("none", None, "none"), (codec, level, "none")]

    configs = []
    for chunk_label, chunks in spec["chunks"].items():
        settings = all_codec_settings() if chunk_label in sweep else baseline_settings
        for codec_setting in settings:
            configs.append(make_config(1, chunk_label, chunks, *codec_setting))
    for chunk_label, chunks in default_chunks(label).items():
        configs.append(make_config(1, chunk_label, chunks, codec, level, "none"))

    stage1_ids = {c.config_id for c in configs}
    for chunk_label, chunks in spec["chunks"].items():
        if chunk_label in sweep:
            continue
        for codec_setting in STAGE2_SETTINGS[spec["prefix"]]:
            config = make_config(2, chunk_label, chunks, *codec_setting)
            if config.config_id not in stage1_ids:
                configs.append(config)
    return configs


def check_chunks(label: str) -> None:
    """Raise ValueError unless each EC or OC chunk shape fits its size limit, divides the window, and
    spans at most half of it."""
    spec = DATASETS[label]
    window = ECEPHYS_WINDOW_SAMPLES if spec["prefix"] == "EC" else OPHYS_WINDOW_FRAMES
    itemsize = np.dtype(spec["dtype"]).itemsize
    for chunk_label, chunks in spec["chunks"].items():
        nbytes = int(np.prod(chunks)) * itemsize
        limit = SIZE_LIMITS[chunk_label[-1]]
        if nbytes > limit or window % chunks[0] or chunks[0] > window // 2:
            raise ValueError(
                f"{label} {chunk_label} {chunks}: {nbytes} bytes (limit {limit}), {chunks[0]} time "
                f"steps in a window of {window}, which must be a multiple of it and at least twice it"
            )


def write_buffer_length(chunks: tuple[int, ...], target: int) -> int:
    """The largest whole number of chunk time extents that fits in target, and at least one.

    Writing buffers of this length writes each chunk in one piece. A write that covers part of a
    chunk makes HDF5 compress the chunk with fill values once it leaves the chunk cache, then read,
    decompress, merge, recompress, and rewrite it when the rest of the chunk arrives.
    """
    return max(1, target // chunks[0]) * chunks[0]


def config_table_path(label: str) -> Path:
    return CONFIG_DIR / f"exp12_configs_{label}.tsv"


def write_config_table(path: Path, configs: list[Config]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(COLUMNS)
        for c in configs:
            writer.writerow([c.config_id, c.stage, ",".join(str(n) for n in c.chunks), c.codec,
                             "-" if c.level is None else c.level, c.shuffle])


def load_configs(path: Path) -> list[Config]:
    with open(path, newline="") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    if tuple(rows[0]) != COLUMNS:
        raise ValueError(f"{path}: expected header {COLUMNS}, got {tuple(rows[0])}")
    configs = []
    for row in rows[1:]:
        config_id, stage, chunks, codec, level, shuffle = row
        configs.append(Config(
            config_id=config_id,
            stage=int(stage),
            chunks=tuple(int(n) for n in chunks.split(",")),
            codec=codec,
            level=None if level == "-" else int(level),
            shuffle=shuffle,
        ))
    return configs


def read_config(path: Path, row: int) -> Config:
    """Return the configuration in data row `row` (1-based) of the table at `path`."""
    configs = load_configs(path)
    if not 1 <= row <= len(configs):
        raise ValueError(f"{path} has {len(configs)} configurations; row {row} is out of range")
    return configs[row - 1]


def h5dataio_kwargs(config: Config) -> dict[str, Any]:
    """H5DataIO keyword arguments (other than data) for writing a dataset with this configuration."""
    kwargs: dict[str, Any] = {"chunks": config.chunks}
    if config.codec in BLOSC2_CNAMES:
        filt = hdf5plugin.Blosc2(
            cname=BLOSC2_CNAMES[config.codec], clevel=config.level, filters=BLOSC2_FILTERS[config.shuffle]
        )
        kwargs.update(compression=filt.filter_id, compression_opts=filt.filter_options,
                      allow_plugin_filters=True)
        return kwargs

    if config.shuffle == "bitshuffle":
        raise ValueError(f"{config.config_id}: bitshuffle is only available with the Blosc2 codecs")
    if config.codec == "none":
        if config.shuffle != "none":
            raise ValueError(f"{config.config_id}: shuffle without a codec does not reduce size")
        return kwargs
    if config.codec == "gzip":
        kwargs.update(compression="gzip", compression_opts=config.level)
    elif config.codec == "lzf":
        kwargs.update(compression="lzf")
    elif config.codec == "lz4":
        filt = hdf5plugin.LZ4()
        kwargs.update(compression=filt.filter_id, compression_opts=filt.filter_options,
                      allow_plugin_filters=True)
    elif config.codec == "zstd":
        filt = hdf5plugin.Zstd(clevel=config.level)
        kwargs.update(compression=filt.filter_id, compression_opts=filt.filter_options,
                      allow_plugin_filters=True)
    else:
        raise ValueError(f"{config.config_id}: unknown codec {config.codec!r}")
    if config.shuffle == "shuffle":
        kwargs["shuffle"] = True
    return kwargs


def main() -> None:
    for label in DATASETS:
        check_chunks(label)
        configs = build_configs(label)
        path = config_table_path(label)
        write_config_table(path, configs)
        print(f"Wrote {len(configs)} configurations to {path}")


if __name__ == "__main__":
    main()
