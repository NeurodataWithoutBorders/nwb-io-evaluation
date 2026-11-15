"""
Chunking and compression configurations for the write and read benchmarks.

Each dataset has a tab-separated table, scripts/exp12_configs_<label>.tsv, with one configuration
per row and these columns:

    config_id  readable name used in output file names, for example EC2-zstd3-shuf
    chunks     chunk shape, for example 15000,384
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

Chunk shape labels follow the paper: EC1 to EC5 for ecephys and OC1 to OC5 for ophys. Two more
labels hold library default chunk shapes, computed for each full-length recording (the shape,
dtype, and label of every recording are in DATASETS) and written with gzip 4:

    ECh5py, OCh5py  h5py's chunk guess for chunks=True (h5py._hl.filters.guess_chunk), which hdmf
                    used for chunks=True and iterator writes before hdmf computed its own shape.
                    Many existing DANDI files have these chunks.
    EChdmf, OChdmf  hdmf's default chunk shape (HDF5IO.compute_default_chunk_shape), which hdmf
                    uses for chunks=True and iterator writes.

Both computations depend on the dataset shape, so they use the full-length shape, which is what
users writing the full recording get. The tables record the shapes for the installed h5py and hdmf.
Regenerating the tables and comparing them with the committed ones checks that the shapes still
hold.

Row N of a table (1-based, after the header) is SLURM array task N.

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

CONFIG_DIR = Path(__file__).resolve().parent
COLUMNS = ("config_id", "chunks", "codec", "level", "shuffle")

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
# shuffle, so the h5py and hdmf rows differ only in chunk shape.
DEFAULT_CHUNKS_SETTING = ("gzip", 4)

ECEPHYS_CHUNKS = {
    "EC1": (1500, 384),
    "EC2": (15000, 384),
    "EC3": (6000, 96),
    "EC4": (60000, 96),
    "EC5": (180000, 32),
}


def ophys_chunks(width: int, height: int) -> dict[str, tuple[int, ...]]:
    return {
        "OC1": (2, width, height),
        "OC2": (20, width, height),
        "OC3": (32, 128, 128),
        "OC4": (320, 128, 128),
        "OC5": (20480, 16, 16),
    }


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


def default_chunks(label: str) -> dict[str, tuple[int, ...]]:
    """Library default chunk shapes for the full-length recording of a dataset, by chunk label."""
    spec = DATASETS[label]
    shape, dtype = spec["full_shape"], spec["dtype"]
    return {
        f"{spec['prefix']}h5py": tuple(guess_chunk(shape, shape, np.dtype(dtype).itemsize)),
        f"{spec['prefix']}hdmf": tuple(HDF5IO.compute_default_chunk_shape(shape, dtype)),
    }


@dataclass(frozen=True)
class Config:
    config_id: str
    chunks: tuple[int, ...]
    codec: str
    level: int | None
    shuffle: str


def make_config(chunk_label: str, chunks: tuple[int, ...], codec: str, level: int | None,
                shuffle: str) -> Config:
    level_token = "" if level is None else str(level)
    parts = [chunk_label, f"{CODEC_TOKENS[codec]}{level_token}"]
    if SHUFFLE_TOKENS[shuffle]:
        parts.append(SHUFFLE_TOKENS[shuffle])
    return Config("-".join(parts), chunks, codec, level, shuffle)


def build_configs(label: str) -> list[Config]:
    spec = DATASETS[label]
    configs = []
    for chunk_label, chunks in spec["chunks"].items():
        configs.append(make_config(chunk_label, chunks, "none", None, "none"))
        for codec, level in STANDARD_SETTINGS:
            for shuffle in ("none", "shuffle"):
                configs.append(make_config(chunk_label, chunks, codec, level, shuffle))
        for codec, level in BLOSC2_SETTINGS:
            for shuffle in ("shuffle", "bitshuffle"):
                configs.append(make_config(chunk_label, chunks, codec, level, shuffle))
    codec, level = DEFAULT_CHUNKS_SETTING
    for chunk_label, chunks in default_chunks(label).items():
        configs.append(make_config(chunk_label, chunks, codec, level, "none"))
    return configs


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
            writer.writerow([c.config_id, ",".join(str(n) for n in c.chunks), c.codec,
                             "-" if c.level is None else c.level, c.shuffle])


def load_configs(path: Path) -> list[Config]:
    with open(path, newline="") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    if tuple(rows[0]) != COLUMNS:
        raise ValueError(f"{path}: expected header {COLUMNS}, got {tuple(rows[0])}")
    configs = []
    for row in rows[1:]:
        config_id, chunks, codec, level, shuffle = row
        configs.append(Config(
            config_id=config_id,
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
        configs = build_configs(label)
        path = config_table_path(label)
        write_config_table(path, configs)
        print(f"Wrote {len(configs)} configurations to {path}")


if __name__ == "__main__":
    main()
