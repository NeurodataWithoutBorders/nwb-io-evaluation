"""
Generate NWB files with ElectricalSeries data from flat binary files.

This script reads ElectricalSeries metadata from an existing NWB file and raw data
from flat binary files (time-major int16, 384 channels), then writes new NWB files
using various configurations of chunking and compression specified in a config file.

Supports multiple series per NWB file (e.g., multiple probes with AP and LF streams).

Usage:
    python exp12_write_ecephys_bin_to_nwb.py <config_file> <config_number> <input_nwb_filepath> \
        <output_label> <output_dir> <series_name> <binary_file> [<series_name> <binary_file> ...]
"""

import sys
import time
from pathlib import Path
from typing import NoReturn

import hdf5plugin  # enable HDF5 compression filters
from hdmf.backends.hdf5.h5_utils import H5DataIO
from hdmf.data_utils import AbstractDataChunkIterator, DataChunk
import numpy as np
import pynwb


N_CHANNELS = 384


class BinaryDataChunkIterator(AbstractDataChunkIterator):
    """A data chunk iterator that reads samples from a flat binary file via memmap."""

    def __init__(
        self,
        binary_path: Path,
        n_samples: int,
        n_channels: int,
        dtype: np.dtype,
        buffer_samples: int,
    ) -> None:
        self.data = np.memmap(
            binary_path, dtype=dtype, mode="r", shape=(n_samples, n_channels)
        )
        self.n_samples = n_samples
        self.n_channels = n_channels
        self._dtype = np.dtype(dtype)
        self.buffer_samples = buffer_samples
        self._current_sample = 0

    def __iter__(self):
        return self

    def __next__(self) -> DataChunk:
        if self._current_sample >= self.n_samples:
            raise StopIteration

        start = self._current_sample
        end = min(start + self.buffer_samples, self.n_samples)

        data = np.array(self.data[start:end])
        selection = np.s_[start:end, :]

        self._current_sample = end
        return DataChunk(data=data, selection=selection)

    def recommended_chunk_shape(self) -> NoReturn:
        raise ValueError(
            "recommended_chunk_shape is not implemented for BinaryDataChunkIterator and should not be "
            "called in this context because H5DataIO is managing chunk shapes."
        )

    def recommended_data_shape(self) -> tuple[int, int]:
        return self.maxshape

    @property
    def dtype(self) -> np.dtype:
        return self._dtype

    @property
    def maxshape(self) -> tuple[int, int]:
        return (self.n_samples, self.n_channels)


def parse_compression(algo: str, clevel: str):
    """Parse compression algorithm and level into h5py-compatible parameters."""
    if algo == "NA":
        return None, None
    elif algo == "gzip":
        return "gzip", int(clevel)
    elif algo == "lzf":
        return "lzf", None
    elif algo == "lz4":
        filt = hdf5plugin.LZ4(nbytes=int(clevel))
        return filt.filter_id, filt.filter_options
    elif algo == "zstd":
        filt = hdf5plugin.Zstd(clevel=int(clevel))
        return filt.filter_id, filt.filter_options
    elif algo.startswith("blosc2-"):
        blosc2_algo_map = {
            "blosc2-blosclz": "blosclz",
            "blosc2-lz4": "lz4",
            "blosc2-lz4hc": "lz4hc",
            "blosc2-zstd": "zstd",
        }
        filt = hdf5plugin.Blosc2(cname=blosc2_algo_map[algo], clevel=int(clevel))
        return filt.filter_id, filt.filter_options
    else:
        raise ValueError(f"Unknown compression algorithm: {algo}")


def make_h5_data_io(data_iterator, chunk_shape, compr, comp_op):
    """Create H5DataIO with the given chunk shape and compression."""
    data_io_kwargs = {
        "data": data_iterator,
        "chunks": chunk_shape,
    }
    if compr is not None:
        data_io_kwargs["compression"] = compr
        data_io_kwargs["allow_plugin_filters"] = True
        if comp_op is not None:
            data_io_kwargs["compression_opts"] = comp_op
    return H5DataIO(**data_io_kwargs)


def process_config(
    config_number: int,
    input_filepath: Path,
    series_pairs: list[tuple[str, Path]],
    output_dir: Path,
    output_label: str,
    params: list[str],
) -> str:
    """Process a single configuration and return stats."""
    assert (
        len(params) == 3
    ), f"Config line {config_number} requires exactly 3 parameters, got {len(params)}"

    # Set chunk sizes
    if params[0].lower() == "true":
        chunk_shape = True
    else:
        chunk_shape = tuple(map(int, params[0].split(",")))

    # Set compression filter
    compr, comp_op = parse_compression(params[1], params[2])

    dtype = np.dtype("int16")
    first_n_samples = None

    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()

        for series_name, binary_path in series_pairs:
            orig_series = nwbfile.acquisition[series_name]
            n_channels = orig_series.data.shape[1] if orig_series.data.ndim > 1 else 1
            assert n_channels == N_CHANNELS, (
                f"Expected {N_CHANNELS} channels for {series_name}, got {n_channels}"
            )
            orig_n_samples = orig_series.data.shape[0]

            # Validate binary file dimensions
            file_size = binary_path.stat().st_size
            n_samples = file_size // (n_channels * dtype.itemsize)
            assert n_samples * n_channels * dtype.itemsize == file_size, (
                f"Binary file size {file_size} not divisible by "
                f"{n_channels} channels x {dtype.itemsize} bytes for {series_name}"
            )
            assert n_samples == orig_n_samples, (
                f"Sample count mismatch for {series_name}: "
                f"binary has {n_samples}, original has {orig_n_samples}"
            )

            if first_n_samples is None:
                first_n_samples = n_samples

            data_iterator = BinaryDataChunkIterator(
                binary_path=binary_path,
                n_samples=n_samples,
                n_channels=n_channels,
                dtype=dtype,
                buffer_samples=10000000,  # ~7.2 GB buffer for 384 channels int16
            )

            data = make_h5_data_io(data_iterator, chunk_shape, compr, comp_op)

            kwargs = dict(
                name=orig_series.name,
                description=orig_series.description,
                data=data,
                electrodes=orig_series.electrodes,
                starting_time=orig_series.starting_time,
                rate=orig_series.rate,
                conversion=orig_series.conversion,
                resolution=orig_series.resolution,
                comments=orig_series.comments,
            )
            if hasattr(orig_series, "offset") and orig_series.offset is not None:
                kwargs["offset"] = orig_series.offset
            if orig_series.timestamps is not None:
                kwargs["timestamps"] = orig_series.timestamps[:]
            if hasattr(orig_series, "channel_conversion") and orig_series.channel_conversion is not None:
                kwargs["channel_conversion"] = orig_series.channel_conversion[:]

            new_series = pynwb.ecephys.ElectricalSeries(**kwargs)

            nwbfile.acquisition.pop(series_name)
            nwbfile.add_acquisition(new_series)

        output_filepath = output_dir / f"{output_label}_Config{config_number:03d}.nwb"

        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(output_filepath, "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)
        end_time_write = time.perf_counter_ns()
        net_time_write_s = (end_time_write - start_time_write) / 1e9

    stats = f"{config_number} {first_n_samples} {net_time_write_s:.4f} "

    try:
        file_size_gb = output_filepath.stat().st_size / (1024 * 1024 * 1024)
        stats += f"{file_size_gb} "
    except OSError:
        stats += "N/A "

    return stats


def main() -> None:
    usage = (
        "Usage: python3 exp12_write_ecephys_bin_to_nwb.py "
        "<config_file> <config_number> <input_nwb_filepath> "
        "<output_label> <output_dir> <series_name> <binary_file> "
        "[<series_name> <binary_file> ...]"
    )
    if len(sys.argv) < 8 or (len(sys.argv) - 6) % 2 != 0:
        print(usage)
        sys.exit(1)

    config_file = Path(sys.argv[1])  # Path to the configuration file
    config_number = int(sys.argv[2])  # Config number (corresponds to line in config file)
    input_filepath = Path(sys.argv[3])  # Path to the input NWB file (for metadata)
    output_label = sys.argv[4]  # Label for this experiment
    output_dir = Path(sys.argv[5])  # Directory to save output NWB files

    # Parse series_name binary_file pairs from remaining args
    series_pairs = []
    for i in range(6, len(sys.argv), 2):
        series_name = sys.argv[i]
        binary_path = Path(sys.argv[i + 1])
        series_pairs.append((series_name, binary_path))

    stats_filepath = output_dir / f"stats_{output_label}_Config{config_number:03d}.txt"
    header = "configNo n_samples t_write(s) file_size(Gb) total_t(s)\n"

    start_tot_time = time.perf_counter_ns()

    # Find and process the matching configuration
    stats = ""
    with open(config_file) as configset:
        for lineno, config_line in enumerate(configset):
            if lineno == config_number:
                fields = config_line.strip().split()
                configno = int(fields[0])
                assert configno == lineno, f"configno {configno} does not match line number {lineno}"
                params = fields[1:]
                stats = process_config(
                    config_number,
                    input_filepath,
                    series_pairs,
                    output_dir,
                    output_label,
                    params,
                )
                break

    if not stats:
        raise ValueError(f"Config number {config_number} not found in {config_file}")

    end_tot_time = time.perf_counter_ns()
    net_tot_time_s = (end_tot_time - start_tot_time) / 1e9
    stats += f"{net_tot_time_s:.4f}\n"

    with open(stats_filepath, "w") as stats_f:
        stats_f.write(header)
        stats_f.write(stats)


if __name__ == "__main__":
    main()
