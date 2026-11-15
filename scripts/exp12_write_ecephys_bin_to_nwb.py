"""
Generate an NWB file with ElectricalSeries data from flat binary files, using one chunking and
compression configuration.

This script reads ElectricalSeries metadata from an existing NWB file and raw data from flat
binary files (time-major int16, 384 channels), then writes a new NWB file using the
configuration in one row of a config table (see exp12_configs.py). The new series holds the
window of samples defined in exp12_window.py, with its timing metadata shifted to match, and is
named ElectricalSeries (see exp12_series.py for what else the test file leaves out).

Writes <output_dir>/<output_label>_<config_id>.nwb and a stats table
<output_dir>/stats_<output_label>_<config_id>.tsv (see exp12_write_stats.py).
The written data is checked against the source before the NWB file gets its final name
(see exp12_verify.py).

Usage:
    python exp12_write_ecephys_bin_to_nwb.py <config_table> <row> <input_nwb_filepath> \
        <output_label> <output_dir> <series_name> <binary_file>

Arguments:
    config_table: Path to a config table, e.g. scripts/exp12_configs_ecephys1.tsv
    row: 1-based row of the config table (the SLURM array task ID)
    series_name: Name of the ElectricalSeries in the source NWB file
    binary_file: The series' raw data, from exp12_nwb_to_binary.py
"""

import sys
import time
from pathlib import Path
from typing import NoReturn

import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
from hdmf.backends.hdf5.h5_utils import H5DataIO
from hdmf.data_utils import AbstractDataChunkIterator, DataChunk
import numpy as np
import pynwb

from exp12_configs import h5dataio_kwargs, read_config, write_buffer_length
from exp12_series import ECEPHYS_SERIES_NAME, replace_series
from exp12_verify import verify_series
from exp12_window import ECEPHYS_WINDOW_SAMPLES, centered_window, windowed_timing
from exp12_write_stats import (
    SeriesMeasurements, fsync, partial_path, publish_nwb, remove_outputs, run_info, series_rows, write_stats,
)


N_CHANNELS = 384

# Each write buffer is the largest whole number of the configuration's chunk time extent that fits
# in this many samples (6.9 GB for 384 channels of int16), so every chunk is written in one piece.
WRITE_BUFFER_TARGET_SAMPLES = 9_000_000


class BinaryDataChunkIterator(AbstractDataChunkIterator):
    """A data chunk iterator that reads n_samples samples, starting at sample_offset, from a flat
    binary file via memmap.

    measurements.series times the span from the first call to __next__ to StopIteration, and
    measurements.source_read the reads of the binary file. hdmf writes the data of iterators after
    the rest of the file, so that span covers writing this series alone.
    """

    def __init__(
        self,
        binary_path: Path,
        sample_offset: int,
        n_samples: int,
        n_channels: int,
        dtype: np.dtype,
        buffer_samples: int,
        measurements: SeriesMeasurements,
    ) -> None:
        self.data = np.memmap(
            binary_path,
            dtype=dtype,
            mode="r",
            offset=sample_offset * n_channels * np.dtype(dtype).itemsize,
            shape=(n_samples, n_channels),
        )
        self.n_samples = n_samples
        self.n_channels = n_channels
        self._dtype = np.dtype(dtype)
        self.buffer_samples = buffer_samples
        self._current_sample = 0
        self.measurements = measurements

    def __iter__(self):
        return self

    def __next__(self) -> DataChunk:
        if self._current_sample == 0 and not self.measurements.series.running:
            self.measurements.series.start()
        if self._current_sample >= self.n_samples:
            if self.measurements.series.running:
                self.measurements.series.stop()
            raise StopIteration

        start = self._current_sample
        end = min(start + self.buffer_samples, self.n_samples)

        with self.measurements.source_read:
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


def main() -> None:
    usage = (
        "Usage: python3 exp12_write_ecephys_bin_to_nwb.py "
        "<config_table> <row> <input_nwb_filepath> "
        "<output_label> <output_dir> <series_name> <binary_file>"
    )
    if len(sys.argv) != 8:
        print(usage)
        sys.exit(1)

    config_table = Path(sys.argv[1])
    row = int(sys.argv[2])
    input_filepath = Path(sys.argv[3])  # Path to the input NWB file (for metadata)
    output_label = sys.argv[4]  # Label for this dataset
    output_dir = Path(sys.argv[5])  # Directory to save output NWB files
    series_name = sys.argv[6]  # Name of the ElectricalSeries in the source NWB file
    binary_path = Path(sys.argv[7])  # Raw data of that series

    start_tot_time = time.perf_counter_ns()
    run = run_info()
    config = read_config(config_table, row)
    print(f"Config {row}: {config}")
    output_filepath = output_dir / f"{output_label}_{config.config_id}.nwb"
    stats_filepath = output_dir / f"stats_{output_label}_{config.config_id}.tsv"
    remove_outputs(output_filepath, stats_filepath)
    dtype = np.dtype("int16")

    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()

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

        start, stop = centered_window(n_samples, ECEPHYS_WINDOW_SAMPLES)
        print(f"{series_name}: writing samples {start}:{stop} of {n_samples} as {ECEPHYS_SERIES_NAME}")

        buffer_samples = write_buffer_length(config.chunks, WRITE_BUFFER_TARGET_SAMPLES)
        measurements = SeriesMeasurements(source_name=series_name)
        data_iterator = BinaryDataChunkIterator(
            binary_path=binary_path,
            sample_offset=start,
            n_samples=stop - start,
            n_channels=n_channels,
            dtype=dtype,
            buffer_samples=buffer_samples,
            measurements=measurements,
        )

        electrodes_region = nwbfile.create_electrode_table_region(
            region=orig_series.electrodes.data[:].tolist(),
            name=orig_series.electrodes.name,
            description=orig_series.electrodes.description,
        )

        kwargs = dict(
            name=ECEPHYS_SERIES_NAME,
            description=orig_series.description,
            data=H5DataIO(data=data_iterator, **h5dataio_kwargs(config)),
            electrodes=electrodes_region,
            conversion=orig_series.conversion,
            resolution=orig_series.resolution,
            comments=orig_series.comments,
            **windowed_timing(orig_series, start, stop),
        )
        if hasattr(orig_series, "offset") and orig_series.offset is not None:
            kwargs["offset"] = orig_series.offset
        if hasattr(orig_series, "channel_conversion") and orig_series.channel_conversion is not None:
            kwargs["channel_conversion"] = orig_series.channel_conversion[:]

        replace_series(nwbfile, series_name, pynwb.ecephys.ElectricalSeries(**kwargs))

        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(partial_path(output_filepath), "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)
        fsync(partial_path(output_filepath))
        net_time_write_s = (time.perf_counter_ns() - start_time_write) / 1e9

    start_time_verify = time.perf_counter_ns()
    source = np.memmap(binary_path, dtype=dtype, mode="r", shape=(n_samples, n_channels))[start:stop]
    measurements.data_sha256 = verify_series(
        partial_path(output_filepath),
        f"acquisition/{ECEPHYS_SERIES_NAME}/data",
        lambda a, b: source[a:b],
        buffer_samples,
    )
    measurements.verify_s = (time.perf_counter_ns() - start_time_verify) / 1e9
    print(f"Data matches the source; SHA-256 {measurements.data_sha256}")

    publish_nwb(output_filepath)
    net_tot_time_s = (time.perf_counter_ns() - start_tot_time) / 1e9
    rows = series_rows(
        output_filepath,
        config.config_id,
        {ECEPHYS_SERIES_NAME: measurements},
        net_time_write_s,
        net_tot_time_s,
        run,
    )
    write_stats(stats_filepath, rows)
    print(f"Wrote {output_filepath} in {net_time_write_s:.1f} s; stats in {stats_filepath}")


if __name__ == "__main__":
    main()
