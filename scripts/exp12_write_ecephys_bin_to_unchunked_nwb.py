"""
Write an NWB file with unchunked, uncompressed ElectricalSeries data from a flat binary file.

This script reads an existing NWB file and replaces one ElectricalSeries with a new one that has
unchunked, uncompressed data read from a flat binary file (time-major int16, 384 channels). The
new series holds the window of samples defined in exp12_window.py, with its timing metadata
shifted to match, and is named ElectricalSeries (see exp12_series.py for what else the test file
leaves out).

Writes the NWB file and a stats table stats_<output_stem>.tsv next to it
(see exp12_write_stats.py).

Usage:
    python exp12_write_ecephys_bin_to_unchunked_nwb.py <input_nwb_filepath> <output_nwb_filepath> \
        <series_name> <binary_file>

Arguments:
    series_name: Name of the ElectricalSeries in the source NWB file
    binary_file: The series' raw data, from exp12_nwb_to_binary.py
"""

import sys
import time
from pathlib import Path

import h5py
import numpy as np
from hdmf.backends.hdf5.h5_utils import H5DataIO
import pynwb

from exp12_series import ECEPHYS_SERIES_NAME, replace_series
from exp12_window import ECEPHYS_WINDOW_SAMPLES, centered_window, windowed_timing
from exp12_write_stats import fsync, partial_path, publish_nwb, remove_outputs, series_rows, write_stats


N_CHANNELS = 384
WRITE_BUFFER_SAMPLES = 9_000_000  # 6.9 GB for 384 channels of int16


def main() -> None:
    usage = (
        "Usage: python exp12_write_ecephys_bin_to_unchunked_nwb.py "
        "<input_nwb_filepath> <output_nwb_filepath> <series_name> <binary_file>"
    )
    if len(sys.argv) != 5:
        print(usage)
        sys.exit(1)

    input_filepath = Path(sys.argv[1])
    output_filepath = Path(sys.argv[2])
    series_name = sys.argv[3]
    binary_path = Path(sys.argv[4])

    if not input_filepath.exists():
        print(f"Error: Input NWB file not found: {input_filepath}")
        sys.exit(1)
    if not binary_path.exists():
        print(f"Error: Binary file not found: {binary_path}")
        sys.exit(1)

    dtype = np.dtype("int16")

    # Validate the binary file and get its dimensions
    file_size = binary_path.stat().st_size
    n_samples = file_size // (N_CHANNELS * dtype.itemsize)
    assert n_samples * N_CHANNELS * dtype.itemsize == file_size, (
        f"Binary file size {file_size} not divisible by "
        f"{N_CHANNELS} channels x {dtype.itemsize} bytes for {series_name}"
    )
    print(f"{series_name}: {n_samples} samples from {binary_path}")

    stats_filepath = output_filepath.parent / f"stats_{output_filepath.stem}.tsv"
    remove_outputs(output_filepath, stats_filepath)
    start_tot_time = time.perf_counter_ns()

    # Export the NWB file with an empty dataset placeholder
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()

        orig_series = nwbfile.acquisition[series_name]
        n_channels = orig_series.data.shape[1] if orig_series.data.ndim > 1 else 1
        assert n_channels == N_CHANNELS, (
            f"Expected {N_CHANNELS} channels for {series_name}, got {n_channels}"
        )
        assert n_samples == orig_series.data.shape[0], (
            f"Sample count mismatch for {series_name}: "
            f"binary has {n_samples}, original has {orig_series.data.shape[0]}"
        )

        window_start, window_stop = centered_window(n_samples, ECEPHYS_WINDOW_SAMPLES)
        n_window = window_stop - window_start
        print(f"{series_name}: writing samples {window_start}:{window_stop} of {n_samples} "
              f"as {ECEPHYS_SERIES_NAME}")

        # Create H5DataIO with data=None to create an empty dataset with correct shape
        data = H5DataIO(
            data=None,
            dtype=dtype,
            shape=(n_window, n_channels),
            chunks=None,  # No chunking (contiguous)
        )

        electrodes_region = nwbfile.create_electrode_table_region(
            region=orig_series.electrodes.data[:].tolist(),
            name=orig_series.electrodes.name,
            description=orig_series.electrodes.description,
        )

        kwargs = dict(
            name=ECEPHYS_SERIES_NAME,
            description=orig_series.description,
            data=data,
            electrodes=electrodes_region,
            conversion=orig_series.conversion,
            resolution=orig_series.resolution,
            comments=orig_series.comments,
            **windowed_timing(orig_series, window_start, window_stop),
        )
        if hasattr(orig_series, "offset") and orig_series.offset is not None:
            kwargs["offset"] = orig_series.offset
        if hasattr(orig_series, "channel_conversion") and orig_series.channel_conversion is not None:
            kwargs["channel_conversion"] = orig_series.channel_conversion[:]

        replace_series(nwbfile, series_name, pynwb.ecephys.ElectricalSeries(**kwargs))

        print("Exporting NWB file with an empty dataset...")
        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(partial_path(output_filepath), "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)

    # Now open the HDF5 file directly and fill in the data from the binary file
    print("Filling the dataset with data from the binary file...")
    source_read_s = 0.0
    with h5py.File(partial_path(output_filepath), "r+") as f:
        dataset = f[f"/acquisition/{ECEPHYS_SERIES_NAME}/data"]
        binary_data = np.memmap(
            binary_path, dtype=dtype, mode="r", shape=(n_samples, N_CHANNELS)
        )[window_start:window_stop]

        for start in range(0, n_window, WRITE_BUFFER_SAMPLES):
            end = min(start + WRITE_BUFFER_SAMPLES, n_window)
            read_start = time.perf_counter_ns()
            block = np.array(binary_data[start:end])
            source_read_s += (time.perf_counter_ns() - read_start) / 1e9
            dataset[start:end, :] = block
            print(f"    Wrote {end}/{n_window} samples")
    fsync(partial_path(output_filepath))

    net_time_write_s = (time.perf_counter_ns() - start_time_write) / 1e9
    publish_nwb(output_filepath)
    net_tot_time_s = (time.perf_counter_ns() - start_tot_time) / 1e9

    rows = series_rows(output_filepath, "unchunked", {ECEPHYS_SERIES_NAME: series_name},
                       net_time_write_s, {ECEPHYS_SERIES_NAME: source_read_s}, net_tot_time_s)
    write_stats(stats_filepath, rows)

    print()
    print(f"Done. Output: {output_filepath}")
    print(f"Stats: {stats_filepath}")
    print(f"Write time: {net_time_write_s:.4f} s")
    print(f"Total time: {net_tot_time_s:.4f} s")


if __name__ == "__main__":
    main()
