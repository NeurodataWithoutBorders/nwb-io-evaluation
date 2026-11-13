"""
Write an NWB file with unchunked, uncompressed ElectricalSeries data from flat binary files.

This script reads an existing NWB file, replaces the ElectricalSeries with new ones
that have unchunked, uncompressed data read from flat binary files (time-major int16,
384 channels).

Supports multiple series per NWB file (e.g., multiple probes with AP and LF streams).

Usage:
    python exp12_write_ecephys_bin_to_unchunked_nwb.py <input_nwb_filepath> <output_nwb_filepath> \
        <series_name> <binary_file> [<series_name> <binary_file> ...]
"""

import sys
import time
from pathlib import Path

import h5py
import numpy as np
from hdmf.backends.hdf5.h5_utils import H5DataIO
import pynwb


N_CHANNELS = 384


def main() -> None:
    usage = (
        "Usage: python exp12_write_ecephys_bin_to_unchunked_nwb.py "
        "<input_nwb_filepath> <output_nwb_filepath> "
        "<series_name> <binary_file> [<series_name> <binary_file> ...]"
    )
    if len(sys.argv) < 5 or (len(sys.argv) - 3) % 2 != 0:
        print(usage)
        sys.exit(1)

    input_filepath = Path(sys.argv[1])
    output_filepath = Path(sys.argv[2])

    # Parse series_name binary_file pairs from remaining args
    series_pairs = []
    for i in range(3, len(sys.argv), 2):
        series_name = sys.argv[i]
        binary_path = Path(sys.argv[i + 1])
        series_pairs.append((series_name, binary_path))

    if not input_filepath.exists():
        print(f"Error: Input NWB file not found: {input_filepath}")
        sys.exit(1)

    dtype = np.dtype("int16")

    # Validate binary files and collect dimensions
    series_info = []
    for series_name, binary_path in series_pairs:
        if not binary_path.exists():
            print(f"Error: Binary file not found: {binary_path}")
            sys.exit(1)
        file_size = binary_path.stat().st_size
        n_samples = file_size // (N_CHANNELS * dtype.itemsize)
        assert n_samples * N_CHANNELS * dtype.itemsize == file_size, (
            f"Binary file size {file_size} not divisible by "
            f"{N_CHANNELS} channels x {dtype.itemsize} bytes for {series_name}"
        )
        series_info.append((series_name, binary_path, n_samples))
        print(f"{series_name}: {n_samples} samples from {binary_path}")

    start_tot_time = time.perf_counter_ns()

    # Export the NWB file with empty dataset placeholders
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()

        for series_name, binary_path, n_samples in series_info:
            orig_series = nwbfile.acquisition[series_name]
            n_channels = orig_series.data.shape[1] if orig_series.data.ndim > 1 else 1
            assert n_channels == N_CHANNELS, (
                f"Expected {N_CHANNELS} channels for {series_name}, got {n_channels}"
            )

            # Create H5DataIO with data=None to create an empty dataset with correct shape
            data = H5DataIO(
                data=None,
                dtype=dtype,
                shape=(n_samples, n_channels),
                chunks=None,  # No chunking (contiguous)
            )

            electrodes_region = nwbfile.create_electrode_table_region(
                region=orig_series.electrodes.data[:].tolist(),
                name=orig_series.electrodes.name,
                description=orig_series.electrodes.description,
            )

            kwargs = dict(
                name=orig_series.name,
                description=orig_series.description,
                data=data,
                electrodes=electrodes_region,
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

        print("Exporting NWB file with empty datasets...")
        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(output_filepath, "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)

    # Now open the HDF5 file directly and fill in the data from binary files
    print("Filling datasets with data from binary files...")
    chunk_size = 10000000  # samples per write chunk (~7.2 GB for 384 channels int16)
    with h5py.File(output_filepath, "r+") as f:
        for series_name, binary_path, n_samples in series_info:
            print(f"  Writing {series_name}...")
            data_path = f"/acquisition/{series_name}/data"
            dataset = f[data_path]

            binary_data = np.memmap(
                binary_path, dtype=dtype, mode="r", shape=(n_samples, N_CHANNELS)
            )

            for start in range(0, n_samples, chunk_size):
                end = min(start + chunk_size, n_samples)
                dataset[start:end, :] = binary_data[start:end]

                if end == n_samples or (start // chunk_size + 1) % 10 == 0:
                    print(f"    Wrote {end}/{n_samples} samples")

    end_time_write = time.perf_counter_ns()
    net_time_write_s = (end_time_write - start_time_write) / 1e9

    end_tot_time = time.perf_counter_ns()
    net_tot_time_s = (end_tot_time - start_tot_time) / 1e9

    # Get file size
    try:
        file_size_gb = output_filepath.stat().st_size / (1024 * 1024 * 1024)
    except OSError:
        file_size_gb = float("nan")

    # Write stats file
    stats_filepath = output_filepath.parent / f"stats_{output_filepath.stem}.txt"
    header = "n_samples t_write(s) file_size(Gb) total_t(s)\n"
    stats = f"{series_info[0][2]} {net_time_write_s:.4f} {file_size_gb} {net_tot_time_s:.4f}\n"

    with open(stats_filepath, "w") as stats_f:
        stats_f.write(header)
        stats_f.write(stats)

    print()
    print(f"Done. Output: {output_filepath}")
    print(f"Stats: {stats_filepath}")
    print(f"File size: {file_size_gb:.2f} GB")
    print(f"Write time: {net_time_write_s:.4f} s")
    print(f"Total time: {net_tot_time_s:.4f} s")


if __name__ == "__main__":
    main()
