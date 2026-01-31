"""
Write an NWB file with unchunked, uncompressed TwoPhotonSeries data from TIFF files.

This script reads an existing NWB file, replaces the TwoPhotonSeries with a new one
that has unchunked, uncompressed data read from a directory of TIFF files.
"""

import sys
import time
from pathlib import Path

import h5py
import numpy as np
from hdmf.backends.hdf5.h5_utils import H5DataIO
import pynwb
import tifffile


def main() -> None:
    usage = (
        "Usage: python exp12_tiff_to_nwb.py "
        "<input_nwb_filepath> <series_name> <tiff_dir> <output_nwb_filepath>"
    )
    if len(sys.argv) != 5:
        print(usage)
        sys.exit(1)

    input_filepath = Path(sys.argv[1])
    series_name = sys.argv[2]
    tiff_dir = Path(sys.argv[3])
    output_filepath = Path(sys.argv[4])

    if not input_filepath.exists():
        print(f"Error: Input NWB file not found: {input_filepath}")
        sys.exit(1)

    if not tiff_dir.is_dir():
        print(f"Error: TIFF directory not found: {tiff_dir}")
        sys.exit(1)

    # Get sorted list of TIFF files
    tiff_files = sorted(tiff_dir.glob("*.tiff")) + sorted(tiff_dir.glob("*.tif"))
    if not tiff_files:
        print(f"Error: No TIFF files found in {tiff_dir}")
        sys.exit(1)

    n_frames = len(tiff_files)
    print(f"Found {n_frames} TIFF files")

    # Read first TIFF to get dimensions and dtype
    first_frame = tifffile.imread(tiff_files[0])
    # TIFF is height x width, NWB stores width x height, so transpose
    frame_height, frame_width = first_frame.shape
    dtype = first_frame.dtype
    print(f"Frame shape: {frame_width}x{frame_height} (width x height in NWB)")
    print(f"Data type: {dtype}")

    start_tot_time = time.perf_counter_ns()

    # Export the NWB file with an empty dataset placeholder
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        orig_2pseries = nwbfile.acquisition[series_name]

        # Create H5DataIO with data=None to create an empty dataset with correct shape
        data = H5DataIO(
            data=None,
            dtype=dtype,
            shape=(n_frames, frame_width, frame_height),
            chunks=None,  # No chunking (contiguous)
        )

        kwargs = dict(
            name=orig_2pseries.name,
            description=orig_2pseries.description,
            data=data,
            starting_time=orig_2pseries.starting_time,
            rate=orig_2pseries.rate,
            unit=orig_2pseries.unit,
            conversion=orig_2pseries.conversion,
            resolution=orig_2pseries.resolution,
            comments=orig_2pseries.comments,
            dimension=orig_2pseries.dimension,
            imaging_plane=orig_2pseries.imaging_plane,
        )
        if orig_2pseries.timestamps is not None:
            kwargs["timestamps"] = orig_2pseries.timestamps[:]

        new_2pseries = pynwb.ophys.TwoPhotonSeries(**kwargs)

        nwbfile.acquisition.pop(series_name)
        nwbfile.add_acquisition(new_2pseries)

        print("Exporting NWB file with empty dataset...")
        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(output_filepath, "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)

    # Now open the HDF5 file directly and fill in the data from TIFFs
    print("Filling dataset with data from TIFFs...")
    with h5py.File(output_filepath, "r+") as f:
        data_path = f"/acquisition/{series_name}/data"
        dataset = f[data_path]

        for frame_idx, tiff_path in enumerate(tiff_files):
            frame = tifffile.imread(tiff_path)
            # Transpose from (height, width) to (width, height) for NWB
            dataset[frame_idx, :, :] = frame.T

            if (frame_idx + 1) % 100 == 0 or frame_idx == n_frames - 1:
                print(f"  Wrote {frame_idx + 1}/{n_frames} frames")

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
    header = "n_frames t_write(s) file_size(Gb) total_t(s)\n"
    stats = f"{n_frames} {net_time_write_s:.4f} {file_size_gb} {net_tot_time_s:.4f}\n"

    with open(stats_filepath, "w") as stats_f:
        stats_f.write(header)
        stats_f.write(stats)

    print()
    print(f"Done. Output: {output_filepath}")
    print(f"Stats: {stats_filepath}")
    print(f"File size: {file_size_gb} GB")
    print(f"Write time: {net_time_write_s:.4f} s")
    print(f"Total time: {net_tot_time_s:.4f} s")


if __name__ == "__main__":
    main()
