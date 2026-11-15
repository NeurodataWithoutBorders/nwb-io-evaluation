"""
Write an NWB file with unchunked, uncompressed TwoPhotonSeries data from TIFF files.

This script reads an existing NWB file, replaces the TwoPhotonSeries with a new one
that has unchunked, uncompressed data read from a directory of TIFF files. The new series
holds the window of frames defined in exp12_window.py, with its timing metadata shifted to
match, and is named TwoPhotonSeries (see exp12_series.py for what else the test file leaves
out).

Writes the NWB file and a stats table stats_<output_stem>.tsv next to it
(see exp12_write_stats.py).

Usage:
    python exp12_write_ophys_tiff_to_unchunked_nwb.py <input_nwb_filepath> <series_name> \
        <tiff_dir> <output_nwb_filepath>
"""

import sys
import time
from pathlib import Path

import h5py
from hdmf.backends.hdf5.h5_utils import H5DataIO
import pynwb
import tifffile

from exp12_series import OPHYS_SERIES_NAME, replace_series
from exp12_window import OPHYS_WINDOW_FRAMES, centered_window, windowed_timing
from exp12_write_stats import fsync, partial_path, publish_nwb, remove_outputs, series_rows, write_stats


def main() -> None:
    usage = (
        "Usage: python exp12_write_ophys_tiff_to_unchunked_nwb.py "
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

    stats_filepath = output_filepath.parent / f"stats_{output_filepath.stem}.tsv"
    remove_outputs(output_filepath, stats_filepath)
    start_tot_time = time.perf_counter_ns()

    # Export the NWB file with an empty dataset placeholder
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        orig_2pseries = nwbfile.acquisition[series_name]
        orig_n_frames = orig_2pseries.data.shape[0]
        if n_frames != orig_n_frames:
            raise ValueError(
                f"Number of TIFF files ({n_frames}) does not match "
                f"original series frame count ({orig_n_frames})"
            )

        start, stop = centered_window(n_frames, OPHYS_WINDOW_FRAMES)
        window_files = tiff_files[start:stop]
        print(f"{series_name}: writing frames {start}:{stop} of {n_frames} as {OPHYS_SERIES_NAME}")

        # Create H5DataIO with data=None to create an empty dataset with correct shape
        data = H5DataIO(
            data=None,
            dtype=dtype,
            shape=(len(window_files), frame_width, frame_height),
            chunks=None,  # No chunking (contiguous)
        )

        kwargs = dict(
            name=OPHYS_SERIES_NAME,
            description=orig_2pseries.description,
            data=data,
            unit=orig_2pseries.unit,
            conversion=orig_2pseries.conversion,
            resolution=orig_2pseries.resolution,
            comments=orig_2pseries.comments,
            dimension=orig_2pseries.dimension,
            imaging_plane=orig_2pseries.imaging_plane,
            **windowed_timing(orig_2pseries, start, stop),
        )

        replace_series(nwbfile, series_name, pynwb.ophys.TwoPhotonSeries(**kwargs))

        print("Exporting NWB file with empty dataset...")
        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(partial_path(output_filepath), "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)

    # Now open the HDF5 file directly and fill in the data from TIFFs
    print("Filling dataset with data from TIFFs...")
    source_read_s = 0.0
    with h5py.File(partial_path(output_filepath), "r+") as f:
        dataset = f[f"/acquisition/{OPHYS_SERIES_NAME}/data"]

        for frame_idx, tiff_path in enumerate(window_files):
            read_start = time.perf_counter_ns()
            frame = tifffile.imread(tiff_path)
            source_read_s += (time.perf_counter_ns() - read_start) / 1e9
            # Transpose from (height, width) to (width, height) for NWB
            dataset[frame_idx, :, :] = frame.T

            if (frame_idx + 1) % 100 == 0 or frame_idx == len(window_files) - 1:
                print(f"  Wrote {frame_idx + 1}/{len(window_files)} frames")
    fsync(partial_path(output_filepath))

    net_time_write_s = (time.perf_counter_ns() - start_time_write) / 1e9
    publish_nwb(output_filepath)
    net_tot_time_s = (time.perf_counter_ns() - start_tot_time) / 1e9

    rows = series_rows(output_filepath, "unchunked", {OPHYS_SERIES_NAME: series_name}, net_time_write_s,
                       {OPHYS_SERIES_NAME: source_read_s}, net_tot_time_s)
    write_stats(stats_filepath, rows)

    print()
    print(f"Done. Output: {output_filepath}")
    print(f"Stats: {stats_filepath}")
    print(f"Write time: {net_time_write_s:.4f} s")
    print(f"Total time: {net_tot_time_s:.4f} s")


if __name__ == "__main__":
    main()
