"""
Write an NWB file with unchunked, uncompressed TwoPhotonSeries data from TIFF files.

This script reads an existing NWB file, replaces the TwoPhotonSeries with a new one
that has unchunked, uncompressed data read from a directory of multi-page TIFF files (see
exp12_tiff.py). The new series holds the window of frames defined in exp12_window.py, with its
timing metadata shifted to match, and is named TwoPhotonSeries (see exp12_series.py for what
else the test file leaves out).

Writes the NWB file and a stats table stats_<output_stem>.tsv next to it
(see exp12_write_stats.py).
The written data is checked against the source before the NWB file gets its final name
(see exp12_verify.py).

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

from exp12_series import OPHYS_SERIES_NAME, replace_series
from exp12_tiff import TiffStack
from exp12_verify import verify_series
from exp12_window import OPHYS_WINDOW_FRAMES, centered_window, windowed_timing
from exp12_write_stats import (
    SeriesMeasurements, fsync, partial_path, publish_nwb, remove_outputs, run_info, series_rows, write_stats,
)

WRITE_BUFFER_FRAMES = 20_480  # 10.7 GB for 512 x 512 frames, 16.7 GB for 796 x 512 frames


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

    stack = TiffStack(tiff_dir)
    print(f"Found {stack.n_frames} frames in {len(stack.files)} TIFF files")
    print(f"Frame shape: {stack.frame_width}x{stack.frame_height} (width x height in NWB)")
    print(f"Data type: {stack.dtype}")

    stats_filepath = output_filepath.parent / f"stats_{output_filepath.stem}.tsv"
    remove_outputs(output_filepath, stats_filepath)
    start_tot_time = time.perf_counter_ns()
    run = run_info()

    # Export the NWB file with an empty dataset placeholder
    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        orig_2pseries = nwbfile.acquisition[series_name]
        stack.check_shape(orig_2pseries.data.shape)

        window_start, window_stop = centered_window(stack.n_frames, OPHYS_WINDOW_FRAMES)
        n_window = window_stop - window_start
        print(f"{series_name}: writing frames {window_start}:{window_stop} of {stack.n_frames} "
              f"as {OPHYS_SERIES_NAME}")

        # Create H5DataIO with data=None to create an empty dataset with correct shape
        data = H5DataIO(
            data=None,
            dtype=stack.dtype,
            shape=(n_window, stack.frame_width, stack.frame_height),
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
            **windowed_timing(orig_2pseries, window_start, window_stop),
        )

        replace_series(nwbfile, series_name, pynwb.ophys.TwoPhotonSeries(**kwargs))

        print("Exporting NWB file with empty dataset...")
        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(partial_path(output_filepath), "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)

    # Now open the HDF5 file directly and fill in the data from TIFFs
    print("Filling dataset with data from TIFFs...")
    measurements = SeriesMeasurements(source_name=series_name)
    with h5py.File(partial_path(output_filepath), "r+") as f:
        dataset = f[f"/acquisition/{OPHYS_SERIES_NAME}/data"]
        with measurements.series:
            for start in range(0, n_window, WRITE_BUFFER_FRAMES):
                end = min(start + WRITE_BUFFER_FRAMES, n_window)
                with measurements.source_read:
                    block = stack.read(window_start + start, window_start + end)
                dataset[start:end, :, :] = block
                print(f"  Wrote {end}/{n_window} frames")
                # Free the block before reading the next one, so the loop holds one block in memory.
                del block
    start_time_fsync = time.perf_counter_ns()
    fsync(partial_path(output_filepath))
    net_time_fsync_s = (time.perf_counter_ns() - start_time_fsync) / 1e9
    net_time_write_s = (time.perf_counter_ns() - start_time_write) / 1e9

    start_time_verify = time.perf_counter_ns()
    measurements.data_sha256 = verify_series(
        partial_path(output_filepath),
        f"acquisition/{OPHYS_SERIES_NAME}/data",
        lambda a, b: stack.read(window_start + a, window_start + b),
        WRITE_BUFFER_FRAMES,
    )
    measurements.verify_s = (time.perf_counter_ns() - start_time_verify) / 1e9
    print(f"Data matches the source; SHA-256 {measurements.data_sha256}")

    publish_nwb(output_filepath)
    net_tot_time_s = (time.perf_counter_ns() - start_tot_time) / 1e9

    rows = series_rows(output_filepath, "unchunked", {OPHYS_SERIES_NAME: measurements}, net_time_write_s,
                       net_time_fsync_s, net_tot_time_s, run)
    write_stats(stats_filepath, rows)

    print()
    print(f"Done. Output: {output_filepath}")
    print(f"Stats: {stats_filepath}")
    print(f"Write time: {net_time_write_s:.4f} s")
    print(f"Total time: {net_tot_time_s:.4f} s")


if __name__ == "__main__":
    main()
