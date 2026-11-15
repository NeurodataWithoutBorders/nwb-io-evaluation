"""
Generate an NWB file with TwoPhotonSeries data from TIFF files, using one chunking and
compression configuration.

This script reads TwoPhotonSeries metadata from an existing NWB file and raw imaging data
from a directory of TIFF files (one per frame), then writes a new NWB file using the
configuration in one row of a config table (see exp12_configs.py). The new series holds the
window of frames defined in exp12_window.py, with its timing metadata shifted to match, and is
named TwoPhotonSeries (see exp12_series.py for what else the test file leaves out).

Writes <output_dir>/<output_label>_<config_id>.nwb and a stats table
<output_dir>/stats_<output_label>_<config_id>.tsv (see exp12_write_stats.py).

Usage:
    python exp12_write_ophys_tiff_to_nwb.py <config_table> <row> <input_nwb_filepath> \
        <series_name> <tiff_dir> <output_label> <output_dir>

Arguments:
    config_table: Path to a config table, e.g. scripts/exp12_configs_ophys1.tsv
    row: 1-based row of the config table (the SLURM array task ID)
"""

import sys
import time
from pathlib import Path
from typing import Any, NoReturn

import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
from hdmf.backends.hdf5.h5_utils import H5DataIO
from hdmf.data_utils import AbstractDataChunkIterator, DataChunk
import numpy as np
import pynwb
import tifffile

from exp12_configs import h5dataio_kwargs, read_config, write_buffer_length
from exp12_series import OPHYS_SERIES_NAME, replace_series
from exp12_window import OPHYS_WINDOW_FRAMES, centered_window, windowed_timing
from exp12_write_stats import fsync, partial_path, publish_nwb, remove_outputs, series_rows, write_stats

# Each write buffer is the largest whole number of the configuration's chunk time extent that fits
# in this many frames (10.7 GB for 512 x 512 frames, 16.7 GB for 796 x 512 frames), so every chunk
# is written in one piece.
WRITE_BUFFER_TARGET_FRAMES = 20_480


class TiffDataChunkIterator(AbstractDataChunkIterator):
    """A data chunk iterator that reads frames from a list of TIFF files, one per frame.
    read_time_s accumulates the time spent reading the TIFF files."""

    def __init__(
        self,
        tiff_files: list[Path],
        frame_width: int,
        frame_height: int,
        dtype: Any,
        buffer_frames: int,
    ) -> None:
        self.tiff_files = tiff_files
        self.frame_width = frame_width
        self.frame_height = frame_height
        self._dtype = np.dtype(dtype)
        self.buffer_frames = buffer_frames
        self._current_frame = 0
        self.read_time_s = 0.0

    def __iter__(self):
        return self

    def __next__(self) -> DataChunk:
        if self._current_frame >= len(self.tiff_files):
            raise StopIteration

        # Determine how many frames to read in this buffer
        start_frame = self._current_frame
        end_frame = min(start_frame + self.buffer_frames, len(self.tiff_files))

        # Read the frames
        start_time = time.perf_counter_ns()
        data = np.empty((end_frame - start_frame, self.frame_width, self.frame_height), dtype=self._dtype)
        for i, frame_idx in enumerate(range(start_frame, end_frame)):
            # Transpose from (height, width) to (width, height) for NWB
            data[i] = tifffile.imread(self.tiff_files[frame_idx]).T
        self.read_time_s += (time.perf_counter_ns() - start_time) / 1e9

        selection = np.s_[start_frame:end_frame, :, :]

        self._current_frame = end_frame
        return DataChunk(data=data, selection=selection)

    def recommended_chunk_shape(self) -> NoReturn:
        raise ValueError(
            "recommended_chunk_shape is not implemented for TiffDataChunkIterator and should not be "
            "called in this context because H5DataIO is managing chunk shapes."
        )

    def recommended_data_shape(self) -> tuple[int, int, int]:
        return self.maxshape

    @property
    def dtype(self) -> np.dtype:
        return self._dtype

    @property
    def maxshape(self) -> tuple[int, int, int]:
        return (len(self.tiff_files), self.frame_width, self.frame_height)


def main() -> None:
    usage = (
        "Usage: python3 exp12_write_ophys_tiff_to_nwb.py "
        "<config_table> <row> <input_nwb_filepath> <series_name> <tiff_dir> "
        "<output_label> <output_dir>"
    )
    if len(sys.argv) != 8:
        print(usage)
        sys.exit(1)
    config_table = Path(sys.argv[1])
    row = int(sys.argv[2])
    input_filepath = Path(sys.argv[3])  # Path to the input NWB file (for metadata)
    series_name = sys.argv[4]  # Name of the TwoPhotonSeries in the source NWB file
    tiff_dir = Path(sys.argv[5])  # Directory containing TIFF files
    output_label = sys.argv[6]  # Label for this dataset
    output_dir = Path(sys.argv[7])  # Directory to save output NWB files

    start_tot_time = time.perf_counter_ns()
    config = read_config(config_table, row)
    print(f"Config {row}: {config}")
    output_filepath = output_dir / f"{output_label}_{config.config_id}.nwb"
    stats_filepath = output_dir / f"stats_{output_label}_{config.config_id}.tsv"
    remove_outputs(output_filepath, stats_filepath)

    # Get sorted list of TIFF files
    tiff_files = sorted(tiff_dir.glob("*.tiff"))
    if not tiff_files:
        raise ValueError(f"No TIFF files found in {tiff_dir}")

    n_frames = len(tiff_files)

    # Read first TIFF to get dimensions and dtype
    first_frame = tifffile.imread(tiff_files[0])
    # TIFF is height x width, NWB stores width x height
    frame_height, frame_width = first_frame.shape
    dtype = first_frame.dtype
    assert dtype == np.uint16, f"Expected uint16 data type, got {dtype}"

    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        orig_2pseries = nwbfile.acquisition[series_name]

        # Validate TIFF frame count matches original series
        orig_n_frames = orig_2pseries.data.shape[0]
        if n_frames != orig_n_frames:
            raise ValueError(
                f"Number of TIFF files ({n_frames}) does not match "
                f"original series frame count ({orig_n_frames})"
            )

        # Validate TIFF dimensions match original series dimensions
        orig_width, orig_height = (
            orig_2pseries.data.shape[1],
            orig_2pseries.data.shape[2],
        )
        if frame_width != orig_width or frame_height != orig_height:
            raise ValueError(
                f"TIFF frame dimensions ({frame_width}x{frame_height}) do not match "
                f"original series dimensions ({orig_width}x{orig_height})"
            )

        start, stop = centered_window(n_frames, OPHYS_WINDOW_FRAMES)
        print(f"{series_name}: writing frames {start}:{stop} of {n_frames} as {OPHYS_SERIES_NAME}")

        data_iterator = TiffDataChunkIterator(
            tiff_files=tiff_files[start:stop],
            frame_width=frame_width,
            frame_height=frame_height,
            dtype=dtype,
            buffer_frames=write_buffer_length(config.chunks, WRITE_BUFFER_TARGET_FRAMES),
        )

        kwargs = dict(
            name=OPHYS_SERIES_NAME,
            description=orig_2pseries.description,
            data=H5DataIO(data=data_iterator, **h5dataio_kwargs(config)),
            unit=orig_2pseries.unit,
            conversion=orig_2pseries.conversion,
            resolution=orig_2pseries.resolution,
            comments=orig_2pseries.comments,
            dimension=orig_2pseries.dimension,
            imaging_plane=orig_2pseries.imaging_plane,
            **windowed_timing(orig_2pseries, start, stop),
        )

        replace_series(nwbfile, series_name, pynwb.ophys.TwoPhotonSeries(**kwargs))

        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(partial_path(output_filepath), "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)
        fsync(partial_path(output_filepath))
        net_time_write_s = (time.perf_counter_ns() - start_time_write) / 1e9

    publish_nwb(output_filepath)
    net_tot_time_s = (time.perf_counter_ns() - start_tot_time) / 1e9
    rows = series_rows(
        output_filepath,
        config.config_id,
        {OPHYS_SERIES_NAME: series_name},
        net_time_write_s,
        {OPHYS_SERIES_NAME: data_iterator.read_time_s},
        net_tot_time_s,
    )
    write_stats(stats_filepath, rows)
    print(f"Wrote {output_filepath} in {net_time_write_s:.1f} s; stats in {stats_filepath}")


if __name__ == "__main__":
    main()
