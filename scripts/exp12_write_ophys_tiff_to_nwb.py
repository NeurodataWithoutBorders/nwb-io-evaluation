"""
Generate an NWB file with TwoPhotonSeries data from TIFF files, using one chunking and
compression configuration.

This script reads TwoPhotonSeries metadata from an existing NWB file and raw imaging data
from a directory of multi-page TIFF files (see exp12_tiff.py), then writes a new NWB file using the
configuration in one row of a config table (see exp12_configs.py). The new series holds the
window of frames defined in exp12_window.py, with its timing metadata shifted to match, and is
named TwoPhotonSeries (see exp12_series.py for what else the test file leaves out).

Writes <output_dir>/<output_label>_<config_id>.nwb and a stats table
<output_dir>/stats_<output_label>_<config_id>.tsv (see exp12_write_stats.py).
The written data is checked against the source before the NWB file gets its final name
(see exp12_verify.py).

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
from typing import NoReturn

import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
from hdmf.backends.hdf5.h5_utils import H5DataIO
from hdmf.data_utils import AbstractDataChunkIterator, DataChunk
import numpy as np
import pynwb

from exp12_configs import h5dataio_kwargs, read_config, write_buffer_length
from exp12_series import OPHYS_SERIES_NAME, replace_series
from exp12_tiff import TiffStack
from exp12_verify import verify_series
from exp12_window import OPHYS_WINDOW_FRAMES, centered_window, windowed_timing
from exp12_write_stats import (
    SeriesMeasurements, fsync, partial_path, publish_nwb, remove_outputs, run_info, series_rows, write_stats,
)

# Each write buffer is the largest whole number of the configuration's chunk time extent that fits
# in this many frames (10.7 GB for 512 x 512 frames, 16.7 GB for 796 x 512 frames), so every chunk
# is written in one piece.
WRITE_BUFFER_TARGET_FRAMES = 20_480


class TiffDataChunkIterator(AbstractDataChunkIterator):
    """A data chunk iterator that reads frames start:stop of a TiffStack, in NWB orientation.

    measurements.series times the span from the first call to __next__ to StopIteration, and
    measurements.source_read the reads of the TIFF files. hdmf writes the data of iterators after
    the rest of the file, so that span covers writing this series alone.
    """

    def __init__(self, stack: TiffStack, start: int, stop: int, buffer_frames: int,
                 measurements: SeriesMeasurements) -> None:
        self.stack = stack
        self.start = start
        self.n_frames = stop - start
        self.buffer_frames = buffer_frames
        self._current_frame = 0
        self.measurements = measurements

    def __iter__(self):
        return self

    def __next__(self) -> DataChunk:
        if self._current_frame == 0 and not self.measurements.series.running:
            self.measurements.series.start()
        if self._current_frame >= self.n_frames:
            if self.measurements.series.running:
                self.measurements.series.stop()
            raise StopIteration

        start_frame = self._current_frame
        end_frame = min(start_frame + self.buffer_frames, self.n_frames)

        with self.measurements.source_read:
            data = self.stack.read(self.start + start_frame, self.start + end_frame)

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
        return self.stack.dtype

    @property
    def maxshape(self) -> tuple[int, int, int]:
        return (self.n_frames, self.stack.frame_width, self.stack.frame_height)


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
    tiff_dir = Path(sys.argv[5])  # Directory of multi-page TIFF files from exp12_nwb_to_tiff.py
    output_label = sys.argv[6]  # Label for this dataset
    output_dir = Path(sys.argv[7])  # Directory to save output NWB files

    start_tot_time = time.perf_counter_ns()
    run = run_info()
    config = read_config(config_table, row)
    print(f"Config {row}: {config}")
    output_filepath = output_dir / f"{output_label}_{config.config_id}.nwb"
    stats_filepath = output_dir / f"stats_{output_label}_{config.config_id}.tsv"
    remove_outputs(output_filepath, stats_filepath)

    stack = TiffStack(tiff_dir)
    assert stack.dtype == np.uint16, f"Expected uint16 data type, got {stack.dtype}"

    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        orig_2pseries = nwbfile.acquisition[series_name]
        stack.check_shape(orig_2pseries.data.shape)

        start, stop = centered_window(stack.n_frames, OPHYS_WINDOW_FRAMES)
        print(f"{series_name}: writing frames {start}:{stop} of {stack.n_frames} as {OPHYS_SERIES_NAME}")

        buffer_frames = write_buffer_length(config.chunks, WRITE_BUFFER_TARGET_FRAMES)
        measurements = SeriesMeasurements(source_name=series_name)
        data_iterator = TiffDataChunkIterator(
            stack=stack,
            start=start,
            stop=stop,
            buffer_frames=buffer_frames,
            measurements=measurements,
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

    start_time_verify = time.perf_counter_ns()
    measurements.data_sha256 = verify_series(
        partial_path(output_filepath),
        f"acquisition/{OPHYS_SERIES_NAME}/data",
        lambda a, b: stack.read(start + a, start + b),
        buffer_frames,
    )
    measurements.verify_s = (time.perf_counter_ns() - start_time_verify) / 1e9
    print(f"Data matches the source; SHA-256 {measurements.data_sha256}")

    publish_nwb(output_filepath)
    net_tot_time_s = (time.perf_counter_ns() - start_tot_time) / 1e9
    rows = series_rows(
        output_filepath,
        config.config_id,
        {OPHYS_SERIES_NAME: measurements},
        net_time_write_s,
        net_tot_time_s,
        run,
    )
    write_stats(stats_filepath, rows)
    print(f"Wrote {output_filepath} in {net_time_write_s:.1f} s; stats in {stats_filepath}")


if __name__ == "__main__":
    main()
