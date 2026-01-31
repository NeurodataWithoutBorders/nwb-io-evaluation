"""
Generate NWB files with TwoPhotonSeries data from TIFF files.

This script reads TwoPhotonSeries metadata from an existing NWB file and raw imaging data
from a directory of TIFF files (one per frame), then writes new NWB files using various
configurations of chunking and compression specified in a config file.

Usage:
    python exp12_write_ophys_tiff.py <config_file> <config_number> <input_nwb_filepath> \
        <series_name> <tiff_dir> <output_label> <output_dir>
"""

import sys
import time
from pathlib import Path
from typing import Any, NoReturn

import hdf5plugin  # enable HDF5 compression filters
from hdmf.backends.hdf5.h5_utils import H5DataIO
from hdmf.data_utils import AbstractDataChunkIterator, DataChunk
import numpy as np
import pynwb
import tifffile


class TiffDataChunkIterator(AbstractDataChunkIterator):
    """A data chunk iterator that reads frames from a directory of TIFF files."""

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

    def __iter__(self):
        return self

    def __next__(self) -> DataChunk:
        if self._current_frame >= len(self.tiff_files):
            raise StopIteration

        # Determine how many frames to read in this buffer
        start_frame = self._current_frame
        end_frame = min(start_frame + self.buffer_frames, len(self.tiff_files))

        # Read the frames
        frames = []
        for frame_idx in range(start_frame, end_frame):
            frame = tifffile.imread(self.tiff_files[frame_idx])
            # Transpose from (height, width) to (width, height) for NWB
            frames.append(frame.T)

        data = np.stack(frames, axis=0)
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


def process_config(
    config_number: int,
    input_filepath: Path,
    series_name: str,
    tiff_dir: Path,
    output_dir: Path,
    output_label: str,
    params: list[str],
) -> str:
    """Process a single configuration and return stats."""
    assert (
        len(params) == 5
    ), f"Config line {config_number} requires exactly 5 parameters, got {len(params)}"

    # Set chunk sizes
    if params[0].lower() == "true":
        chunk_shape = True
    else:
        chunk_shape = tuple(map(int, params[0].split(",")))

    # Set compression algo or filter id
    if params[2].lower() == "na":
        compr = str(params[1])
    else:
        compr = int(params[2])

    # Set compression level
    if params[1] == "gzip":
        comp_op = int(params[4])
    elif params[1] in ("lz4", "zstd"):
        comp_op = (int(params[4]),)
    elif params[1] == "zfp":
        comp_op = (int(params[3]), 0, 0, 0, 0, 0)
    elif params[1] in ("blosc-zstd", "blosc-lz4hc", "blosc-blosclz", "blosc-lz4"):
        comp_op = (0, 0, 0, 0, int(params[4]), 0, int(params[3]))
    else:
        comp_op = None

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

        data_iterator = TiffDataChunkIterator(
            tiff_files=tiff_files,
            frame_width=frame_width,
            frame_height=frame_height,
            dtype=dtype,
            buffer_frames=10000,  # 5-9 GB buffer depending on frame size
        )

        # Build H5DataIO parameters based on compression algorithm
        data_io_kwargs = {
            "data": data_iterator,
            "chunks": chunk_shape,
        }

        if params[1] != "NA":
            data_io_kwargs["compression"] = compr
            data_io_kwargs["allow_plugin_filters"] = True
            if comp_op is not None:
                data_io_kwargs["compression_opts"] = comp_op

        data = H5DataIO(**data_io_kwargs)

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

        output_filepath = output_dir / f"{output_label}_Config{config_number:03d}.nwb"

        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(output_filepath, "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)
        end_time_write = time.perf_counter_ns()
        net_time_write_s = (end_time_write - start_time_write) / 1e9

    stats = f"{config_number} {n_frames} {net_time_write_s:.4f} "

    try:
        file_size_gb = output_filepath.stat().st_size / (1024 * 1024 * 1024)
        stats += f"{file_size_gb} "
    except OSError:
        stats += "N/A "

    return stats


def main() -> None:
    usage = (
        "Usage: python3 exp12_write_ophys_tiff.py "
        "<config_file> <config_number> <input_nwb_filepath> <series_name> <tiff_dir> "
        "<output_label> <output_dir>"
    )
    assert len(sys.argv) == 8, usage
    config_file = Path(sys.argv[1])  # Path to the configuration file
    config_number = int(
        sys.argv[2]
    )  # Config number (corresponds to line in config file)
    input_filepath = Path(sys.argv[3])  # Path to the input NWB file (for metadata)
    series_name = sys.argv[4]  # Name of the TwoPhotonSeries in the NWB file
    tiff_dir = Path(sys.argv[5])  # Directory containing TIFF files
    output_label = sys.argv[6]  # Label for this experiment
    output_dir = Path(sys.argv[7])  # Directory to save output NWB files

    stats_filepath = output_dir / f"stats_{output_label}_Config{config_number:03d}.txt"
    header = "configNo n_frames t_write(s) file_size(Gb) total_t(s)\n"

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
                    series_name,
                    tiff_dir,
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
