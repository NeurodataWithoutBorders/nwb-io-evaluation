"""
The script generates NWB files using various configurations of chunking and compression

Created by Urjoshi Sinha on June 22, 2022.
Last modified by Ryan Ly on December 2, 2025.
"""

import os
import sys
import time
from typing import Any

import hdf5plugin  # enable HDF5 compression filters
from hdmf.backends.hdf5.h5_utils import H5DataIO
from hdmf.data_utils import GenericDataChunkIterator
import pynwb


class H5DatasetDataChunkIterator(GenericDataChunkIterator):
    """A data chunk iterator that reads chunks over the 0th dimension of an HDF5 dataset up to a max length."""

    def __init__(self, dataset: Any, max_length: int, **kwargs: Any) -> None:
        self.dataset = dataset
        self.max_length = max_length
        super().__init__(**kwargs)

    def _get_data(self, selection: Any) -> Any:
        return self.dataset[selection]

    def _get_maxshape(self) -> tuple[int, int, int]:
        return (self.max_length, self.dataset.shape[1], self.dataset.shape[2])

    def _get_dtype(self) -> Any:
        return self.dataset.dtype


def process_config(
    config_number: int,
    input_filepath: str,
    series_name: str,
    output_dir: str,
    output_label: str,
    params: list[str],
) -> str:
    """Process a single configuration and return stats."""
    assert len(params) == 5, f"Config line {config_number} requires exactly 5 parameters, got {len(params)}"

    # Set chunk sizes
    if params[0].lower() == "none":
        chunksizes = None
    elif params[0].lower() == "true":
        chunksizes = True
    else:
        chunksizes = tuple(map(int, params[0].split(",")))

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

    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        orig_2pseries = nwbfile.acquisition[series_name]

        assert orig_2pseries.data.chunks
        max_timestamps = orig_2pseries.data.shape[0]

        data_iterator = H5DatasetDataChunkIterator(
            dataset=orig_2pseries.data,
            max_length=max_timestamps,
            chunk_shape=orig_2pseries.data.chunks,
            buffer_gb=8,
        )

        # Build H5DataIO parameters based on compression algorithm
        data_io_kwargs = {
            "data": data_iterator,
            "chunks": chunksizes,
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
        if orig_2pseries.timestamps:
            kwargs["timestamps"] = orig_2pseries.timestamps[:]

        new_2pseries = pynwb.ophys.TwoPhotonSeries(**kwargs)

        nwbfile.acquisition.pop(series_name)
        nwbfile.add_acquisition(new_2pseries)

        output_filepath = f"{output_dir}/exp12_{output_label}_Config{config_number:03d}.nwb"

        start_time_write = time.perf_counter_ns()
        with pynwb.NWBHDF5IO(output_filepath, "w", manager=io.manager) as export_io:
            export_io.export(io, nwbfile)
        end_time_write = time.perf_counter_ns()
        net_time_write_s = (end_time_write - start_time_write) / 1e9

    stats = f"{config_number} {max_timestamps} {net_time_write_s:.4f} "

    try:
        file_size_gb = os.path.getsize(output_filepath) / (1024 * 1024 * 1024)
        stats += f"{file_size_gb} "
    except OSError:
        stats += "N/A "

    return stats


def main() -> None:
    usage = (
        "Usage: python3 generate_nwb_files.py "
        "<config_number> <input_filepath> <series_name> "
        "<output_label> <output_dir> <config_file>"
    )
    assert len(sys.argv) == 7, usage
    config_number = int(sys.argv[1])  # Config number (corresponds to line in config file)
    input_filepath = sys.argv[2]  # Path to the input NWB file
    series_name = sys.argv[3]  # Name of the TwoPhotonSeries in the NWB file to read
    output_label = sys.argv[4]  # Label for this input NWB file experiment
    output_dir = sys.argv[5]
    config_file = sys.argv[6]

    stats_filepath = f"{output_dir}/stats_exp12_{output_label}_Config{config_number:03d}.txt"
    header = "configNo n_timestamps t_target_write(s) file_size(Gb) total_t(s)\n"

    start_tot_time = time.perf_counter_ns()

    # Find and process the matching configuration
    stats = ""
    with open(config_file) as configset:
        for configno, config_line in enumerate(configset):
            if configno == config_number:
                params = config_line.strip().split()
                stats = process_config(
                    config_number,
                    input_filepath,
                    series_name,
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

    with open(stats_filepath, "a") as stats_f:
        stats_f.write(header)
        stats_f.write(stats)


if __name__ == "__main__":
    main()
