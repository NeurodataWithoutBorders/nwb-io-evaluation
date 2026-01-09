"""
This script reads a slice of an ophys NWB HDF5 file for different use cases and records the result as an HDF5 file.

UC4: (span all time, 16, 16) - the 16x16 patch is randomly located in x and y dimensions
UC5: (span 4000, all x, all y) - the time slice is randomly located in the time dimension

Each use case is read 100 times with different random locations, and the time taken for each read is recorded.

Note that different datasets will have different lengths of time, x, and y dimensions.
"""

import math
import pynwb
import sys
import random
import time
import h5py
import hdf5plugin  # enable HDF5 compression filters

ucs = dict(
    uc4=dict(
        span_t=None,
        span_x=16,
        span_y=16,
    ),
    uc5=dict(
        span_t=4000,
        span_x=None,
        span_y=None,
    ),
)

def time_slice(t0: int, t1: int, x0: int, x1: int, y0: int, y1: int, series: pynwb.ophys.TwoPhotonSeries) -> float:
    """Time reading a slice of the TwoPhotonSeries data array in seconds."""
    start_eval = time.perf_counter_ns()
    series.data[t0:t1, x0:x1, y0:y1]
    end_eval = time.perf_counter_ns()
    return (end_eval - start_eval) / math.pow(10, 9)  # time in seconds


def gen_random(n_timestamps: int, spans: int) -> int:
    num = random.randint(0, n_timestamps-spans)
    return num


def main():
    config_number = int(sys.argv[1])  # Config number (corresponds to line in config file)
    input_dir = sys.argv[2]  # Path to input NWB files
    series_name = sys.argv[3]  # Name of the TwoPhotonSeries in the NWB file to read
    output_label = sys.argv[4]  # Label for this input NWB file experiment
    output_dir = sys.argv[5]  # Path to directory to write output HDF5 files

    max_samples = 100
    random.seed(30)

    input_filepath = f"{input_dir}/{output_label}_Config{config_number:03d}.nwb"
    output_filepath = f"{output_dir}/read_{output_label}_Config{config_number:03d}.h5"

    with pynwb.NWBHDF5IO(input_filepath, "r") as io:
        nwbfile = io.read()
        series = nwbfile.acquisition[series_name]
        dim_t = series.data.shape[0]
        dim_x = series.data.shape[1]
        dim_y = series.data.shape[2]

        with h5py.File(output_filepath, "w") as f:
            for usecase, spans in ucs.items():
                span_t = spans["span_t"]
                span_x = spans["span_x"]
                span_y = spans["span_y"]
                if span_t is None:
                    span_t = dim_t
                if span_x is None:
                    span_x = dim_x
                if span_y is None:
                    span_y = dim_y

                samples_dataset_path = f"/{usecase}/t{span_t}x{span_x}_y{span_y}/samples_t0x0y0"
                results_dataset_path = f"/{usecase}/t{span_t}x{span_x}_y{span_y}/time_read"

                f.require_dataset(samples_dataset_path, shape=(max_samples, 3), dtype="i")
                f.require_dataset(results_dataset_path, shape=(max_samples,), dtype="f")

                for samples in range(0, max_samples):
                    t0 = gen_random(dim_t, span_t)  # for uc4, t0 can only be 0
                    x0 = gen_random(dim_x, span_x)  # for uc5, x0 can only be 0
                    y0 = gen_random(dim_y, span_y)  # for uc5, y0 can only be 0
                    t1 = t0 + span_t
                    x1 = x0 + span_x
                    y1 = y0 + span_y
                    f[samples_dataset_path][samples,0] = t0
                    f[samples_dataset_path][samples,1] = x0
                    f[samples_dataset_path][samples,2] = y0

                    exec_time = time_slice(t0, t1, x0, x1, y0, y1, series)

                    # write results
                    f[results_dataset_path][samples] = exec_time


if __name__ == "__main__":
    main()
