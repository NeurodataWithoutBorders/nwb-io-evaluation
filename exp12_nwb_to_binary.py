#!/usr/bin/env python3
"""Export data from an ElectricalSeries in an NWB file to a flat binary file in time-major order."""

import sys
from pathlib import Path

import numpy as np
import pynwb
from tqdm import tqdm


def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python exp12_nwb_to_binary.py <nwb_file> <series_name> <output_file>")
        print("Example: python exp12_nwb_to_binary.py data.nwb ElectricalSeries output.bin")
        sys.exit(1)

    nwb_filepath = Path(sys.argv[1])
    series_name = sys.argv[2]
    output_filepath = Path(sys.argv[3])

    if not nwb_filepath.exists():
        print(f"Error: NWB file not found: {nwb_filepath}")
        sys.exit(1)

    output_filepath.parent.mkdir(parents=True, exist_ok=True)

    with pynwb.NWBHDF5IO(nwb_filepath, "r") as io:
        nwbfile = io.read()

        if series_name not in nwbfile.acquisition:
            print(f"Error: '{series_name}' not found in acquisition group")
            print(f"Available: {list(nwbfile.acquisition.keys())}")
            sys.exit(1)

        series = nwbfile.acquisition[series_name]
        data = series.data
        n_samples = data.shape[0]
        n_channels = data.shape[1] if len(data.shape) > 1 else 1

        # Some data have more channels (e.g., 385 if the sync channel is included). Since the other channels
        # are not neural and we are simulating creating an ElectricalSeries with only neural data, we limit to
        # 384 channels.
        max_channels = 384
        n_channels_out = min(n_channels, max_channels)

        print(f"Exporting {series_name}")
        print(f"Shape: {data.shape} (samples x channels)")
        if n_channels > max_channels:
            print(f"Limiting to first {max_channels} of {n_channels} channels")
        print(f"Data type: {data.dtype}")
        print(f"Output will be time-major order (C order)")

        # Read and write in chunks to handle large datasets
        chunk_size = 100000  # samples per chunk
        with open(output_filepath, "wb") as f:
            for start_idx in tqdm(range(0, n_samples, chunk_size), desc="Exporting", unit="chunk"):
                end_idx = min(start_idx + chunk_size, n_samples)
                chunk = data[start_idx:end_idx, :n_channels_out]

                # Ensure C-contiguous (time-major) order
                chunk = np.ascontiguousarray(chunk)
                chunk.tofile(f)

    print(f"Done. Binary file saved to {output_filepath}")
    print(f"To read in Python: np.fromfile('{output_filepath}', dtype='{data.dtype}').reshape({n_samples}, {n_channels_out})")


if __name__ == "__main__":
    main()
