#!/usr/bin/env python3
"""
Export the frames of a TwoPhotonSeries in an NWB file to uncompressed multi-page TIFF files.

Each file holds FRAMES_PER_FILE consecutive frames (the last file holds the rest), one page per
frame, in the layout described in exp12_tiff.py. The output directory must be empty or not exist.
"""

import sys
from pathlib import Path

import numpy as np
import pynwb
import tifffile
from tqdm import tqdm

from exp12_tiff import FRAMES_PER_FILE, tiff_file_name


def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python exp12_nwb_to_tiff.py <nwb_file> <series_name> <output_dir>")
        print("Example: python exp12_nwb_to_tiff.py data.nwb TwoPhotonSeries ./tiffs")
        sys.exit(1)

    nwb_filepath = Path(sys.argv[1])
    series_name = sys.argv[2]
    output_dir = Path(sys.argv[3])

    if not nwb_filepath.exists():
        print(f"Error: NWB file not found: {nwb_filepath}")
        sys.exit(1)
    if output_dir.exists() and any(output_dir.iterdir()):
        print(f"Error: {output_dir} is not empty")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    with pynwb.NWBHDF5IO(nwb_filepath, "r") as io:
        nwbfile = io.read()

        if series_name not in nwbfile.acquisition:
            print(f"Error: '{series_name}' not found in acquisition group")
            print(f"Available: {list(nwbfile.acquisition.keys())}")
            sys.exit(1)

        series = nwbfile.acquisition[series_name]
        data = series.data
        n_frames = data.shape[0]

        print(f"Exporting {n_frames} frames from {series_name} in files of {FRAMES_PER_FILE} frames")
        print(f"Frame shape: {data.shape[1]}x{data.shape[2]} (width x height)")
        print(f"Data type: {data.dtype}")

        for file_index, start in enumerate(tqdm(range(0, n_frames, FRAMES_PER_FILE), desc="Exporting",
                                                unit="file")):
            stop = min(start + FRAMES_PER_FILE, n_frames)
            # Transpose each frame from (width, height) to (height, width)
            frames = np.ascontiguousarray(data[start:stop].transpose(0, 2, 1))
            tifffile.imwrite(output_dir / tiff_file_name(file_index), frames, photometric="minisblack",
                             compression=None)

    print(f"Done. TIFF files saved to {output_dir}")


if __name__ == "__main__":
    main()
