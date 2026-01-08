#!/usr/bin/env python3
"""Export frames from a TwoPhotonSeries in an NWB file to uncompressed TIFF files."""

import sys
from pathlib import Path

import pynwb
import tifffile


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

        print(f"Exporting {n_frames} frames from {series_name}")
        print(f"Frame shape: {data.shape[1]}x{data.shape[2]} (width x height)")
        print(f"Data type: {data.dtype}")

        for frame_idx in range(n_frames):
            frame = data[frame_idx, :, :].T  # transpose from (width, height) to (height, width)
            output_path = output_dir / f"frame_{frame_idx:06d}.tiff"
            tifffile.imwrite(output_path, frame, compression=None)

            if (frame_idx + 1) % 100 == 0 or frame_idx == n_frames - 1:
                print(f"  Exported {frame_idx + 1}/{n_frames} frames")

    print(f"Done. TIFF files saved to {output_dir}")


if __name__ == "__main__":
    main()
