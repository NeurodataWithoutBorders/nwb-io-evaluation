"""
Raw ophys data as a directory of multi-page TIFF files.

Each TIFF file holds a run of consecutive frames, one page per frame, in the layout ScanImage
writes when it splits a session into files of a fixed number of frames. exp12_nwb_to_tiff.py
writes the files, named so that sorting the names puts the frames in order. Each page is a frame
in TIFF orientation, (height, width). NWB stores frames as (width, height), so TiffStack.read
returns frames transposed into NWB orientation.
"""

from pathlib import Path

import numpy as np
import tifffile

TIFF_SUFFIX = ".tif"

# 1,000 frames is 524 MB per file for 512 x 512 frames and 815 MB for 796 x 512 frames.
FRAMES_PER_FILE = 1000


def tiff_file_name(file_index: int) -> str:
    return f"frames_{file_index:05d}{TIFF_SUFFIX}"


class TiffStack:
    """The frames of all multi-page TIFF files in a directory, in file name order."""

    def __init__(self, tiff_dir: Path) -> None:
        self.files = sorted(tiff_dir.glob(f"*{TIFF_SUFFIX}"))
        if not self.files:
            raise ValueError(f"No {TIFF_SUFFIX} files found in {tiff_dir}")
        frame_counts = []
        page_shape = dtype = None
        for path in self.files:
            with tifffile.TiffFile(path) as tif:
                series = tif.series[0]
                if len(series.shape) != 3:
                    raise ValueError(f"{path}: expected a (frames, height, width) series, got shape {series.shape}")
                if page_shape is None:
                    page_shape, dtype = series.shape[1:], series.dtype
                elif series.shape[1:] != page_shape or series.dtype != dtype:
                    raise ValueError(
                        f"{path}: frames are {series.shape[1:]} {series.dtype}, but {self.files[0]} has "
                        f"{page_shape} {dtype}"
                    )
                frame_counts.append(series.shape[0])
        self.frame_height, self.frame_width = page_shape
        self.dtype = np.dtype(dtype)
        # self._file_starts[i] is the index of the first frame in self.files[i].
        self._file_starts = np.concatenate(([0], np.cumsum(frame_counts)))
        self.n_frames = int(self._file_starts[-1])

    @property
    def shape(self) -> tuple[int, int, int]:
        """(frames, width, height), the shape of the stack in NWB orientation."""
        return (self.n_frames, self.frame_width, self.frame_height)

    def check_shape(self, nwb_shape: tuple[int, ...]) -> None:
        """Raise ValueError unless the stack has the shape of an NWB series with data shape nwb_shape."""
        if self.shape != tuple(nwb_shape):
            raise ValueError(
                f"The TIFF stack has shape {self.shape} (frames, width, height), but the NWB series has "
                f"shape {tuple(nwb_shape)}"
            )

    def read(self, start: int, stop: int) -> np.ndarray:
        """Frames start:stop as an array of shape (stop - start, width, height)."""
        if not 0 <= start <= stop <= self.n_frames:
            raise ValueError(f"Frames {start}:{stop} are outside the {self.n_frames} frames of the stack")
        out = np.empty((stop - start, self.frame_width, self.frame_height), dtype=self.dtype)
        file_index = int(np.searchsorted(self._file_starts, start, side="right")) - 1
        frame = start
        while frame < stop:
            file_start = int(self._file_starts[file_index])
            file_stop = min(stop, int(self._file_starts[file_index + 1]))
            with tifffile.TiffFile(self.files[file_index]) as tif:
                pages = tif.asarray(key=range(frame - file_start, file_stop - file_start))
            pages = pages.reshape(file_stop - frame, self.frame_height, self.frame_width)
            out[frame - start:file_stop - start] = pages.transpose(0, 2, 1)
            frame = file_stop
            file_index += 1
        return out
