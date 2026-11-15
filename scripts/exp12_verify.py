"""
Checking a written series against its source data.

The write scripts call verify_series on the ".partial" NWB file before renaming it into place, so
every NWB file at its final name holds exactly the source window. The check reads the file with
h5py and the HDF5 filters alone (the write scripts do not import b2h5py), and its SHA-256 of the
data is the reference for checking what other read paths return.
"""

from collections.abc import Callable
import hashlib
from pathlib import Path

import h5py
import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
import numpy as np

# The source is compared with each block read back in pieces of about this many bytes, so a check
# holds one block and one piece of the source in memory.
COMPARE_BYTES = 2**30


class DataMismatchError(ValueError):
    pass


def verify_series(
    path: Path,
    dataset_name: str,
    read_source: Callable[[int, int], np.ndarray],
    block_length: int,
) -> str:
    """Compare a dataset with its source and return the SHA-256 of its data in C order.

    read_source(start, stop) returns the source data for rows start:stop of the dataset. The
    dataset is read in blocks of block_length rows along its first axis. When block_length is a
    whole number of the dataset's chunk extents along that axis, each block covers whole chunks
    and each chunk is decompressed once.

    Raises DataMismatchError at the first piece that differs from the source.
    """
    digest = hashlib.sha256()
    with h5py.File(path, "r") as f:
        dset = f[dataset_name]
        n_rows = dset.shape[0]
        row_bytes = max(1, dset.dtype.itemsize * int(np.prod(dset.shape[1:])))
        compare_length = max(1, COMPARE_BYTES // row_bytes)
        for start in range(0, n_rows, block_length):
            stop = min(start + block_length, n_rows)
            block = dset[start:stop]
            for a in range(start, stop, compare_length):
                b = min(a + compare_length, stop)
                written = block[a - start:b - start]
                expected = read_source(a, b)
                if written.shape != expected.shape or not np.array_equal(written, expected):
                    raise DataMismatchError(
                        f"{path}:{dataset_name} rows {a}:{b} differ from the source "
                        f"(shapes {written.shape} and {expected.shape})"
                    )
            digest.update(block.data)
    return digest.hexdigest()
