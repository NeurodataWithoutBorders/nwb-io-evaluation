"""
Measurements and output files of the write scripts.

A write first removes its earlier outputs, writes the NWB file under a ".partial" name, renames
it into place once complete, and then writes the stats file. An interrupted write therefore leaves
neither the NWB file nor the stats file, and exp12_check_nwb_files.py reports it as missing.

Each write produces a tab-separated stats file with one row per series written:

    config_id           configuration (see exp12_configs.py), or "unchunked"
    series              name of the series in the test file's /acquisition
    source_series       name of the series in the source NWB file
    n_samples           samples (ecephys) or frames (ophys) written
    chunks              chunk shape of the written dataset, or "contiguous"
    t_write_s           wall time to write the whole file, including an fsync, so the time covers
                        writing the data through to the file system
    t_source_read_s     part of t_write_s spent reading this series from the raw binary or TIFF
                        files
    file_bytes          size of the whole NWB file, which also holds the rest of the source file
    data_nbytes         uncompressed size of the series data
    data_storage_bytes  bytes the series data occupies in the file
    compression_ratio   data_nbytes / data_storage_bytes
    lustre_stripe       Lustre stripe count and starting OST index of the file, when lfs exists
    total_s             wall time of the whole script
"""

import csv
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any

import h5py

STATS_COLUMNS = (
    "config_id", "series", "source_series", "n_samples", "chunks", "t_write_s", "t_source_read_s",
    "file_bytes", "data_nbytes", "data_storage_bytes", "compression_ratio", "lustre_stripe", "total_s",
)


def fsync(path: Path) -> None:
    fd = os.open(path, os.O_RDWR)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def lustre_stripe(path: Path) -> str:
    if shutil.which("lfs") is None:
        return ""
    values = []
    for flag in ("-c", "-i"):
        proc = subprocess.run(["lfs", "getstripe", flag, str(path)], capture_output=True, text=True)
        values.append(proc.stdout.strip())
    return f"count={values[0]};index={values[1]}"


def series_rows(
    path: Path,
    config_id: str,
    series: dict[str, str],
    t_write_s: float,
    t_source_read_s: dict[str, float],
    total_s: float,
) -> list[dict[str, Any]]:
    """One stats row per series, measured from the written file.

    series maps each series name in the written file to its name in the source file, and
    t_source_read_s is keyed by the name in the written file.
    """
    file_bytes = path.stat().st_size
    stripe = lustre_stripe(path)
    rows = []
    with h5py.File(path, "r") as f:
        for name, source_name in series.items():
            dset = f[f"acquisition/{name}/data"]
            nbytes = dset.size * dset.dtype.itemsize
            storage = dset.id.get_storage_size()
            rows.append({
                "config_id": config_id,
                "series": name,
                "source_series": source_name,
                "n_samples": dset.shape[0],
                "chunks": ",".join(str(n) for n in dset.chunks) if dset.chunks else "contiguous",
                "t_write_s": f"{t_write_s:.4f}",
                "t_source_read_s": f"{t_source_read_s[name]:.4f}",
                "file_bytes": file_bytes,
                "data_nbytes": nbytes,
                "data_storage_bytes": storage,
                "compression_ratio": f"{nbytes / storage:.4f}",
                "lustre_stripe": stripe,
                "total_s": f"{total_s:.4f}",
            })
    return rows


def partial_path(path: Path) -> Path:
    """Where a write creates the NWB file at `path` before it is complete."""
    return path.with_name(path.name + ".partial")


def remove_outputs(nwb_path: Path, stats_path: Path) -> None:
    """Remove the outputs of an earlier write of the same file."""
    for path in (nwb_path, stats_path, partial_path(nwb_path)):
        path.unlink(missing_ok=True)


def publish_nwb(nwb_path: Path) -> None:
    """Move the completed NWB file from its partial path into place."""
    os.replace(partial_path(nwb_path), nwb_path)


def write_stats(path: Path, rows: list[dict[str, Any]]) -> None:
    tmp_path = path.with_name(path.name + ".partial")
    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STATS_COLUMNS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)
