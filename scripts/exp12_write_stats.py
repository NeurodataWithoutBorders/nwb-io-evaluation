"""
Measurements and output files of the write scripts.

A write first removes its earlier outputs, writes the NWB file under a ".partial" name, checks the
written data against the source (see exp12_verify.py), renames the file into place, and then
writes the stats file. An interrupted or failed write therefore leaves neither the NWB file nor
the stats file, and exp12_check_nwb_files.py reports it as missing and lists any ".partial" file
it left.

Each write produces a tab-separated stats file with one row per series written:

    config_id           configuration (see exp12_configs.py), or "unchunked"
    series              name of the series in the test file's /acquisition
    source_series       name of the series in the source NWB file
    n_samples           samples (ecephys) or frames (ophys) written
    chunks              chunk shape of the written dataset, or "contiguous"
    t_write_s           wall time to write the whole file, including an fsync (t_fsync_s), so the
                        time covers writing the data through to the file system
    t_series_s          part of t_write_s spent writing this series' data: from the first read of
                        the source data to the last write of the series, including the source
                        reads. It leaves out the rest of the file and the fsync. Chunks still in
                        HDF5's chunk cache are compressed when the file closes, after this span.
                        The cache holds only chunks no larger than itself, and hdmf opens the file
                        it writes with a 32 MiB cache per dataset (hdmf.backends.hdf5.h5tools.
                        RDCC_NBYTES), so at most 32 MiB of data is compressed after the span.
    cpu_series_s        process CPU time over the t_series_s span, summed over all threads. With
                        one core per task this is close to the compression cost plus
                        cpu_source_read_s, and is much less sensitive to file system load than
                        t_series_s.
    t_source_read_s     part of t_series_s spent reading this series from the raw binary or TIFF
                        files
    cpu_source_read_s   process CPU time over the t_source_read_s intervals
    t_fsync_s           part of t_write_s spent in the fsync after the file is closed, flushing data
                        still in the client's write cache to the file system. t_series_s ends when
                        the last write returns, which can be before its data leave the cache, so
                        t_series_s + t_fsync_s is the time to get the series data to the file
                        system, plus the time to flush the small remainder of the file.
    t_verify_s          wall time to read the series back from the written file and compare it
                        with the source
    data_sha256         SHA-256 of the series data in C order, the same for every configuration of
                        a dataset
    file_bytes          size of the whole NWB file, which also holds the rest of the source file
    data_nbytes         uncompressed size of the series data
    data_storage_bytes  bytes the series data occupies in the file
    compression_ratio   data_nbytes / data_storage_bytes
    lustre_stripe       Lustre stripe count, stripe size in bytes, and starting OST index of the
                        file, when lfs exists, as count=8;size=16777216;index=11. For a composite
                        (progressive) layout these describe its last instantiated component,
                        which on scratch holds everything past 128 MiB (see the README).
    total_s             wall time of the whole script, including the check against the source

and these columns describing the run, the same for every row of a file:

    host                node the write ran on
    cpu_model           CPU model of the node
    cpu_affinity_count  CPU cores the process may run on
    slurm_job_id        SLURM job ID (the array job ID for an array task), when run under SLURM
    slurm_array_task_id SLURM array task ID, when run as an array task
    start_utc           UTC start time of the script, ISO 8601
    python, hdf5, h5py, hdf5plugin, hdmf, pynwb, numpy
                        versions of Python, the HDF5 library h5py is built with, and the packages
"""

import csv
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import time
from typing import Any, Self

import h5py

RUN_COLUMNS = (
    "host", "cpu_model", "cpu_affinity_count", "slurm_job_id", "slurm_array_task_id", "start_utc",
    "python", "hdf5", "h5py", "hdf5plugin", "hdmf", "pynwb", "numpy",
)
STATS_COLUMNS = (
    "config_id", "series", "source_series", "n_samples", "chunks", "t_write_s", "t_series_s",
    "cpu_series_s", "t_source_read_s", "cpu_source_read_s", "t_fsync_s", "t_verify_s", "data_sha256",
    "file_bytes", "data_nbytes", "data_storage_bytes", "compression_ratio", "lustre_stripe", "total_s",
) + RUN_COLUMNS


class Stopwatch:
    """Wall time and process CPU time, summed over the intervals it times.

    Time an interval with `with stopwatch:` or with start() and stop().
    """

    def __init__(self) -> None:
        self.wall_s = 0.0
        self.cpu_s = 0.0
        self._started: tuple[float, float] | None = None

    @property
    def running(self) -> bool:
        return self._started is not None

    def start(self) -> None:
        if self._started is not None:
            raise RuntimeError("Stopwatch is already running")
        self._started = (time.perf_counter(), time.process_time())

    def stop(self) -> None:
        if self._started is None:
            raise RuntimeError("Stopwatch is not running")
        wall0, cpu0 = self._started
        self.wall_s += time.perf_counter() - wall0
        self.cpu_s += time.process_time() - cpu0
        self._started = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()


@dataclass
class SeriesMeasurements:
    """Measurements of one series that the write script takes while writing and checking it."""

    source_name: str
    series: Stopwatch = field(default_factory=Stopwatch)
    source_read: Stopwatch = field(default_factory=Stopwatch)
    verify_s: float = 0.0
    data_sha256: str = ""


def cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor()


def cpu_affinity_count() -> int:
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 0


def run_info() -> dict[str, Any]:
    """Where and with what software the script runs. Call it when the script starts."""
    return {
        "host": socket.gethostname(),
        "cpu_model": cpu_model(),
        "cpu_affinity_count": cpu_affinity_count(),
        "slurm_job_id": os.environ.get("SLURM_ARRAY_JOB_ID", os.environ.get("SLURM_JOB_ID", "")),
        "slurm_array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID", ""),
        "start_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "hdf5": h5py.version.hdf5_version,
        **{package: version(package) for package in ("h5py", "hdf5plugin", "hdmf", "pynwb", "numpy")},
    }


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
    for flag in ("-c", "-S", "-i"):
        proc = subprocess.run(["lfs", "getstripe", flag, str(path)], capture_output=True, text=True)
        values.append(proc.stdout.strip())
    return f"count={values[0]};size={values[1]};index={values[2]}"


def series_rows(
    path: Path,
    config_id: str,
    series: dict[str, SeriesMeasurements],
    t_write_s: float,
    t_fsync_s: float,
    total_s: float,
    run: dict[str, Any],
) -> list[dict[str, Any]]:
    """One stats row per series, measured from the written file.

    series maps each series name in the written file to its measurements, and run is the
    run_info() of the script.
    """
    file_bytes = path.stat().st_size
    stripe = lustre_stripe(path)
    rows = []
    with h5py.File(path, "r") as f:
        for name, m in series.items():
            dset = f[f"acquisition/{name}/data"]
            nbytes = dset.size * dset.dtype.itemsize
            storage = dset.id.get_storage_size()
            rows.append({
                "config_id": config_id,
                "series": name,
                "source_series": m.source_name,
                "n_samples": dset.shape[0],
                "chunks": ",".join(str(n) for n in dset.chunks) if dset.chunks else "contiguous",
                "t_write_s": f"{t_write_s:.4f}",
                "t_series_s": f"{m.series.wall_s:.4f}",
                "cpu_series_s": f"{m.series.cpu_s:.4f}",
                "t_source_read_s": f"{m.source_read.wall_s:.4f}",
                "cpu_source_read_s": f"{m.source_read.cpu_s:.4f}",
                "t_fsync_s": f"{t_fsync_s:.4f}",
                "t_verify_s": f"{m.verify_s:.4f}",
                "data_sha256": m.data_sha256,
                "file_bytes": file_bytes,
                "data_nbytes": nbytes,
                "data_storage_bytes": storage,
                "compression_ratio": f"{nbytes / storage:.4f}",
                "lustre_stripe": stripe,
                "total_s": f"{total_s:.4f}",
                **run,
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
