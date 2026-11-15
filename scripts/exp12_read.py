"""
Read one test file with one pattern, read path, and chunk cache setting, and record the timings.

    python scripts/exp12_read.py <task> [--schedule PATH] [--input-dir DIR] [--output-dir DIR]

<task> is a task number in scripts/exp12_read_schedule.tsv (see exp12_read_schedule.py). The
script reads <input-dir>/<label>_<config_id>.nwb (default write_output/) and writes
<output-dir>/<task name>.h5 (default read_output/), where the task name is
<label>_<config_id>_<pattern>_<path>_<cache>_s<stage>.

Each unit of the pattern, a repeat or a sample (see exp12_read_patterns.py), runs in its own
child process. The child evicts the file's pages from the client page cache
(posix_fadvise DONTNEED), opens the file with h5py at the task's chunk cache setting, opens it
with pynwb, and times each read of the series data, in wall time and process CPU time. Only the
reads are timed. With one core per task, wall time minus CPU time is mostly time spent waiting on
storage. A unit that crashes is recorded with its exit status, and the task goes on with the next
unit, stopping after MAX_CRASHES crashed units. A sequential pattern stops early after
MAX_SLOW_REPEATS slow repeats (see exp12_read_patterns.py).

The parent process does not open the test file, so it does not bring the file into the page cache
before a unit. Lustre's server caches cannot be flushed from user space.

After the timed units, two checks run, each in a child process:

    selection  the first and last selections of the first unit, read through the task's read
               path, equal the same selections of the dataset's unchunked file (not run for the
               unchunked file itself)
    sha256     for the b2h5py path, the whole window read through b2h5py has the SHA-256 the
               write recorded in its stats file. The h5py path is the one the write checked.

The output file holds these root attributes:

    task, stage, label, row, config_id, pattern, path, cache
                        the task (row is -1 for unchunked)
    chunks, codec, level, shuffle
                        the configuration ("contiguous" and "none" for unchunked)
    data_shape          shape of the series
    rdcc_nbytes, rdcc_nslots, rdcc_w0
                        the chunk cache setting the file was opened with, as the child read it
                        back from the file
    n_units, n_units_run
                        units in the pattern, and units run before any early stop
    truncated           whether a sequential pattern stopped early on slow repeats
    slow_repeat_threshold_s
    n_crashed           units whose child process failed
    reads, fast_reads   timed reads, and those that b2h5py's fast path served
    page_cache_evicted  whether posix_fadvise was available to evict the file's pages
    check_selection, check_sha256
                        match, mismatch, missing reference, or not run
    start_utc, end_utc
    host, cpu_model, cpu_affinity_count, os_cpu_count, slurm_job_id, python, hdf5, h5py,
    hdf5plugin, hdmf, pynwb, numpy, b2h5py, blosc2, blosc_nthreads
                        where and with what software the reads ran

and these datasets, one row per unit (NaN for a unit that did not run or crashed):

    read_times, read_cpu_times          wall and CPU seconds of each read, units x reads
    unit_total_times, unit_total_cpu_times
                                        their sums per unit
    unit_returncode                     exit status of each unit's child process (-1 if not run)
    unit_fast_reads                     reads of each unit that b2h5py's fast path served
    unit_positions                      random positions of each unit (see unit_positions)
    read_lengths                        time steps of each read of a unit

The script exits with status 1 if a unit crashed or a check did not match, after writing the
output file.
"""

import argparse
import csv
from datetime import UTC, datetime
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Any

import b2h5py
import b2h5py.blosc2
import h5py
import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
import numpy as np
import pynwb

from exp12_configs import DATASETS, Config, config_table_path, read_config
from exp12_read_patterns import (
    CACHE_W0, MAX_SLOW_REPEATS, PATTERNS, SLOW_REPEAT_THRESHOLD_S, Pattern, cache_setting, to_index,
    unit_positions, unit_selections,
)
from exp12_read_schedule import SCHEDULE_PATH, UNCHUNKED, ReadTask, load_schedule
from exp12_series import ECEPHYS_SERIES_NAME, OPHYS_SERIES_NAME
from exp12_window import ECEPHYS_WINDOW_SAMPLES, OPHYS_WINDOW_FRAMES
from exp12_write_stats import run_info

REPO = Path(__file__).resolve().parents[1]
MAX_CRASHES = 3
SERIES_NAMES = {"ecephys": ECEPHYS_SERIES_NAME, "ophys": OPHYS_SERIES_NAME}
WINDOWS = {"ecephys": ECEPHYS_WINDOW_SAMPLES, "ophys": OPHYS_WINDOW_FRAMES}
# The sha256 check reads the window in blocks of whole chunk time extents of about this many bytes.
CHECK_BLOCK_BYTES = 2**30
STDERR_TAIL = 4000


# =============================================================================
# Child process: one unit or one check
# =============================================================================

class FastReadCounter:
    """Counts the reads that b2h5py's fast path serves.

    b2h5py's patched Dataset.__getitem__ calls b2h5py.blosc2.opt_selection_read for every read it
    serves itself and falls back to h5py for the rest, so wrapping that function counts them.
    """

    def __init__(self) -> None:
        self.count = 0
        original = b2h5py.blosc2.opt_selection_read

        def counting(*args: Any, **kwargs: Any) -> Any:
            self.count += 1
            return original(*args, **kwargs)

        b2h5py.blosc2.opt_selection_read = counting


def set_read_path(path: str) -> FastReadCounter:
    counter = FastReadCounter()
    if path == "b2h5py":
        b2h5py.enable_fast_slicing()
    else:
        b2h5py.disable_fast_slicing()
    return counter


def evict_page_cache(path: str) -> bool:
    """Evict the file's pages from the client page cache, where posix_fadvise is available."""
    if not hasattr(os, "posix_fadvise"):
        return False
    fd = os.open(path, os.O_RDONLY)
    try:
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)
    return True


def timed_read(data: Any, index: tuple) -> tuple[float, float]:
    """Read data[index] and return (wall time, CPU time) in seconds."""
    start_wall = time.perf_counter_ns()
    start_cpu = time.process_time_ns()
    _ = data[index]
    end_cpu = time.process_time_ns()
    end_wall = time.perf_counter_ns()
    return (end_wall - start_wall) / 1e9, (end_cpu - start_cpu) / 1e9


def open_series(spec: dict[str, Any]) -> tuple[h5py.File, pynwb.NWBHDF5IO, Any]:
    nbytes, nslots, w0 = spec["rdcc"]
    f = h5py.File(spec["file"], "r", rdcc_nbytes=nbytes, rdcc_nslots=nslots, rdcc_w0=w0)
    io = pynwb.NWBHDF5IO(mode="r", file=f)
    data = io.read().acquisition[spec["series"]].data
    return f, io, data


def child_unit(spec: dict[str, Any]) -> dict[str, Any]:
    counter = set_read_path(spec["path"])
    evicted = evict_page_cache(spec["file"])
    f, io, data = open_series(spec)
    try:
        _, nslots, nbytes, w0 = f.id.get_access_plist().get_cache()
        walls, cpus = [], []
        for selection in spec["selections"]:
            wall_s, cpu_s = timed_read(data, to_index(selection))
            walls.append(wall_s)
            cpus.append(cpu_s)
        return {"wall": walls, "cpu": cpus, "fast_reads": counter.count, "evicted": evicted,
                "rdcc": [nbytes, nslots, w0], "shape": list(data.shape),
                "chunks": None if data.chunks is None else list(data.chunks)}
    finally:
        io.close()
        f.close()


def child_check_selection(spec: dict[str, Any]) -> dict[str, Any]:
    set_read_path(spec["path"])
    f, io, data = open_series(spec)
    try:
        with h5py.File(spec["reference"], "r") as ref:
            ref_data = ref[f"acquisition/{spec['series']}/data"]
            for selection in spec["selections"]:
                index = to_index(selection)
                if not np.array_equal(data[index], ref_data[index]):
                    return {"result": "mismatch", "selection": selection}
        return {"result": "match"}
    finally:
        io.close()
        f.close()


def child_check_sha256(spec: dict[str, Any]) -> dict[str, Any]:
    counter = set_read_path(spec["path"])
    f, io, data = open_series(spec)
    try:
        digest = hashlib.sha256()
        row_bytes = data.dtype.itemsize * int(np.prod(data.shape[1:]))
        extent = data.chunks[0]
        block = max(1, CHECK_BLOCK_BYTES // (row_bytes * extent)) * extent
        for start in range(0, data.shape[0], block):
            digest.update(np.ascontiguousarray(data[start:start + block]).data)
        result = "match" if digest.hexdigest() == spec["sha256"] else "mismatch"
        return {"result": result, "sha256": digest.hexdigest(), "fast_reads": counter.count}
    finally:
        io.close()
        f.close()


CHILD_MODES = {"unit": child_unit, "check_selection": child_check_selection, "check_sha256": child_check_sha256}


def child_main(spec_path: str, result_path: str) -> None:
    with open(spec_path) as f:
        spec = json.load(f)
    result = CHILD_MODES[spec["mode"]](spec)
    with open(result_path, "w") as f:
        json.dump(result, f)


# =============================================================================
# Parent process: the task
# =============================================================================

def run_child(spec: dict[str, Any], workdir: Path) -> tuple[int, dict[str, Any] | None, str]:
    """Run one child process; return its exit status, its result, and the tail of its stderr."""
    spec_path = workdir / "spec.json"
    result_path = workdir / "result.json"
    result_path.unlink(missing_ok=True)
    spec_path.write_text(json.dumps(spec))
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "child", str(spec_path), str(result_path)],
        stderr=subprocess.PIPE, text=True,
    )
    if proc.stderr:
        sys.stderr.write(proc.stderr)
        sys.stderr.flush()
    result = json.loads(result_path.read_text()) if proc.returncode == 0 and result_path.exists() else None
    return proc.returncode, result, proc.stderr[-STDERR_TAIL:]


def find_task(schedule: Path, number: int) -> ReadTask:
    for task in load_schedule(schedule):
        if task.task == number:
            return task
    raise SystemExit(f"Task {number} is not in {schedule}")


def task_config(task: ReadTask) -> Config | None:
    if task.config_id == UNCHUNKED:
        return None
    config = read_config(config_table_path(task.label), task.row)
    if config.config_id != task.config_id:
        raise SystemExit(
            f"Row {task.row} of {config_table_path(task.label)} is {config.config_id}, but the schedule "
            f"lists {task.config_id} for task {task.task}. Regenerate the schedule with "
            "python scripts/exp12_read_schedule.py"
        )
    return config


def recorded_sha256(stats_path: Path) -> str:
    with open(stats_path, newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return rows[0]["data_sha256"] if rows else ""


def run_info_with_reads() -> dict[str, Any]:
    info = run_info()
    info["os_cpu_count"] = os.cpu_count() or 0
    info["b2h5py"] = version("b2h5py")
    info["blosc2"] = version("blosc2")
    info["blosc_nthreads"] = os.environ.get("BLOSC_NTHREADS", "")
    return info


def run_task(task: ReadTask, input_dir: Path, output_dir: Path) -> int:
    config = task_config(task)
    pattern: Pattern = PATTERNS[task.pattern]
    spec = DATASETS[task.label]
    modality = pattern.modality
    shape = (WINDOWS[modality], *spec["full_shape"][1:])
    itemsize = np.dtype(spec["dtype"]).itemsize
    chunks = None if config is None else config.chunks
    nbytes, nslots = cache_setting(task.cache, shape, chunks, itemsize)
    series = SERIES_NAMES[modality]

    input_path = input_dir / f"{task.label}_{task.config_id}.nwb"
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{task.name}.h5"
    partial = output_path.with_name(output_path.name + ".partial")
    output_path.unlink(missing_ok=True)
    partial.unlink(missing_ok=True)

    info = run_info_with_reads()
    print(f"Task {task.task}: {task.name}, cache {nbytes} bytes, {nslots} slots", flush=True)

    positions = unit_positions(pattern, shape)
    base = {"file": str(input_path), "series": series, "path": task.path, "rdcc": [nbytes, nslots, CACHE_W0]}
    n_units = pattern.n_units
    reads_per_unit = len(unit_selections(pattern, shape, positions[0].tolist()))
    read_times = np.full((n_units, reads_per_unit), np.nan)
    read_cpu_times = np.full((n_units, reads_per_unit), np.nan)
    returncodes = np.full(n_units, -1, dtype=np.int64)
    fast_reads = np.zeros(n_units, dtype=np.int64)
    crashes: list[str] = []
    slow = 0
    truncated = False
    evicted = True
    rdcc_in_file: list[Any] = [nbytes, nslots, CACHE_W0]
    threshold = SLOW_REPEAT_THRESHOLD_S[modality]

    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        n_run = 0
        for unit in range(n_units):
            selections = unit_selections(pattern, shape, positions[unit].tolist())
            code, result, stderr_tail = run_child({**base, "mode": "unit", "selections": selections}, workdir)
            n_run += 1
            returncodes[unit] = code
            if result is None:
                crashes.append(f"unit {unit}: exit status {code}: {stderr_tail}")
                print(f"  Unit {unit} failed with exit status {code}", flush=True)
                if len(crashes) >= MAX_CRASHES:
                    print(f"  Stopping after {len(crashes)} failed units", flush=True)
                    break
                continue
            read_times[unit] = result["wall"]
            read_cpu_times[unit] = result["cpu"]
            fast_reads[unit] = result["fast_reads"]
            evicted &= result["evicted"]
            rdcc_in_file = result["rdcc"]
            if list(result["shape"]) != list(shape):
                raise SystemExit(f"{input_path} holds shape {result['shape']}, expected {list(shape)}")
            total = float(np.sum(result["wall"]))
            print(f"  Unit {unit}: {total:.3f} s wall, {float(np.sum(result['cpu'])):.3f} s CPU", flush=True)
            if pattern.kind == "sequential" and total > threshold:
                slow += 1
                if slow >= MAX_SLOW_REPEATS:
                    print(f"  Stopping after {slow} repeats over {threshold} s", flush=True)
                    truncated = True
                    break

        first = unit_selections(pattern, shape, positions[0].tolist())
        reference = input_dir / f"{task.label}_{UNCHUNKED}.nwb"
        if config is None:
            check_selection = "not run"
        elif not reference.exists():
            check_selection = "missing reference"
        else:
            _, result, _ = run_child({**base, "mode": "check_selection", "reference": str(reference),
                                      "selections": [first[0], first[-1]]}, workdir)
            check_selection = "crashed" if result is None else result["result"]

        if task.path == "b2h5py":
            stats_path = input_dir / f"stats_{task.label}_{task.config_id}.tsv"
            sha256 = recorded_sha256(stats_path) if stats_path.exists() else ""
            if not sha256:
                check_sha256 = "missing reference"
            else:
                _, result, _ = run_child({**base, "mode": "check_sha256", "sha256": sha256}, workdir)
                check_sha256 = "crashed" if result is None else result["result"]
        else:
            check_sha256 = "not run"

    with h5py.File(partial, "w") as out:
        attrs = {
            "task": task.task, "stage": task.stage, "label": task.label,
            "row": -1 if task.row is None else task.row, "config_id": task.config_id,
            "pattern": task.pattern, "path": task.path, "cache": task.cache,
            "chunks": "contiguous" if chunks is None else ",".join(str(c) for c in chunks),
            "codec": "none" if config is None else config.codec,
            "level": -1 if config is None or config.level is None else config.level,
            "shuffle": "none" if config is None else config.shuffle,
            "data_shape": list(shape),
            "rdcc_nbytes": int(rdcc_in_file[0]), "rdcc_nslots": int(rdcc_in_file[1]),
            "rdcc_w0": float(rdcc_in_file[2]),
            "n_units": n_units, "n_units_run": n_run, "truncated": truncated,
            "slow_repeat_threshold_s": threshold if pattern.kind == "sequential" else -1,
            "n_crashed": len(crashes), "crashes": "\n\n".join(crashes),
            "reads": int(np.sum(~np.isnan(read_times))), "fast_reads": int(fast_reads.sum()),
            "page_cache_evicted": evicted,
            "check_selection": check_selection, "check_sha256": check_sha256,
            "start_utc": info["start_utc"],
            "end_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            **{k: v for k, v in info.items() if k != "start_utc"},
        }
        for key, value in attrs.items():
            out.attrs[key] = value
        out.create_dataset("read_times", data=read_times)
        out.create_dataset("read_cpu_times", data=read_cpu_times)
        out.create_dataset("unit_total_times", data=read_times.sum(axis=1))
        out.create_dataset("unit_total_cpu_times", data=read_cpu_times.sum(axis=1))
        out.create_dataset("unit_returncode", data=returncodes)
        out.create_dataset("unit_fast_reads", data=fast_reads)
        out.create_dataset("unit_positions", data=positions)
        out.create_dataset("read_lengths", data=np.array(
            [1 if isinstance(s[0], int) else s[0][1] - s[0][0] for s in first], dtype=np.int64))
    partial.replace(output_path)

    print(f"Checks: selection {check_selection}, sha256 {check_sha256}; "
          f"{len(crashes)} failed units; results in {output_path}", flush=True)
    failed = crashes or check_selection in ("mismatch", "crashed", "missing reference") or \
        check_sha256 in ("mismatch", "crashed", "missing reference")
    return 1 if failed else 0


def main() -> None:
    if len(sys.argv) == 4 and sys.argv[1] == "child":
        child_main(sys.argv[2], sys.argv[3])
        return
    parser = argparse.ArgumentParser(description="Run one read task of the read schedule.")
    parser.add_argument("task", type=int)
    parser.add_argument("--schedule", type=Path, default=SCHEDULE_PATH)
    parser.add_argument("--input-dir", type=Path, default=REPO / "write_output")
    parser.add_argument("--output-dir", type=Path, default=REPO / "read_output")
    args = parser.parse_args()
    sys.exit(run_task(find_task(args.schedule, args.task), args.input_dir, args.output_dir))


if __name__ == "__main__":
    main()
