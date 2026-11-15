"""
Read one test file with one pattern, read path, and chunk cache setting, and record the timings.

    python scripts/exp12_read.py <task> [--schedule PATH] [--input-dir DIR] [--output-dir DIR]
        [--time-limit-s SECONDS]

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
MAX_SLOW_UNITS slow units (see exp12_read_patterns.py).

The parent process does not open the test file, so it does not bring the file into the page cache
before a unit. Lustre's server caches cannot be flushed from user space.

The task fits in the job's time limit, --time-limit-s (default TIME_LIMIT_S, the --time of
exp12_read.sh). No unit starts once the elapsed time plus the longest unit so far would pass
UNIT_MARGIN_S before the limit, a unit still running at that point is stopped and left out, and
the checks are stopped CHECK_MARGIN_S before the limit. The output file is rewritten after every
completed unit, so a job killed before it finishes keeps the units it completed, with complete
false.

After the timed units, three checks run, the first two each in a child process:

    selection  the first and last selections of the first unit, read through the task's read
               path, equal the same selections of the dataset's unchunked file (not run for the
               unchunked file itself)
    sha256     for the b2h5py path, the whole window read through b2h5py has the SHA-256 the
               write recorded in its stats file. The h5py path is the one the write checked.
    fast_path  b2h5py's fast path served every timed read of the b2h5py path and none of the h5py
               path, so each path's timings are of that path

The output file holds these root attributes:

    task, stage, label, row, config_id, pattern, path, cache
                        the task (row is -1 for unchunked)
    chunks, codec, level, shuffle
                        the configuration ("contiguous" and "none" for unchunked)
    data_shape          shape of the series
    rdcc_nbytes, rdcc_nslots, rdcc_w0
                        the chunk cache setting the series was read with, as the child read it
                        back from the series (from the file for unchunked, which has no chunk cache)
    n_units, n_units_run
                        units in the pattern, and units started
    complete            whether the task finished: it ran every unit or stopped early, and ran the
                        checks. A file with complete false is from a job that was killed
    stop_reason         why the task ran fewer than n_units units: slow units, time budget, or
                        failed units (MAX_CRASHES crashed units), or empty
    truncated           whether the task stopped early on slow units or the time budget, so its
                        completed units are a right-censored sample
    slow_unit_threshold_s
                        SLOW_UNIT_THRESHOLD_S of a sequential pattern, -1 for the others
    time_limit_s        the time limit the task was fitted in
    n_crashed           units whose child process failed
    reads, fast_reads   timed reads, and those that b2h5py's fast path served
    page_cache_evicted  whether posix_fadvise was available to evict the file's pages
    check_selection, check_sha256, check_fast_path
                        match, mismatch, missing reference, crashed, timed out, or not run
    start_utc, end_utc
    host, cpu_model, cpu_affinity_count, os_cpu_count, slurm_job_id, python, hdf5, h5py,
    hdf5plugin, hdmf, pynwb, numpy, b2h5py, blosc2, blosc2_nthreads
                        where and with what software the reads ran; blosc2_nthreads is the
                        number of threads blosc2, and so b2h5py, decompresses with

and these datasets, one row per unit (NaN for a unit that did not complete):

    read_times, read_cpu_times          wall and CPU seconds of each read, units x reads
    unit_total_times, unit_total_cpu_times
                                        their sums per unit
    unit_returncode                     exit status of each unit's child process (NOT_RUN if not
                                        run, STOPPED if stopped at the time budget)
    unit_fast_reads                     reads of each unit that b2h5py's fast path served
    unit_positions                      random positions of each unit (see unit_positions)
    read_lengths                        time steps of each read of a unit

The script exits with status 1 if a unit crashed or a check did not match or did not run to its
end, after writing the output file.
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
import blosc2
import h5py
import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
import numpy as np
import pynwb

from exp12_configs import DATASETS, Config, config_table_path, read_config
from exp12_read_patterns import (
    CACHE_W0, MAX_SLOW_UNITS, PATTERNS, SLOW_UNIT_THRESHOLD_S, Pattern, cache_setting, to_index,
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

# The --time of exp12_read.sh. Units stop UNIT_MARGIN_S before it, leaving time for the checks,
# which stop CHECK_MARGIN_S before it, leaving time to write the output.
TIME_LIMIT_S = 24 * 3600
UNIT_MARGIN_S = 2 * 3600
CHECK_MARGIN_S = 30 * 60

# unit_returncode of a unit that did not run, and of one stopped at the time budget.
NOT_RUN = -1
STOPPED = -2

# Check results that fail the task.
FAILED_CHECKS = ("mismatch", "crashed", "missing reference", "timed out")


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


def chunk_cache(f: h5py.File, data: h5py.Dataset) -> list[Any]:
    """[rdcc_nbytes, rdcc_nslots, rdcc_w0] that reads of data use: the dataset's own chunk cache,
    or the file's for a contiguous dataset, which has none."""
    if data.chunks is None:
        _, nslots, nbytes, w0 = f.id.get_access_plist().get_cache()
    else:
        nslots, nbytes, w0 = data.id.get_access_plist().get_chunk_cache()
    return [nbytes, nslots, w0]


def child_unit(spec: dict[str, Any]) -> dict[str, Any]:
    counter = set_read_path(spec["path"])
    evicted = evict_page_cache(spec["file"])
    f, io, data = open_series(spec)
    try:
        counter.count = 0
        walls, cpus = [], []
        for selection in spec["selections"]:
            wall_s, cpu_s = timed_read(data, to_index(selection))
            walls.append(wall_s)
            cpus.append(cpu_s)
        return {"wall": walls, "cpu": cpus, "fast_reads": counter.count, "evicted": evicted,
                "rdcc": chunk_cache(f, data), "shape": list(data.shape),
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

def run_child(spec: dict[str, Any], workdir: Path,
              timeout_s: float) -> tuple[int | None, dict[str, Any] | None, str]:
    """Run one child process, stopping it after timeout_s seconds.

    Return its exit status (None if it was stopped), its result, and the tail of its stderr.
    """
    spec_path = workdir / "spec.json"
    result_path = workdir / "result.json"
    result_path.unlink(missing_ok=True)
    spec_path.write_text(json.dumps(spec))
    command = [sys.executable, str(Path(__file__).resolve()), "child", str(spec_path), str(result_path)]
    try:
        proc = subprocess.run(command, stderr=subprocess.PIPE, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired as e:
        # TimeoutExpired holds the captured stderr as bytes whatever the text setting.
        stderr = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
        sys.stderr.write(stderr)
        sys.stderr.flush()
        return None, None, stderr[-STDERR_TAIL:]
    if proc.stderr:
        sys.stderr.write(proc.stderr)
        sys.stderr.flush()
    result = json.loads(result_path.read_text()) if proc.returncode == 0 and result_path.exists() else None
    return proc.returncode, result, proc.stderr[-STDERR_TAIL:]


def run_check(spec: dict[str, Any], workdir: Path, deadline_s: float, start: float) -> str:
    """Run one check in a child process, stopping it deadline_s seconds after start."""
    remaining = deadline_s - (time.monotonic() - start)
    if remaining <= 0:
        return "timed out"
    code, result, _ = run_child(spec, workdir, remaining)
    if code is None:
        return "timed out"
    return "crashed" if result is None else result["result"]


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
    del info["slurm_array_task_id"]  # each read is its own job
    info["os_cpu_count"] = os.cpu_count() or 0
    info["b2h5py"] = version("b2h5py")
    info["blosc2"] = version("blosc2")
    info["blosc2_nthreads"] = blosc2.nthreads
    return info


def write_results(path: Path, attrs: dict[str, Any], datasets: dict[str, np.ndarray]) -> None:
    """Write the output file, replacing an existing one in a single rename."""
    partial = path.with_name(path.name + ".partial")
    with h5py.File(partial, "w") as out:
        for key, value in attrs.items():
            out.attrs[key] = value
        for key, value in datasets.items():
            out.create_dataset(key, data=value)
    partial.replace(path)


def run_task(task: ReadTask, input_dir: Path, output_dir: Path, time_limit_s: float) -> int:
    start = time.monotonic()
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
    output_path.unlink(missing_ok=True)
    output_path.with_name(output_path.name + ".partial").unlink(missing_ok=True)

    info = run_info_with_reads()
    print(f"Task {task.task}: {task.name}, cache {nbytes} bytes, {nslots} slots", flush=True)

    positions = unit_positions(pattern, shape)
    base = {"file": str(input_path), "series": series, "path": task.path, "rdcc": [nbytes, nslots, CACHE_W0]}
    n_units = pattern.n_units
    first = unit_selections(pattern, shape, positions[0].tolist())
    reads_per_unit = len(first)
    read_times = np.full((n_units, reads_per_unit), np.nan)
    read_cpu_times = np.full((n_units, reads_per_unit), np.nan)
    returncodes = np.full(n_units, NOT_RUN, dtype=np.int64)
    fast_reads = np.zeros(n_units, dtype=np.int64)
    completed = np.zeros(n_units, dtype=bool)
    crashes: list[str] = []
    evicted = True
    rdcc_read: list[Any] = [nbytes, nslots, CACHE_W0]
    threshold = SLOW_UNIT_THRESHOLD_S[modality] if pattern.sequential else -1
    unit_deadline_s = time_limit_s - UNIT_MARGIN_S
    check_deadline_s = time_limit_s - CHECK_MARGIN_S
    checks = {"check_selection": "not run", "check_sha256": "not run", "check_fast_path": "not run"}
    stop_reason = ""

    def save(complete: bool) -> None:
        attrs = {
            "task": task.task, "stage": task.stage, "label": task.label,
            "row": -1 if task.row is None else task.row, "config_id": task.config_id,
            "pattern": task.pattern, "path": task.path, "cache": task.cache,
            "chunks": "contiguous" if chunks is None else ",".join(str(c) for c in chunks),
            "codec": "none" if config is None else config.codec,
            "level": -1 if config is None or config.level is None else config.level,
            "shuffle": "none" if config is None else config.shuffle,
            "data_shape": list(shape),
            "rdcc_nbytes": int(rdcc_read[0]), "rdcc_nslots": int(rdcc_read[1]),
            "rdcc_w0": float(rdcc_read[2]),
            "n_units": n_units, "n_units_run": int(np.sum(returncodes != NOT_RUN)),
            "complete": complete, "stop_reason": stop_reason,
            "truncated": stop_reason in ("slow units", "time budget"),
            "slow_unit_threshold_s": threshold, "time_limit_s": time_limit_s,
            "n_crashed": len(crashes), "crashes": "\n\n".join(crashes),
            "reads": int(np.sum(~np.isnan(read_times))), "fast_reads": int(fast_reads.sum()),
            "page_cache_evicted": evicted,
            **checks,
            "start_utc": info["start_utc"],
            "end_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            **{k: v for k, v in info.items() if k != "start_utc"},
        }
        write_results(output_path, attrs, {
            "read_times": read_times,
            "read_cpu_times": read_cpu_times,
            "unit_total_times": read_times.sum(axis=1),
            "unit_total_cpu_times": read_cpu_times.sum(axis=1),
            "unit_returncode": returncodes,
            "unit_fast_reads": fast_reads,
            "unit_positions": positions,
            "read_lengths": np.array(
                [1 if isinstance(s[0], int) else s[0][1] - s[0][0] for s in first], dtype=np.int64),
        })

    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        slow = 0
        longest = 0.0
        for unit in range(n_units):
            elapsed = time.monotonic() - start
            if elapsed + longest > unit_deadline_s:
                stop_reason = "time budget"
                print(f"  Stopping before unit {unit}: {elapsed:.0f} s elapsed, longest unit "
                      f"{longest:.0f} s, units stop at {unit_deadline_s:.0f} s", flush=True)
                break
            selections = unit_selections(pattern, shape, positions[unit].tolist())
            unit_start = time.monotonic()
            code, result, stderr_tail = run_child(
                {**base, "mode": "unit", "selections": selections}, workdir, unit_deadline_s - elapsed)
            longest = max(longest, time.monotonic() - unit_start)
            if code is None:
                returncodes[unit] = STOPPED
                stop_reason = "time budget"
                print(f"  Unit {unit} stopped at {unit_deadline_s:.0f} s", flush=True)
                break
            returncodes[unit] = code
            if result is None:
                crashes.append(f"unit {unit}: exit status {code}: {stderr_tail}")
                print(f"  Unit {unit} failed with exit status {code}", flush=True)
                if len(crashes) >= MAX_CRASHES:
                    stop_reason = "failed units"
                    print(f"  Stopping after {len(crashes)} failed units", flush=True)
                    break
                continue
            if list(result["shape"]) != list(shape):
                raise SystemExit(f"{input_path} holds shape {result['shape']}, expected {list(shape)}")
            file_chunks = None if result["chunks"] is None else tuple(result["chunks"])
            if file_chunks != chunks:
                raise SystemExit(f"{input_path} holds chunks {file_chunks}, expected {chunks}")
            read_times[unit] = result["wall"]
            read_cpu_times[unit] = result["cpu"]
            fast_reads[unit] = result["fast_reads"]
            completed[unit] = True
            evicted &= result["evicted"]
            rdcc_read = result["rdcc"]
            total = float(np.sum(result["wall"]))
            print(f"  Unit {unit}: {total:.3f} s wall, {float(np.sum(result['cpu'])):.3f} s CPU", flush=True)
            save(complete=False)
            if pattern.sequential and total > threshold:
                slow += 1
                if slow >= MAX_SLOW_UNITS:
                    stop_reason = "slow units"
                    print(f"  Stopping after {slow} units over {threshold} s", flush=True)
                    break

        reference = input_dir / f"{task.label}_{UNCHUNKED}.nwb"
        if config is None:
            checks["check_selection"] = "not run"
        elif not reference.exists():
            checks["check_selection"] = "missing reference"
        else:
            checks["check_selection"] = run_check(
                {**base, "mode": "check_selection", "reference": str(reference),
                 "selections": [first[0], first[-1]]}, workdir, check_deadline_s, start)

        if task.path == "b2h5py":
            stats_path = input_dir / f"stats_{task.label}_{task.config_id}.tsv"
            sha256 = recorded_sha256(stats_path) if stats_path.exists() else ""
            if not sha256:
                checks["check_sha256"] = "missing reference"
            else:
                checks["check_sha256"] = run_check(
                    {**base, "mode": "check_sha256", "sha256": sha256}, workdir, check_deadline_s, start)

    if completed.any():
        expected = reads_per_unit if task.path == "b2h5py" else 0
        checks["check_fast_path"] = "match" if np.all(fast_reads[completed] == expected) else "mismatch"
    save(complete=True)

    print(f"Checks: selection {checks['check_selection']}, sha256 {checks['check_sha256']}, "
          f"fast path {checks['check_fast_path']}; {len(crashes)} failed units; results in {output_path}",
          flush=True)
    failed = crashes or any(result in FAILED_CHECKS for result in checks.values())
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
    parser.add_argument("--time-limit-s", type=float, default=TIME_LIMIT_S,
                        help=f"the job's time limit in seconds (default {TIME_LIMIT_S})")
    args = parser.parse_args()
    sys.exit(run_task(find_task(args.schedule, args.task), args.input_dir, args.output_dir, args.time_limit_s))


if __name__ == "__main__":
    main()
