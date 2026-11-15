"""
Submit the writes of one modality (ecephys or ophys) in scripts/exp12_write_schedule.tsv as SLURM
jobs, keeping the number of jobs under a cap, and report their status.

The ecephys and ophys writes are separate runs: a submission takes the tasks of one modality only,
so each modality can start once its raw data exist and be checked and rerun on its own.

Each task is its own job, scripts/exp12_write.sh <task>, named exp12_write_<label>_<config_id>,
with logs write_logs/<label>_<config_id>--<job ID>-{out,err}.log. The cap counts every job of the
user in the queue, pending and running, including this script's own job and jobs unrelated to the
writes. While tasks remain, the script submits as many as fit under the cap, in schedule order,
then waits and checks the queue again. It exits once every task of the modality is submitted. The
ecephys and ophys submissions can run at the same time: each counts the queue and submits while
holding a lock on write_logs/exp12_write_submit.lock, so together they stay under the cap.

Each submission is appended to write_logs/exp12_write_submitted.tsv (task, job_id, submitted_utc).
A task in that file is not submitted again, so a rerun after the script stops continues where it
left off. --tasks submits the given tasks whether or not they were submitted before, for smoke tests
and resubmitting failed writes; the status report uses the latest job of each task.

Usage:
    python scripts/exp12_submit_writes.py submit {ecephys,ophys} [--tasks 3,17,250] [--max-jobs 95]
        [--dry-run]
    python scripts/exp12_submit_writes.py status [--modality {ecephys,ophys}]

Run the full submission of a modality as a job, so it can wait on the queue for as long as the
writes take:
    sbatch scripts/exp12_submit_writes.sh ecephys
"""

import argparse
import csv
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
import fcntl
import getpass
import os
from pathlib import Path
import subprocess
import time

from exp12_write_schedule import MODALITIES, Task, load_schedule

REPO = Path(__file__).resolve().parent.parent
WRITE_SCRIPT = REPO / "scripts" / "exp12_write.sh"
LOG_DIR = REPO / "write_logs"
OUTPUT_DIR = REPO / "write_output"
SUBMITTED_PATH = LOG_DIR / "exp12_write_submitted.tsv"
SUBMITTED_COLUMNS = ("task", "job_id", "submitted_utc")
LOCK_PATH = LOG_DIR / "exp12_write_submit.lock"

# This project allows at most 100 jobs per user in the queue, pending and running combined. The
# default leaves room for jobs submitted by hand while this script runs.
DEFAULT_MAX_JOBS = 95
DEFAULT_POLL_S = 300
SACCT_BATCH = 500


def queued_job_count(user: str) -> int:
    """Jobs of the user in the queue, counting each array task as a job."""
    proc = subprocess.run(
        ["squeue", "--noheader", "--array", "--user", user, "--format=%i"],
        capture_output=True, text=True, check=True,
    )
    return sum(1 for line in proc.stdout.splitlines() if line.strip())


def sbatch_env() -> dict[str, str]:
    """This process's environment without SLURM and SBATCH variables.

    When this script runs inside a job, its SLURM_* and SBATCH_* variables describe that job, and
    sbatch reads some of them as options for the job it submits.
    """
    return {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "SBATCH_"))}


def sbatch_command(task: Task) -> list[str]:
    name = f"{task.label}_{task.config_id}"
    return [
        "sbatch", "--parsable",
        f"--job-name=exp12_write_{name}",
        f"--output={LOG_DIR}/{name}--%j-out.log",
        f"--error={LOG_DIR}/{name}--%j-err.log",
        str(WRITE_SCRIPT), str(task.task),
    ]


@contextmanager
def submit_lock() -> Iterator[None]:
    """Hold the lock that submissions take while they count the queue and submit."""
    with open(LOCK_PATH, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def load_submitted() -> dict[int, str]:
    """The latest job ID of each submitted task."""
    if not SUBMITTED_PATH.exists():
        return {}
    with open(SUBMITTED_PATH, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return {int(row["task"]): row["job_id"] for row in reader}


def record_submission(task: Task, job_id: str) -> None:
    new_file = not SUBMITTED_PATH.exists()
    with open(SUBMITTED_PATH, "a", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        if new_file:
            writer.writerow(SUBMITTED_COLUMNS)
        writer.writerow([task.task, job_id, datetime.now(UTC).isoformat(timespec="seconds")])


def parse_tasks(text: str) -> list[int]:
    return [int(t) for t in text.split(",") if t.strip()]


def submit(args: argparse.Namespace) -> None:
    schedule = {t.task: t for t in load_schedule() if t.modality == args.modality}
    if args.tasks:
        unknown = [t for t in args.tasks if t not in schedule]
        if unknown:
            raise SystemExit(f"Tasks not in the {args.modality} schedule: {unknown}")
        remaining = [schedule[t] for t in args.tasks]
    else:
        submitted = load_submitted()
        remaining = [t for t in schedule.values() if t.task not in submitted]
    print(f"{len(remaining)} {args.modality} tasks to submit, at most {args.max_jobs} jobs in the queue")

    if args.dry_run:
        for task in remaining:
            print(" ".join(sbatch_command(task)))
        return

    LOG_DIR.mkdir(exist_ok=True)
    user = getpass.getuser()
    env = sbatch_env()
    while remaining:
        with submit_lock():
            try:
                free = args.max_jobs - queued_job_count(user)
            except subprocess.CalledProcessError as e:
                print(f"squeue failed, retrying later: {e.stderr.strip()}", flush=True)
                free = 0
            while free > 0 and remaining:
                task = remaining[0]
                proc = subprocess.run(sbatch_command(task), capture_output=True, text=True, env=env)
                if proc.returncode != 0:
                    print(f"sbatch failed for task {task.task}, retrying later: {proc.stderr.strip()}",
                          flush=True)
                    break
                job_id = proc.stdout.strip().split(";")[0]
                record_submission(task, job_id)
                print(f"Submitted task {task.task} ({task.label} {task.config_id}) as job {job_id}",
                      flush=True)
                remaining.pop(0)
                free -= 1
        if remaining:
            time.sleep(args.poll_s)
    print(f"All {args.modality} tasks submitted")


def sacct_states(job_ids: list[str]) -> dict[str, tuple[str, str]]:
    """State and elapsed time of each job, by job ID."""
    states = {}
    for i in range(0, len(job_ids), SACCT_BATCH):
        proc = subprocess.run(
            ["sacct", "--allocations", "--noheader", "--parsable2", "--format=JobID,State,Elapsed",
             "--jobs", ",".join(job_ids[i:i + SACCT_BATCH])],
            capture_output=True, text=True, check=True,
        )
        for line in proc.stdout.splitlines():
            job_id, state, elapsed = line.split("|")
            # States such as "CANCELLED by 123" carry a suffix
            states[job_id] = (state.split()[0], elapsed)
    return states


def status(args: argparse.Namespace) -> None:
    schedule = [t for t in load_schedule() if args.modality in (None, t.modality)]
    submitted = load_submitted()
    states = sacct_states(sorted({submitted[t.task] for t in schedule if t.task in submitted}))
    counts: Counter[str] = Counter()
    problems = []
    for task in schedule:
        job_id = submitted.get(task.task)
        state, elapsed = states.get(job_id, ("UNKNOWN", "-")) if job_id else ("NOT_SUBMITTED", "-")
        counts[state] += 1
        stats = OUTPUT_DIR / f"stats_{task.label}_{task.config_id}.tsv"
        if state == "COMPLETED" and not stats.exists():
            problems.append((task, job_id, "COMPLETED_NO_STATS", elapsed))
        elif state not in ("COMPLETED", "RUNNING", "PENDING", "NOT_SUBMITTED"):
            problems.append((task, job_id, state, elapsed))

    print(f"{len(schedule)} tasks: " + ", ".join(f"{n} {state}" for state, n in sorted(counts.items())))
    if problems:
        print()
        print(f"{'task':>5}  {'label':<9} {'config_id':<22} {'job_id':<10} {'state':<20} elapsed")
        for task, job_id, state, elapsed in problems:
            print(f"{task.task:>5}  {task.label:<9} {task.config_id:<22} {job_id:<10} {state:<20} "
                  f"{elapsed}")
        print()
        print("Logs: write_logs/<label>_<config_id>--<job_id>-{out,err}.log")
        for modality in MODALITIES:
            tasks = [task.task for task, *_ in problems if task.modality == modality]
            if tasks:
                print(f"Resubmit with: python scripts/exp12_submit_writes.py submit {modality} --tasks "
                      + ",".join(str(t) for t in tasks))


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit the exp12 writes and report their status.")
    commands = parser.add_subparsers(dest="command", required=True)
    p_submit = commands.add_parser("submit", help="submit tasks, keeping the queue under a cap")
    p_submit.add_argument("modality", choices=MODALITIES, help="submit the tasks of this modality")
    p_submit.add_argument("--tasks", type=parse_tasks,
                          help="comma-separated task numbers to submit, whether or not submitted before")
    p_submit.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS,
                          help=f"most jobs of the user in the queue (default {DEFAULT_MAX_JOBS})")
    p_submit.add_argument("--poll-s", type=int, default=DEFAULT_POLL_S,
                          help=f"seconds between queue checks (default {DEFAULT_POLL_S})")
    p_submit.add_argument("--dry-run", action="store_true", help="print the sbatch commands only")
    p_submit.set_defaults(func=submit)
    p_status = commands.add_parser("status", help="report the state of every task")
    p_status.add_argument("--modality", choices=MODALITIES, help="report this modality only")
    p_status.set_defaults(func=status)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
