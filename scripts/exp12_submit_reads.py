"""
Submit the reads of one modality and stage in scripts/exp12_read_schedule.tsv as SLURM jobs,
keeping the number of jobs under a cap, and report their status.

Each task is its own job, scripts/exp12_read.sh <task>, named exp12_read_<task name>, with logs
read_logs/<task name>--<job ID>-{out,err}.log (see exp12_read_schedule.ReadTask.name). The cap
counts every job of the user in the queue, as in exp12_submit_writes.py, and submissions take the
same lock as the write submissions, so reads and writes together stay under it. While tasks
remain, the script submits as many as fit under the cap, in schedule order, then waits and checks
the queue again. It exits once every task of the modality and stage is submitted. --max-own-jobs
also caps the jobs of the modality's reads in the queue, so the submissions of both modalities
running at the same time each get a share of the cap.

Each submission is appended to read_logs/exp12_read_submitted.tsv (name, job_id, submitted_utc).
A read in that file is not submitted again, so a rerun after the script stops continues where it
left off. The file identifies reads by task name, so it stays valid when the schedule is
regenerated with new configurations and its task numbers change. --tasks submits the given tasks
whether or not they were submitted before, for smoke tests and resubmitting failed reads; the
status report uses the latest job of each read.

A read job fails (exit status 1) when a unit's child process crashed or a correctness check did
not match or ran out of time, after it writes its results; the status report lists these with the
other failures.

Usage:
    python scripts/exp12_submit_reads.py submit {ecephys,ophys} [--stage 1] [--tasks 3,17,250]
        [--max-jobs 95] [--max-own-jobs 60] [--dry-run]
    python scripts/exp12_submit_reads.py status [--modality {ecephys,ophys}] [--stage 1]

Run the full submission of a modality as a job, so it can wait on the queue for as long as the
reads take:
    sbatch scripts/exp12_submit_reads.sh ecephys
"""

import argparse
import csv
from collections import Counter
from datetime import UTC, datetime
import getpass
from pathlib import Path
import subprocess
import time

from exp12_read_schedule import ReadTask, load_schedule
from exp12_submit_writes import (
    DEFAULT_MAX_JOBS, DEFAULT_POLL_S, LOG_DIR as WRITE_LOG_DIR, parse_tasks, queued_job_count, sacct_states,
    sbatch_env, submit_lock,
)
from exp12_write_schedule import MODALITIES

REPO = Path(__file__).resolve().parents[1]
READ_SCRIPT = REPO / "scripts" / "exp12_read.sh"
LOG_DIR = REPO / "read_logs"
OUTPUT_DIR = REPO / "read_output"
SUBMITTED_PATH = LOG_DIR / "exp12_read_submitted.tsv"
SUBMITTED_COLUMNS = ("name", "job_id", "submitted_utc")


def sbatch_command(task: ReadTask) -> list[str]:
    return [
        "sbatch", "--parsable",
        f"--job-name=exp12_read_{task.name}",
        f"--output={LOG_DIR}/{task.name}--%j-out.log",
        f"--error={LOG_DIR}/{task.name}--%j-err.log",
        str(READ_SCRIPT), str(task.task),
    ]


def queued_read_count(user: str, modality: str) -> int:
    """Read jobs of the modality in the user's queue, by job name."""
    proc = subprocess.run(
        ["squeue", "--noheader", "--array", "--user", user, "--format=%j"],
        capture_output=True, text=True, check=True,
    )
    return sum(1 for name in proc.stdout.split() if name.startswith(f"exp12_read_{modality}"))


def load_submitted() -> dict[str, str]:
    """The latest job ID of each submitted read, by task name."""
    if not SUBMITTED_PATH.exists():
        return {}
    with open(SUBMITTED_PATH, newline="") as f:
        return {row["name"]: row["job_id"] for row in csv.DictReader(f, delimiter="\t")}


def record_submission(task: ReadTask, job_id: str) -> None:
    new_file = not SUBMITTED_PATH.exists()
    with open(SUBMITTED_PATH, "a", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        if new_file:
            writer.writerow(SUBMITTED_COLUMNS)
        writer.writerow([task.name, job_id, datetime.now(UTC).isoformat(timespec="seconds")])


def submit(args: argparse.Namespace) -> None:
    schedule = {t.task: t for t in load_schedule() if t.modality == args.modality and t.stage == args.stage}
    if args.tasks:
        unknown = [t for t in args.tasks if t not in schedule]
        if unknown:
            raise SystemExit(f"Tasks not in the stage {args.stage} {args.modality} schedule: {unknown}")
        remaining = [schedule[t] for t in args.tasks]
    else:
        submitted = load_submitted()
        remaining = [t for t in schedule.values() if t.name not in submitted]
    print(f"{len(remaining)} stage {args.stage} {args.modality} read tasks to submit, "
          f"at most {args.max_jobs} jobs in the queue"
          + ("" if args.max_own_jobs is None else f" and {args.max_own_jobs} {args.modality} reads"))

    if args.dry_run:
        for task in remaining:
            print(" ".join(sbatch_command(task)))
        return

    LOG_DIR.mkdir(exist_ok=True)
    WRITE_LOG_DIR.mkdir(exist_ok=True)  # holds the lock shared with the write submissions
    user = getpass.getuser()
    env = sbatch_env()
    while remaining:
        with submit_lock():
            try:
                free = args.max_jobs - queued_job_count(user)
                if args.max_own_jobs is not None:
                    free = min(free, args.max_own_jobs - queued_read_count(user, args.modality))
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
                print(f"Submitted task {task.task} ({task.name}) as job {job_id}", flush=True)
                remaining.pop(0)
                free -= 1
        if remaining:
            time.sleep(args.poll_s)
    print(f"All stage {args.stage} {args.modality} read tasks submitted")


def status(args: argparse.Namespace) -> None:
    schedule = [t for t in load_schedule()
                if args.modality in (None, t.modality) and args.stage in (None, t.stage)]
    submitted = load_submitted()
    states = sacct_states(sorted({submitted[t.name] for t in schedule if t.name in submitted}))
    counts: Counter[str] = Counter()
    problems = []
    for task in schedule:
        job_id = submitted.get(task.name)
        state, elapsed = states.get(job_id, ("UNKNOWN", "-")) if job_id else ("NOT_SUBMITTED", "-")
        counts[state] += 1
        if state == "COMPLETED" and not (OUTPUT_DIR / f"{task.name}.h5").exists():
            problems.append((task, job_id, "COMPLETED_NO_OUTPUT", elapsed))
        elif state not in ("COMPLETED", "RUNNING", "PENDING", "NOT_SUBMITTED"):
            problems.append((task, job_id, state, elapsed))

    print(f"{len(schedule)} tasks: " + ", ".join(f"{n} {state}" for state, n in sorted(counts.items())))
    if problems:
        print()
        print(f"{'task':>5}  {'name':<60} {'job_id':<10} {'state':<20} elapsed")
        for task, job_id, state, elapsed in problems:
            print(f"{task.task:>5}  {task.name:<60} {job_id:<10} {state:<20} {elapsed}")
        print()
        print("Logs: read_logs/<name>--<job_id>-{out,err}.log. A FAILED read with an output file had a "
              "crashed unit or a check that did not match or ran out of time; its output's attributes say "
              "which. A read killed before it finished keeps the units it completed, with complete false.")
        for modality in MODALITIES:
            for stage in (1, 2):
                tasks = [task.task for task, *_ in problems if task.modality == modality and task.stage == stage]
                if tasks:
                    print(f"Resubmit with: python scripts/exp12_submit_reads.py submit {modality} --stage {stage} "
                          "--tasks " + ",".join(str(t) for t in tasks))


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit the exp12 reads and report their status.")
    commands = parser.add_subparsers(dest="command", required=True)
    p_submit = commands.add_parser("submit", help="submit tasks, keeping the queue under a cap")
    p_submit.add_argument("modality", choices=MODALITIES, help="submit the tasks of this modality")
    p_submit.add_argument("--stage", type=int, choices=(1, 2), default=1, help="read stage (default 1)")
    p_submit.add_argument("--tasks", type=parse_tasks,
                          help="comma-separated task numbers to submit, whether or not submitted before")
    p_submit.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS,
                          help=f"most jobs of the user in the queue (default {DEFAULT_MAX_JOBS})")
    p_submit.add_argument("--max-own-jobs", type=int,
                          help="most read jobs of the modality in the queue (default no separate cap)")
    p_submit.add_argument("--poll-s", type=int, default=DEFAULT_POLL_S,
                          help=f"seconds between queue checks (default {DEFAULT_POLL_S})")
    p_submit.add_argument("--dry-run", action="store_true", help="print the sbatch commands only")
    p_submit.set_defaults(func=submit)
    p_status = commands.add_parser("status", help="report the state of every task")
    p_status.add_argument("--modality", choices=MODALITIES, help="report this modality only")
    p_status.add_argument("--stage", type=int, choices=(1, 2), help="report this read stage only")
    p_status.set_defaults(func=status)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
