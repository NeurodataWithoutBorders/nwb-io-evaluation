"""
The order in which the writes run.

Every write, one configuration or the unchunked baseline of one dataset, is a task in
scripts/exp12_write_schedule.tsv, with these columns:

    task       task number, 1 to the number of writes, the order in which exp12_submit_writes.py
               submits them
    label      dataset label, for example ecephys1
    row        row of the dataset's config table (see exp12_configs.py), or - for unchunked
    config_id  configuration, or unchunked

The tasks are in a random order with a fixed seed. The ecephys and ophys writes are submitted as
separate runs (see exp12_submit_writes.py), each taking its modality's tasks in schedule order,
which is a random order within the modality. The writes running at any one time are then a mix of
datasets, chunk shapes, and codecs, so the load on the file system while a write runs is not tied
to its configuration.

Write the schedule with:
    python scripts/exp12_write_schedule.py
"""

import csv
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from exp12_configs import CONFIG_DIR, DATASETS, config_table_path, load_configs

SCHEDULE_PATH = CONFIG_DIR / "exp12_write_schedule.tsv"
COLUMNS = ("task", "label", "row", "config_id")
UNCHUNKED = "unchunked"
MODALITIES = ("ecephys", "ophys")
SEED = 12


@dataclass(frozen=True)
class Task:
    task: int
    label: str
    row: int | None
    config_id: str

    @property
    def modality(self) -> str:
        """ecephys or ophys, the dataset label without its number."""
        return self.label.rstrip("0123456789")


def build_schedule() -> list[Task]:
    writes = []
    for label in DATASETS:
        writes.append((label, None, UNCHUNKED))
        for row, config in enumerate(load_configs(config_table_path(label)), start=1):
            writes.append((label, row, config.config_id))
    order = np.random.default_rng(SEED).permutation(len(writes))
    return [Task(task, *writes[i]) for task, i in enumerate(order, start=1)]


def write_schedule(path: Path, tasks: list[Task]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(COLUMNS)
        for t in tasks:
            writer.writerow([t.task, t.label, "-" if t.row is None else t.row, t.config_id])


def load_schedule(path: Path = SCHEDULE_PATH) -> list[Task]:
    with open(path, newline="") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    if tuple(rows[0]) != COLUMNS:
        raise ValueError(f"{path}: expected header {COLUMNS}, got {tuple(rows[0])}")
    return [
        Task(int(task), label, None if row == "-" else int(row), config_id)
        for task, label, row, config_id in rows[1:]
    ]


def main() -> None:
    tasks = build_schedule()
    write_schedule(SCHEDULE_PATH, tasks)
    print(f"Wrote {len(tasks)} tasks to {SCHEDULE_PATH}")


if __name__ == "__main__":
    main()
