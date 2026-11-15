"""
The order in which the reads run.

Every read, one test file read with one pattern, read path, and chunk cache setting (see
exp12_read_patterns.py), is a task in scripts/exp12_read_schedule.tsv, with these columns:

    task       task number, the order in which exp12_submit_reads.py submits them
    stage      read stage, 1 or 2
    label      dataset label, for example ecephys1
    row        row of the dataset's config table (see exp12_configs.py), or - for unchunked
    config_id  configuration, or unchunked
    pattern    read pattern, for example e1
    path       read path, h5py or b2h5py
    cache      chunk cache setting, 1mib, 8mib, or fitted

Stage 1 reads every stage 1 configuration and the unchunked file of each dataset. Stage 2 reads
the stage 2 configurations and reads again the stage 1 configurations on the shapes outside the
codec sweep (no compression, gzip 4, and the library defaults), so every comparison between
codec settings on one shape comes from the same read run. Stage 2 has tasks only once
exp12_configs.STAGE2_SETTINGS is filled in.

The tasks of each stage are in a random order with a fixed seed, and the stage 2 tasks follow the
stage 1 tasks, so adding stage 2 leaves every stage 1 task number unchanged. The reads running
at any one time are then a mix of datasets, configurations, patterns, read paths, and cache
settings, and the variants of one file run at unrelated times, so the Lustre server cache one
leaves warm does not favor another.

Write the schedule with:
    python scripts/exp12_read_schedule.py
"""

import csv
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from exp12_configs import CODEC_SWEEP_CHUNKS, CONFIG_DIR, DATASETS, Config, config_table_path, load_configs
from exp12_read_patterns import caches_for, modality_patterns

SCHEDULE_PATH = CONFIG_DIR / "exp12_read_schedule.tsv"
COLUMNS = ("task", "stage", "label", "row", "config_id", "pattern", "path", "cache")
UNCHUNKED = "unchunked"
SEED = 1212


@dataclass(frozen=True)
class ReadTask:
    task: int
    stage: int
    label: str
    row: int | None
    config_id: str
    pattern: str
    path: str
    cache: str

    @property
    def modality(self) -> str:
        """ecephys or ophys, the dataset label without its number."""
        return self.label.rstrip("0123456789")

    @property
    def name(self) -> str:
        """Name of the task's outputs and logs."""
        return f"{self.label}_{self.config_id}_{self.pattern}_{self.path}_{self.cache}_s{self.stage}"


def chunk_label(config_id: str) -> str:
    return config_id.split("-")[0]


def file_reads(label: str, row: int | None, config: Config | None) -> list[tuple]:
    """(label, row, config_id, pattern, path, cache) of every read of one test file."""
    modality = label.rstrip("0123456789")
    chunks = None if config is None else config.chunks
    paths = ["h5py"]
    if config is not None and config.codec.startswith("blosc2"):
        paths.append("b2h5py")
    config_id = UNCHUNKED if config is None else config.config_id
    reads = []
    for pattern in modality_patterns(modality):
        for path in paths:
            for cache in caches_for(pattern, path, chunks):
                reads.append((label, row, config_id, pattern.name, path, cache))
    return reads


def build_schedule() -> list[ReadTask]:
    reads: dict[int, list[tuple]] = {1: [], 2: []}
    has_stage2 = False
    for label, spec in DATASETS.items():
        sweep = CODEC_SWEEP_CHUNKS[spec["prefix"]]
        reads[1] += file_reads(label, None, None)
        for row, config in enumerate(load_configs(config_table_path(label)), start=1):
            has_stage2 |= config.stage == 2
            if config.stage == 1:
                reads[1] += file_reads(label, row, config)
            if config.stage == 2 or chunk_label(config.config_id) not in sweep:
                reads[2] += file_reads(label, row, config)
    if not has_stage2:
        reads[2] = []

    tasks: list[ReadTask] = []
    for stage in (1, 2):
        order = np.random.default_rng([SEED, stage]).permutation(len(reads[stage]))
        tasks += [ReadTask(len(tasks) + n, stage, *reads[stage][i]) for n, i in enumerate(order, start=1)]
    return tasks


def write_schedule(path: Path, tasks: list[ReadTask]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(COLUMNS)
        for t in tasks:
            writer.writerow([t.task, t.stage, t.label, "-" if t.row is None else t.row, t.config_id,
                             t.pattern, t.path, t.cache])


def load_schedule(path: Path = SCHEDULE_PATH) -> list[ReadTask]:
    with open(path, newline="") as f:
        rows = list(csv.reader(f, delimiter="\t"))
    if tuple(rows[0]) != COLUMNS:
        raise ValueError(f"{path}: expected header {COLUMNS}, got {tuple(rows[0])}")
    return [
        ReadTask(int(task), int(stage), label, None if row == "-" else int(row), config_id, pattern, path_,
                 cache)
        for task, stage, label, row, config_id, pattern, path_, cache in rows[1:]
    ]


def main() -> None:
    tasks = build_schedule()
    write_schedule(SCHEDULE_PATH, tasks)
    counts = {stage: sum(t.stage == stage for t in tasks) for stage in (1, 2)}
    print(f"Wrote {len(tasks)} tasks ({counts[1]} stage 1, {counts[2]} stage 2) to {SCHEDULE_PATH}")


if __name__ == "__main__":
    main()
