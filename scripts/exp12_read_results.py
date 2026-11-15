"""
Summarize the read results (see exp12_read.py) in one tab-separated table.

    python scripts/exp12_read_results.py <read_output_dir> <summary.tsv>

Each result file becomes one row: the task and configuration from its attributes, the outcome
(units run, whether the task finished, early stop and its reason, crashed units, checks, reads
served by b2h5py's fast path), and the
median, minimum, and maximum over its completed units of the unit wall time and CPU time
(unit_total_times and unit_total_cpu_times: a whole pass through the window for e1, e2, o1, and
o2, and one sample for e3, e4, o3, and o4). Files that cannot be read are listed on stderr and
left out.
"""

import argparse
import csv
from pathlib import Path
import sys

import h5py
import numpy as np

ATTR_COLUMNS = (
    "task", "stage", "label", "config_id", "chunks", "codec", "level", "shuffle", "pattern", "path", "cache",
    "rdcc_nbytes", "rdcc_nslots", "n_units", "n_units_run", "complete", "stop_reason", "truncated", "n_crashed",
    "reads", "fast_reads", "check_selection", "check_sha256", "check_fast_path", "page_cache_evicted", "host",
    "start_utc", "end_utc",
)
STAT_COLUMNS = tuple(
    f"{name}_{stat}" for name in ("wall_s", "cpu_s") for stat in ("median", "min", "max")
) + ("units_completed",)


def summarize(path: Path) -> dict[str, object]:
    with h5py.File(path, "r") as f:
        row: dict[str, object] = {}
        for key in ATTR_COLUMNS:
            value = f.attrs.get(key, "")
            row[key] = value.item() if isinstance(value, np.generic) else value
        wall = f["unit_total_times"][()]
        cpu = f["unit_total_cpu_times"][()]
    done = ~np.isnan(wall)
    row["units_completed"] = int(done.sum())
    for name, values in (("wall_s", wall[done]), ("cpu_s", cpu[done])):
        for stat, fn in (("median", np.median), ("min", np.min), ("max", np.max)):
            row[f"{name}_{stat}"] = f"{fn(values):.6f}" if values.size else ""
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize the exp12 read results.")
    parser.add_argument("read_output_dir", type=Path)
    parser.add_argument("summary", type=Path)
    args = parser.parse_args()

    rows = []
    for path in sorted(args.read_output_dir.glob("*.h5")):
        try:
            rows.append(summarize(path))
        except (OSError, KeyError) as e:
            print(f"Cannot read {path}: {e}", file=sys.stderr)
    rows.sort(key=lambda r: (str(r["label"]), str(r["config_id"]), str(r["pattern"]), str(r["path"]),
                             str(r["cache"]), int(r["stage"] or 0)))
    with open(args.summary, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ATTR_COLUMNS + STAT_COLUMNS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {args.summary}")


if __name__ == "__main__":
    main()
