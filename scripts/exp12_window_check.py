"""
Check that the centered window (exp12_window.py) is representative of the whole recording for
compression ratio.

For one dataset, the check takes N_SEGMENTS segments of about one minute, spaced evenly across the
full recording in the raw binary or TIFF files, and the centered window in blocks of the same
length. It compresses each segment and block with each codec setting in CODEC_SETTINGS on the
dataset's codec sweep chunk shape for whole time steps (EC384M or OCframeM, see exp12_configs.py),
writing it to an in-memory HDF5 file with h5dataio_kwargs, so the filter stack is the same as in
the test files, and records the dataset's storage size. Segment and block lengths are whole
numbers of the chunk's time extent, and the window blocks start at multiples of it from the
window start, so the window's compression ratio (the sum of its blocks' bytes over the sum of
their storage) is the one a test file of the window has.

    python scripts/exp12_window_check.py run <label> <output_tsv>

writes one row per segment or block and codec setting:

    label          dataset label
    kind           segment or window
    index          segment number (0 to N_SEGMENTS - 1), or block number within the window
    start, stop    samples or frames of the full recording
    config_id      chunk shape and codec setting, as in the config tables
    nbytes         uncompressed bytes
    storage_bytes  bytes the compressed dataset occupies
    ratio          nbytes / storage_bytes

    python scripts/exp12_window_check.py summary <output_tsv> [<output_tsv> ...]

prints, per dataset and codec setting, the window's ratio and the range of the segment ratios,
whether the window's ratio falls inside that range, and whether each segment ranks the codec
settings by ratio in the same order as the window.
"""

import argparse
import csv
from pathlib import Path
import uuid

import h5py
import hdf5plugin  # noqa: F401 - enable HDF5 compression filters
import numpy as np

from exp12_configs import CODEC_SWEEP_CHUNKS, DATASETS, Config, h5dataio_kwargs, make_config
from exp12_tiff import TiffStack
from exp12_window import ECEPHYS_WINDOW_SAMPLES, OPHYS_WINDOW_FRAMES, centered_window

REPO = Path(__file__).resolve().parent.parent
RAW_PATHS = {
    "ecephys1": REPO / "raw_data" / "ecephys1.bin",
    "ecephys2": REPO / "raw_data" / "ecephys2_00ap.bin",
    "ecephys3": REPO / "raw_data" / "ecephys3.bin",
    "ophys1": REPO / "raw_data" / "ophys1",
    "ophys2": REPO / "raw_data" / "ophys2",
    "ophys3": REPO / "raw_data" / "ophys3",
}
# Sampling rates from the dataset notes in the README, used only to size segments of about a minute.
RATES_HZ = {
    "ecephys1": 30000.0, "ecephys2": 30000.0, "ecephys3": 30001.065,
    "ophys1": 15.0, "ophys2": 30.075, "ophys3": 30.075,
}
SEGMENT_SECONDS = 60
N_SEGMENTS = 10
# The status quo (gzip 4), gzip with shuffle, and one setting from each of the other codec families
# that stage 1 compares.
CODEC_SETTINGS = [
    ("gzip", 4, "none"),
    ("gzip", 4, "shuffle"),
    ("zstd", 3, "shuffle"),
    ("blosc2-lz4", 1, "bitshuffle"),
    ("blosc2-zstd", 2, "bitshuffle"),
]
COLUMNS = ("label", "kind", "index", "start", "stop", "config_id", "nbytes", "storage_bytes", "ratio")


class EcephysSource:
    """Samples of a raw binary file of 384 channel int16 data, as (samples, channels)."""

    def __init__(self, path: Path, full_shape: tuple[int, ...]) -> None:
        self.data = np.memmap(path, dtype=np.int16, mode="r")
        if self.data.size != int(np.prod(full_shape)):
            raise ValueError(f"{path} has {self.data.size} values, but the recording has shape {full_shape}")
        self.data = self.data.reshape(full_shape)
        self.n = full_shape[0]

    def read(self, start: int, stop: int) -> np.ndarray:
        return np.array(self.data[start:stop])


class OphysSource:
    """Frames of a TIFF stack, in NWB orientation."""

    def __init__(self, path: Path, full_shape: tuple[int, ...]) -> None:
        self.stack = TiffStack(path)
        self.stack.check_shape(full_shape)
        self.n = self.stack.n_frames

    def read(self, start: int, stop: int) -> np.ndarray:
        return self.stack.read(start, stop)


def storage_bytes(data: np.ndarray, config: Config) -> int:
    """Bytes data occupies in an HDF5 dataset written with config, in an in-memory file."""
    # allow_plugin_filters is an H5DataIO option; h5py takes the plugin filter IDs directly
    kwargs = {k: v for k, v in h5dataio_kwargs(config).items() if k != "allow_plugin_filters"}
    with h5py.File(f"{uuid.uuid4().hex}.h5", "w", driver="core", backing_store=False) as f:
        dset = f.create_dataset("data", data=data, **kwargs)
        return dset.id.get_storage_size()


def blocks(label: str, n_total: int, extent: int) -> list[tuple[str, int, int, int]]:
    """(kind, index, start, stop) of every segment and window block of a recording."""
    length = max(1, round(SEGMENT_SECONDS * RATES_HZ[label] / extent)) * extent
    starts = np.linspace(0, n_total - length, N_SEGMENTS).astype(int)
    out = [("segment", i, int(s), int(s) + length) for i, s in enumerate(starts)]
    window = ECEPHYS_WINDOW_SAMPLES if label.startswith("ecephys") else OPHYS_WINDOW_FRAMES
    w_start, w_stop = centered_window(n_total, window)
    for i, s in enumerate(range(w_start, w_stop, length)):
        out.append(("window", i, s, min(s + length, w_stop)))
    return out


def run(label: str, output: Path) -> None:
    spec = DATASETS[label]
    chunk_label = CODEC_SWEEP_CHUNKS[spec["prefix"]][0]
    chunks = spec["chunks"][chunk_label]
    configs = [make_config(1, chunk_label, chunks, *setting) for setting in CODEC_SETTINGS]
    source_type = EcephysSource if label.startswith("ecephys") else OphysSource
    source = source_type(RAW_PATHS[label], spec["full_shape"])

    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(output.name + ".partial")
    with open(tmp, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(COLUMNS)
        for kind, index, start, stop in blocks(label, source.n, chunks[0]):
            data = source.read(start, stop)
            for config in configs:
                stored = storage_bytes(data, config)
                writer.writerow([label, kind, index, start, stop, config.config_id, data.nbytes, stored,
                                 f"{data.nbytes / stored:.4f}"])
            f.flush()
            print(f"{label} {kind} {index}: frames or samples {start}:{stop}", flush=True)
    tmp.replace(output)


def summary(paths: list[Path]) -> None:
    rows = []
    for path in paths:
        with open(path, newline="") as f:
            rows += list(csv.DictReader(f, delimiter="\t"))
    for label in dict.fromkeys(r["label"] for r in rows):
        mine = [r for r in rows if r["label"] == label]
        config_ids = list(dict.fromkeys(r["config_id"] for r in mine))
        window = {
            c: sum(int(r["nbytes"]) for r in mine if r["kind"] == "window" and r["config_id"] == c)
            / sum(int(r["storage_bytes"]) for r in mine if r["kind"] == "window" and r["config_id"] == c)
            for c in config_ids
        }
        segments = sorted({int(r["index"]) for r in mine if r["kind"] == "segment"})
        seg_ratio = {(int(r["index"]), r["config_id"]): float(r["ratio"]) for r in mine if r["kind"] == "segment"}
        print(f"== {label}: window and {len(segments)} segments")
        print(f"   {'config_id':<28} {'window':>7} {'seg min':>8} {'seg med':>8} {'seg max':>8}  window in range")
        for c in config_ids:
            values = [seg_ratio[(i, c)] for i in segments]
            inside = min(values) <= window[c] <= max(values)
            print(f"   {c:<28} {window[c]:7.3f} {min(values):8.3f} {float(np.median(values)):8.3f} "
                  f"{max(values):8.3f}  {'yes' if inside else 'NO'}")
        window_order = sorted(config_ids, key=lambda c: -window[c])
        differing = [i for i in segments if sorted(config_ids, key=lambda c: -seg_ratio[(i, c)]) != window_order]
        print(f"   ranking by ratio in the window: {' > '.join(window_order)}")
        print(f"   segments ranking the codec settings differently: {differing if differing else 'none'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check the centered window's compression ratios.")
    commands = parser.add_subparsers(dest="command", required=True)
    p_run = commands.add_parser("run", help="compress the segments and window of one dataset")
    p_run.add_argument("label", choices=list(DATASETS))
    p_run.add_argument("output", type=Path)
    p_summary = commands.add_parser("summary", help="summarize the output of run")
    p_summary.add_argument("outputs", type=Path, nargs="+")
    args = parser.parse_args()
    if args.command == "run":
        run(args.label, args.output)
    else:
        summary(args.outputs)


if __name__ == "__main__":
    main()
