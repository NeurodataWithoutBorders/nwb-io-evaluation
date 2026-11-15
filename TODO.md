# TODO

## Validation subset for the final recommendations

**When:** after the main read results are summarized and the recommended configurations are drafted.

**Goal:** check that the recommendations hold outside the conditions of the main runs (shared lr7 nodes, Lustre scratch, one core per task), so the paper can say which conclusions depend on the storage and which do not.

**Configurations** (per dataset, about 15 to 20, from the existing config tables and write outputs):

- The recommended configuration for each access pattern, and the runner-up.
- The uncompressed baselines: `<label>_unchunked.nwb`, plus EC2-none and OC2-none.
- The library default chunk shapes: ECh5py-gzip4, EChdmf-gzip4, ECneuroconv-gzip4, OCh5py-gzip4, OChdmf-gzip4, and OCneuroconv-gzip4.
- One configuration each where chunk shape and access pattern are mismatched, for example EC5 for E1 and OC1 for O4, as a check that the large effects reproduce.

**Conditions** (each run with the same read scripts, recording wall time and CPU time):

1. Exclusive lr7 nodes (`#SBATCH --exclusive`), reading the same files from Lustre. Compares against the shared-node runs to measure noise from other jobs on the node.
2. Node-local disk: copy the files to the node's local scratch inside the job, then read them there. Separates storage latency and bandwidth from decompression cost.
3. Repeats separated in time: run each repeat as its own job, a few hours apart, with the configurations in a random order within each job.
4. Optional: blosc2 with more than one core (`--cpus-per-task=4` and `--cpus-per-task=8`), since blosc2 decompresses in parallel when it has cores.

**Analysis:**

- Rank correlation of configurations between each condition and the main runs, per access pattern.
- The wall time minus CPU time split per configuration under each condition.
- For each recommendation, whether it holds under every condition, and if not, which condition changes it.

## Before the read benchmarks

- Update the read scripts on `exp12-read-improvements` to the config tables: config IDs in file names, the row count from the table, `--array=1-198`, `#SBATCH --export=NONE` with `srun --export=ALL`, and `cpu_affinity_count` and `os_cpu_count` in the recorded metadata.
- Read Blosc2 configurations both with and without b2h5py, run each timed read in its own subprocess so a crash is recorded, check that each read path returns the same bytes as the source, and record whether b2h5py's fast path was used.
- Decide whether the sequential patterns (E1, E2, O1, O2, O4) read the whole window or a bounded span at a random offset per repeat.
- Decide the Lustre stripe count for `write_output/` and set it with `lfs setstripe` before writing.
