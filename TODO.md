# TODO

## Validation subset for the final recommendations

**When:** after the main read results are summarized and the recommended configurations are drafted.

**Goal:** check that the recommendations hold outside the conditions of the main runs (shared lr7 nodes, Lustre scratch, one core per task), so the paper can say which conclusions depend on the storage and which do not.

**Configurations** (per dataset, about 15 to 20, from the existing config tables and write outputs):

- The recommended configuration for each access pattern, and the runner-up.
- The uncompressed baselines: `<label>_unchunked.nwb`, plus EC384M-none and OCframeM-none.
- The library default chunk shapes: ECh5py-gzip4, EChdmf-gzip4, ECneuroconv-gzip4, OCh5py-gzip4, OChdmf-gzip4, and OCneuroconv-gzip4.
- One configuration each where chunk shape and access pattern are mismatched, for example EC32L for E1 and OCframeL for O4, as a check that the large effects reproduce.

**Conditions** (each run with the same read scripts, recording wall time and CPU time):

1. Exclusive lr7 nodes (`#SBATCH --exclusive`), reading the same files from Lustre. Compares against the shared-node runs to measure noise from other jobs on the node.
2. Node-local disk: copy the files to the node's local scratch inside the job, then read them there. Separates storage latency and bandwidth from decompression cost.
3. Repeats separated in time: run each repeat as its own job, a few hours apart, with the configurations in a random order within each job.
4. Optional: blosc2 with more than one core (`--cpus-per-task=4` and `--cpus-per-task=8`), since blosc2 decompresses in parallel when it has cores.

**Analysis:**

- Rank correlation of configurations between each condition and the main runs, per access pattern.
- The wall time minus CPU time split per configuration under each condition.
- For each recommendation, whether it holds under every condition, and if not, which condition changes it.

## Stage 2

**When:** after the stage 1 reads finish.

- Apply the selection rule at `STAGE2_SETTINGS` in `scripts/exp12_configs.py` to the stage 1 write stats and read results, per modality, and record the inputs and picks with the analysis.
- Fill in `STAGE2_SETTINGS`, regenerate the config tables, the write schedule, and the read schedule, and check that every stage 1 row and task number is unchanged.
- Submit the stage 2 writes, then the stage 2 reads (`--stage 2`). The read schedule's stage 2 holds the stage 2 configurations and re-reads of the stage 1 configurations on the non-sweep shapes (no compression and gzip 4) and the library defaults, in one random order.

## Before the writes

- Check that `write_output/` has no Lustre layout of its own (`lfs getstripe -d write_output`), so the test files get the scratch filesystem's default layout described in the README.
