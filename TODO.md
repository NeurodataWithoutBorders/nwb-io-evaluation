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

### Window check

**When:** any time after the raw `.bin` and TIFF files exist. It does not depend on the read results, and running it before the main writes means an unrepresentative window can be moved without rewriting the test files.

**Goal:** check that the centered window (`scripts/exp12_window.py`) is representative of the whole recording for compression ratio, so the ratios and codec rankings from the main runs can be stated for the session as a whole.

**Method:**

- Per dataset, take 10 one-minute segments spaced evenly across the full recording from the raw files, plus the centered window.
- Compress each segment in memory with about 5 codec settings on one frame-aligned chunk shape (EC384M and OCframeM): gzip 4, the recommended codecs, and one Blosc2 bitshuffle setting. Write each to an in-memory HDF5 file (`h5py.File(..., driver="core", backing_store=False)`) with `h5dataio_kwargs`, so the filter stack is the same as in the test files, and record the dataset's storage size.

**Analysis:**

- Per codec, the ratio of each segment and of the centered window, and the spread across segments.
- Whether the ranking of codecs by ratio changes across segments.
- If the centered window falls outside the range of the segments, or the ranking changes, say so in the paper and consider moving the window.

## Stage 2

**When:** after the stage 1 reads finish.

- Apply the selection rule at `STAGE2_SETTINGS` in `scripts/exp12_configs.py` to the stage 1 write stats and read results, per modality, and record the inputs and picks with the analysis.
- Fill in `STAGE2_SETTINGS`, regenerate the config tables and the write schedule, and check that every stage 1 row and task number is unchanged.
- Run the stage 2 reads together with re-reads of the stage 1 configurations on the non-sweep shapes (no compression and gzip 4) and the library defaults, in one randomized run.

## Before the writes

- Check that `write_output/` has no Lustre layout of its own (`lfs getstripe -d write_output`), so the test files get the scratch filesystem's default layout described in the README.

## Before the read benchmarks

- Update the read scripts on `exp12-read-improvements` to the config tables: config IDs in file names, the row count from the table, submission under the 100 job cap with a random order (as `scripts/exp12_submit_writes.py` does for the writes), `#SBATCH --export=NONE` with `srun --export=ALL`, and `cpu_affinity_count` and `os_cpu_count` in the recorded metadata.
- Read Blosc2 configurations both with and without b2h5py, run each timed read in its own subprocess so a crash is recorded, check that each read path returns the same bytes as the source, and record whether b2h5py's fast path was used.
- Decide whether the sequential patterns (E1, E2, O1, O2, O4) read the whole window or a bounded span at a random offset per repeat.
- Set the chunk cache size explicitly for every read (`rdcc_nbytes` and `rdcc_nslots`, passing an `h5py.File` to `NWBHDF5IO` with `file=`) and record it. The default is 8 MiB per dataset in HDF5 2.x (h5py 3.16.0 wheels on PyPI bundle HDF5 2.0.0) and 1 MiB in HDF5 1.x (h5py 3.15.1 wheels bundle HDF5 1.14.6), and a chunk larger than the cache is decompressed again on every read that touches it. The S shapes and the h5py defaults fit both, the M shapes and the hdmf defaults fit 8 MiB only, and the L shapes and the NeuroConv defaults (about 10 MB) fit neither. A sequential read keeps a chunk for the next read call only if the cache holds every chunk across the full width of one time step of chunks, so chunks that fit the cache one at a time are still decompressed again by each read that touches them when that row does not fit (for example E1 on EC64M touches six 7.68 MB chunks, each of which spans two E1 reads). Run the patterns that touch a chunk in more than one read call with 1 MiB, 8 MiB, and a fitted cache, so the results separate chunk size from cache fit and hold for readers on either HDF5. Size the fitted cache per configuration from its chunk shape: `rdcc_nbytes` at least the number of chunks across the full width times the chunk's bytes, plus one chunk, and `rdcc_nslots` a prime at least 100 times the number of chunks it holds. The rows range from 0.9 MiB (EC384S) to 264 MiB (EC32L), 560 MiB (OCtile128L, ophys1), 990 MiB (OCh5py, ophys2), 352 MiB to 8.6 GiB for EC1 (384 single channel chunks), and 5 to 16 GiB for OCtile16 (1,024 tiles per 512 x 512 frame and 1,600 per 796 x 512 frame, 16 GiB for OCtile16L on ophys1), which fits a 32 GB job alongside an O2 batch of at most 4 GB.
