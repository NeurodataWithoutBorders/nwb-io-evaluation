# Evaluation of Chunking and Compression Settings on NWB I/O

## Ryan's Experiment Setup

Log in to LBNL's Lawrencium computing cluster.

If using Jupyter, load module `miniforge3/25.9.1` on startup.

### Git repo setup

Before running `git` commands that involve remote access, make sure to start the SSH agent and add your key:

```bash
ssh-agent -s > "$HOME/.ssh/agent.env"
. "$HOME/.ssh/agent.env" > /dev/null
ssh-add -l > /dev/null || ssh-add ~/.ssh/id_ed25519
```

See `~/.bashrc` for more detail.

Check out this git repo to `$SCRATCH`:
```bash
cd $SCRATCH
git clone git@github.com:NeurodataWithoutBorders/nwb-io-evaluation.git
```

### Conda environment setup

Do this if the `environment.yml` file does not exist in the repo or you want to generate a new one to run the evaluation (the last command will overwrite any existing `environment.yml`).

It's useful to install h5py from conda-forge so that we can use utilities like `h5ls`.

```bash
# `conda init` and restarting the shell may be necessary if conda is not recognized
conda create -p $HOME/envs/nwbio -c conda-forge python=3.14 h5py --yes
conda activate $HOME/envs/nwbio
python -m pip install dandi hdf5plugin b2h5py ipykernel tifffile
ipython kernel install --user --name=nwbio
conda env export > environment.yml
```

### Download raw data for "Experiment 12"

Use the [Data Transfer server](https://scienceit-docs.lbl.gov/hpc/data-transfer-node/)

```bash
ssh rly@lrc-xfer.lbl.gov  # with LBNL HPC credentials + 2FA
```

```bash
mkdir -p $SCRATCH/nwb-io-evaluation/data
cd $SCRATCH/nwb-io-evaluation/data
```

Use `screen` or `tmux` because downloads take so long and you don't want the ssh connection to be interrupted because the laptop goes to sleep.

Note: the `h5ls` command can take some time to get the chunking, storage, and filter information for the dataset.

#### Ecephys Dataset 1
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000053/versions/0.210819.0345/assets/?path=sub-npI3/sub-npI3_ses-20190421_behavior+ecephys.nwb"
h5ls -v sub-npI3_ses-20190421_behavior+ecephys.nwb/acquisition/ElectricalSeries/data
```
- Shape: 349975807, 384
- Sampling rate: 30000 Hz (`starting_time` rate)
- Duration: 194.4 min
- Dtype: int16
- Chunks: 341774, 1
- Compression: gzip level 4
- This dataset is also used in the NWB Cloud Benchmarks project where it was rechunked to (262144, 32) with gzip level 4 compression.

#### Ecephys Dataset 2
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000409/versions/0.260309.1324/assets/?path=sub-CSHL045/sub-CSHL045_ses-034e726f-b35f-41e0-8d6c-a22cc32391fb_desc-raw_ecephys.nwb"
h5ls -v sub-CSHL045_ses-034e726f-b35f-41e0-8d6c-a22cc32391fb_desc-raw_ecephys.nwb/acquisition/ElectricalSeriesProbe00AP/data
```
- Shape: 114798408, 384
- Sampling rate: 30000 Hz (from `timestamps`, which have a constant interval)
- Duration: 63.8 min
- Dtype: int16
- Chunks: 13020, 384
- Compression: gzip level 4

#### Ecephys Dataset 3
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000620/versions/0.260127.2208/assets/?path=sub-Elgar/sub-Elgar_ses-2022-08-19_ecephys.nwb"
h5ls -v sub-Elgar_ses-2022-08-19_ecephys.nwb/acquisition/ElectricalSeriesAP/data
```
- Shape: 326714943, 384
- Sampling rate: 30001.065 Hz (`starting_time` rate)
- Duration: 181.5 min
- Dtype: int16
- Chunks: 78125, 64
- Compression: gzip level 4

#### Ophys Dataset 1
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000054/versions/0.210819.1547/assets/?path=sub-R6/sub-R6_ses-20200206T210000_behavior+ophys.nwb"
h5ls -v sub-R6_ses-20200206T210000_behavior+ophys.nwb/acquisition/TwoPhotonSeries/data
```
- Shape: 41673, 796, 512
- Sampling rate: 15.0 Hz (`starting_time` rate and `imaging_plane` `imaging_rate`)
- Duration: 46.3 min
- Dtype: uint16
- Chunks: 2, 100, 64
- Compression: gzip level 4
- This dataset is also used in the NWB Cloud Benchmarks project where it was rechunked to (20, 796, 512) with gzip level 4 compression.

#### Ophys Dataset 2
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000037/versions/0.240209.1623/assets/?path=sub-408021/sub-408021_ses-758519303_obj-raw_behavior+image+ophys.nwb"
h5ls -v sub-408021_ses-758519303_obj-raw_behavior+image+ophys.nwb/acquisition/motion_corrected_stack/data
```
- Shape: 126741, 512, 512
- Sampling rate: 30.075 Hz (from `timestamps`, median interval 0.03325 s; the `imaging_plane` `imaging_rate` says 30.0 Hz)
- Duration: 70.2 min
- Dtype: uint16
- Chunks: 16, 128, 128
- Compression: gzip level 4

#### Ophys Dataset 3
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000728/versions/0.240827.1809/assets/?path=sub-491604967/sub-491604967_ses-496908818-StimB_ophys.nwb"
h5ls -v sub-491604967_ses-496908818-StimB_ophys.nwb/acquisition/MotionCorrectedTwoPhotonSeries/data
```
- Shape: 113855, 512, 512
- Sampling rate: 30.075 Hz (from `timestamps`, median interval 0.03325 s; the `imaging_plane` has no `imaging_rate`)
- Duration: 63.1 min
- Dtype: uint16
- Chunks: 19, 512, 512
- Compression: gzip level 4


### Time windows

The raw binary and TIFF files hold each full recording. The test NWB files (unchunked and every chunking and compression configuration) hold a contiguous window from the middle of each recording, defined in `scripts/exp12_window.py`. Each test file names the series `ElectricalSeries` (ecephys) or `TwoPhotonSeries` (ophys), whatever its name in the source file, and leaves out other series of the same type, in /acquisition or in a processing module (such as ecephys1's 10 GB LFP), and series that link to a left-out series (see `scripts/exp12_series.py`).

- Ecephys: 36,000,000 samples, which is 20 min at 30 kHz (19.999 min for ecephys3 at 30001.065 Hz). Only the AP band is written, from `ElectricalSeries` in ecephys1, `ElectricalSeriesProbe00AP` in ecephys2, and `ElectricalSeriesAP` in ecephys3.
- Ophys: 40,960 frames, which is 45.5 min for ophys1 at 15 Hz and 22.7 min for ophys2 and ophys3 at 30.075 Hz, from `TwoPhotonSeries` in ophys1, `motion_corrected_stack` in ophys2, and `MotionCorrectedTwoPhotonSeries` in ophys3.

Each window is a whole number of chunk time extents for the EC and OC chunk shapes. The ophys1 frames are 796 pixels wide, which is not a multiple of the 128 and 16 pixel tiles, so those datasets have partly filled edge chunks; their uncompressed files report a compression ratio below 1.

### Chunking and compression configurations

Each dataset has a config table, `scripts/exp12_configs_<label>.tsv`, with one configuration per row. The tables are generated by `scripts/exp12_configs.py`, which documents the columns; to regenerate them:

```bash
python scripts/exp12_configs.py
```

Chunk shapes cross aspects with three sizes:

| Aspect | S (at most 1 MiB) | M (at most 8 MiB) | L (at most 24 MiB) |
|---|---|---|---|
| EC384: all channels | 1250 x 384 | 10000 x 384 | 30000 x 384 |
| EC64: 64 channel groups | 7500 x 64 | 60000 x 64 | 180000 x 64 |
| EC32: 32 channel groups | 15000 x 32 | 120000 x 32 | 360000 x 32 |
| EC1: single channels | 480000 x 1 | 3600000 x 1 (7.2 MB) | 12000000 x 1 (24 MB) |
| OCframe: whole frames, ophys2 and ophys3 (ophys1) | 2 (1) frames | 16 (10) frames | 40 (20) frames |
| OCtile128: 128 x 128 pixel tiles | 32 frames | 256 frames | 640 frames |
| OCtile16: 16 x 16 pixel tiles | 2048 frames | 10240 frames (5 MiB) | 20480 frames (10 MiB) |

The sizes are uncompressed. A chunk larger than the reader's chunk cache is decompressed again by every read that touches it again, and the cache depends on how the reader opens the file: `h5py.File` gives HDF5's default, 1 MiB in HDF5 1.x and 8 MiB in HDF5 2.x, and hdmf (so pynwb) opens files with 32 MiB. S fits every reader's cache, M fits HDF5 2.x's and pynwb's, and L fits pynwb's only. L is also sized for streaming: at the compression ratios measured for these data in an earlier run (about 2.2 for ecephys and 1.8 for ophys with the best codecs) and at 1.5, its chunks make byte range requests of about 8 to 15 MiB, within the 8 to 16 MB part size AWS suggests for downloading from S3. The ophys shapes hold the most frames that fit the size, divide the window, and span at most half of it (so every dataset has at least two chunks along time). The OCframe and OCtile128 L shapes are 20 MiB, and 15.5 MiB for ophys1's 796 x 512 frames. OCtile16 matches the 16 x 16 patch of the O4 read pattern, and its M and L shapes are 5 and 10 MiB: 8 MiB of 16 x 16 tiles is 16,384 frames, which does not divide the window, and 24 MiB is more than the window holds. The EC1 time extents are the largest multiples of one second that divide the window.

The configurations run in two stages:

1. Every codec setting on the codec sweep shapes, EC384M, EC32M, and EC1M for ecephys and OCframeM and OCtile16M for ophys, which ranks the codecs; no compression and gzip 4 on the other shapes (nine for ecephys, seven for ophys); and the three library default shapes. 138 configurations per ecephys dataset and 95 per ophys dataset.
2. Codec settings chosen from the stage 1 writes and reads, by the rule documented at `STAGE2_SETTINGS` in `scripts/exp12_configs.py`, on the other shapes. Filling in `STAGE2_SETTINGS` and regenerating the tables, the write schedule, and the read schedule adds the stage 2 configurations and reads after the stage 1 ones, leaving every stage 1 row and task number unchanged.

The stages run in order: stage 1 writes, stage 1 reads, stage 2 writes, stage 2 reads. The stage 2 reads also re-read the stage 1 configurations on the non-sweep shapes (no compression and gzip 4) and the library defaults, so every comparison between codec settings on one shape comes from the same read run.

- Codec settings: uncompressed; gzip 1, 4, 6, 9, lzf, lz4, and zstd 3, 11, 22, each with and without the HDF5 shuffle filter; Blosc2 blosclz 1, 9, lz4 1, 9, lz4hc 1, 5, 9, and zstd 2, 6, 9, each with Blosc2's shuffle and with its bitshuffle. Blosc2 zstd levels 2, 6, and 9 run zstd at levels 3, 11, and 22.
- Library defaults, both with gzip 4 and computed for the full-length recording:
  - ECh5py, OCh5py: h5py's chunk guess for `chunks=True`, which hdmf used for `chunks=True` and iterator writes before it computed its own shape. Many existing DANDI files have these chunks.
  - EChdmf, OChdmf: hdmf's default chunk shape, which the hdmf version in `environment.yml` uses for `chunks=True` and iterator writes.
  - ECneuroconv, OCneuroconv: NeuroConv's default chunk shape, about 10 MB per chunk: `(78125, 64)` for the ecephys datasets, `(12, 796, 512)` for ophys1, and `(19, 512, 512)` for ophys2 and ophys3. In a scan of large DANDI dandisets, `(78125, 64)` with gzip 4 is the most common setting for raw Neuropixels data; `neuroconv_chunks` in `scripts/exp12_configs.py` gives the source and the evidence.

The tables record these shapes for the installed h5py and hdmf. To check that they still hold, regenerate the tables and compare:

```bash
python scripts/exp12_configs.py && git diff --exit-code scripts/exp12_configs_*.tsv
```

Each write produces `write_output/<label>_<config_id>.nwb` and a stats table `write_output/stats_<label>_<config_id>.tsv`, whose columns are documented in `scripts/exp12_write_stats.py`. The unchunked writes produce `<label>_unchunked.nwb` and `stats_<label>_unchunked.tsv`. Each write first removes its earlier outputs, writes the NWB file as `<name>.nwb.partial`, reads the series back and checks it against the source data (`scripts/exp12_verify.py`), and renames the file when the data match. An interrupted write, or one whose data do not match the source, leaves only the `.partial` file, which `exp12_check_nwb_files.py` lists. The stats also record the SHA-256 of the series data, which is the same for every configuration of a dataset.

The test files use the scratch filesystem's default Lustre layout, a progressive file layout whose striping depends on the offset in the file:

- 0 to 64 KiB: 1 stripe on the NVMe pool (`ddn_nvme2`).
- 64 KiB to 128 MiB: 1 stripe of 1 MiB on the HDD pool (`ddn_hdd`).
- 128 MiB to the end of the file: 8 stripes of 16 MiB on the HDD pool.

The test files are 10 to 33 GB, so nearly all of their data is in the 8-stripe HDD component. For the files to inherit this default, `write_output/` must not have a layout of its own. To check both:

```bash
lfs getstripe -d $SCRATCH
lfs getstripe -d write_output
```

### Ophys: Generate raw ophys files (TIFF) from the ophys NWB files

Note that these do not take long to run (~20-30 min) and could have been put into a sbatch script.

Each recording becomes a directory of uncompressed multi-page TIFF files of 1,000 frames each (the last file holds the rest), in the layout ScanImage writes when it splits a session into files. `scripts/exp12_tiff.py` describes the layout and reads it for the write scripts. Each output directory must be empty or not exist.

Run:
```bash
conda activate $HOME/envs/nwbio
cd $SCRATCH/nwb-io-evaluation
python scripts/exp12_nwb_to_tiff.py data/sub-R6_ses-20200206T210000_behavior+ophys.nwb TwoPhotonSeries raw_data/ophys1
python scripts/exp12_nwb_to_tiff.py data/sub-408021_ses-758519303_obj-raw_behavior+image+ophys.nwb motion_corrected_stack raw_data/ophys2
python scripts/exp12_nwb_to_tiff.py data/sub-491604967_ses-496908818-StimB_ophys.nwb MotionCorrectedTwoPhotonSeries raw_data/ophys3
```

### Ecephys: Generating binary files (BIN) from the ecephys NWB files

This takes about 2 hours each, so we put it in a slurm script.

Run:
```bash
cd $SCRATCH/nwb-io-evaluation
mkdir -p write_logs raw_data
sbatch scripts/exp12_write_ecephys_all_binary.sh
```

### Checking the window

`scripts/exp12_window_check.py` checks that the centered window is representative of the whole recording for compression ratio, so the ratios and codec rankings from the test files can be stated for the session as a whole. Per dataset, it takes 10 one-minute segments spaced evenly across the full recording in the raw binary or TIFF files, and the centered window in blocks of the same length, and compresses each with five codec settings (gzip 4, gzip 4 with shuffle, zstd 3 with shuffle, Blosc2 lz4 1 with bitshuffle, and Blosc2 zstd 2 with bitshuffle) on EC384M or OCframeM, in an in-memory HDF5 file with the same filter stack as the test files. Run it once the raw files exist and before the writes, so an unrepresentative window can be moved without rewriting the test files.

Submit one job per dataset (each writes `window_check/<label>.tsv`):

```bash
mkdir -p write_logs window_check
for LABEL in ecephys1 ecephys2 ecephys3 ophys1 ophys2 ophys3; do
    sbatch --job-name=exp12_window_check_$LABEL \
        --output=write_logs/window_check_$LABEL--%j-out.log \
        --error=write_logs/window_check_$LABEL--%j-err.log \
        scripts/exp12_window_check.sh $LABEL
done
```

Then summarize, per dataset and codec setting, the window's ratio against the range of the segment ratios, and whether each segment ranks the codec settings in the same order as the window:

```bash
python scripts/exp12_window_check.py summary window_check/*.tsv
```

For every dataset and codec setting, the window's ratio falls within the range of the segment ratios. Every segment of the ecephys datasets and ophys1 ranks the codec settings in the same order as the window. In ophys2 (segments 6 to 9) and ophys3 (segments 8 and 9), zstd 3 with shuffle and Blosc2 zstd 2 with bitshuffle swap places, and in those segments their ratios differ by less than 0.5%.

### Writing the test files

Each write, one configuration or the unchunked baseline of one dataset, is a task in `scripts/exp12_write_schedule.tsv`: 705 stage 1 tasks (139 per ecephys dataset and 96 per ophys dataset), with the stage 2 tasks after them once they are added, each stage in a random order with a fixed seed. The ecephys and ophys writes are separate runs, each submitted, checked, and rerun on its own, and each starts once the raw binary or TIFF files of its three datasets exist. Within a run the tasks keep the schedule's random order, so the writes running at any one time are a mix of datasets, chunk shapes, and codecs. `scripts/exp12_write_schedule.py` documents the columns; regenerate the schedule whenever the config tables change:

```bash
python scripts/exp12_write_schedule.py
```

Each task runs as its own job, `scripts/exp12_write.sh <task>`, with logs `write_logs/<label>_<config_id>--<job ID>-{out,err}.log`. `scripts/exp12_submit_writes.py` submits the tasks of one modality in schedule order, keeping at most 95 jobs of the user in the queue (pending and running, including jobs unrelated to the writes), and records each submission in `write_logs/exp12_write_submitted.tsv`. A task recorded there is not submitted again, so a stopped submission continues where it left off. The ecephys and ophys submissions can run at the same time and together stay under the cap.

For each modality, first submit a smoke test: per dataset, the unchunked write; EC384M or OCframeM uncompressed, with zstd 22, and with Blosc2 zstd 9 (the slowest codecs); the h5py default chunks (the smallest chunks, with a partly filled last chunk); and EC1L or OCtile16L with gzip 4 (the longest chunks along time, which for EC1L need the largest write buffer, 9.2 GB, and for ophys1 partly filled edge chunks):

```bash
mkdir -p write_logs write_output
MODALITY=ecephys  # or ophys
TASKS=$(awk -F'\t' -v m="$MODALITY" '$3 ~ "^" m && $5 ~ /^(unchunked|(EC384|OCframe)M-(none|zstd22-shuf|b2zstd9-bitshuf)|(EC|OC)h5py-gzip4|(EC1|OCtile16)L-gzip4)$/ {print $1}' scripts/exp12_write_schedule.tsv | paste -sd,)
python scripts/exp12_submit_writes.py submit $MODALITY --tasks $TASKS
```

Once those complete, check their run times against the 24 hour limit in `scripts/exp12_write.sh`, then submit the rest of that modality. The submission runs as a job because it waits on the queue for as long as the writes take:

```bash
sbatch scripts/exp12_submit_writes.sh $MODALITY
```

If the write scripts change after the smoke test, resubmit the smoke test tasks with `--tasks` so every output comes from the same code.

To report the state of every task of a modality, with the failed ones and the command that resubmits them (leave out `--modality` for both):

```bash
python scripts/exp12_submit_writes.py status --modality $MODALITY
```

To list NWB files that fail to open and write outputs that are missing, on a compute node, since it opens every file:

```bash
srun --account=pc_rlyneuro --partition=lr7 --qos=lr_normal --time=2:00:00 --mem=8G --pty bash
conda activate $HOME/envs/nwbio
python scripts/exp12_check_nwb_files.py write_output ecephys1 ecephys2 ecephys3
python scripts/exp12_check_nwb_files.py write_output ophys1 ophys2 ophys3
```

It lists files whose errors show they are corrupt, and `--delete` removes them.

### Reading the test files

Each read, one test file read with one pattern, one read path, and one chunk cache setting, is a task in `scripts/exp12_read_schedule.tsv`: 7,071 stage 1 tasks, with the stage 2 tasks after them once stage 2 is chosen, each stage in a random order with a fixed seed. `scripts/exp12_read_patterns.py` defines the patterns (E1 to E4, O1 to O4), the read paths, and the cache settings, and `scripts/exp12_read_schedule.py` documents the columns. Regenerate the schedule whenever the config tables change:

```bash
python scripts/exp12_read_schedule.py
```

- **Read paths**: every file is read with h5py and the HDF5 filters. Blosc2 files are also read with b2h5py's fast slicing, which reads the needed blocks of each chunk itself and bypasses the HDF5 chunk cache.
- **Chunk cache**: set explicitly when the file is opened. The h5py reads of a sequential pattern (E1, E2, O1, O2, O4) whose batches cut across chunks run with 1 MiB and 8 MiB (what `h5py.File` gives with HDF5 1.x and 2.x), 32 MiB (what pynwb gives when it opens a file by path, since hdmf sets it), and a cache fitted to one row of chunks across the full width, so each chunk is decompressed once. Every other read runs with 32 MiB. On EC1M, where an E1 read with a cache smaller than one row of chunks decompresses each chunk 120 times per pass (about 6 hours per pass), E1 runs with all four caches for no compression and gzip 4 in stage 1 and for the stage 2 picks in stage 2, and with the fitted cache only for the other codec settings (`FITTED_ONLY_READS` in `scripts/exp12_read_schedule.py`).
- **Isolation**: each repeat or sample runs in its own child process, which evicts the file from the page cache, opens it, and times only the reads. A crashed child is recorded and the task continues. The output file is rewritten after every repeat or sample, so a job killed before it finishes keeps what it completed.
- **Time limit**: each task fits the 24 hour limit of `scripts/exp12_read.sh`. No repeat or sample starts once it would end past 22 hours at the length of the longest so far, one still running at 22 hours is stopped, and the checks stop at 23.5 hours. A sequential pattern also stops after three repeats (samples for O4) over 170 minutes for ecephys or 100 minutes for ophys. Either stop sets `truncated`, so the completed repeats are a right-censored sample.
- **Checks**: after the timed reads, the first and last selections of the first repeat or sample are compared with the same selections of the unchunked file, b2h5py reads of the whole window are checked against the SHA-256 the write recorded, and b2h5py's fast path must have served every timed read of the b2h5py path and none of the h5py path.

Each task runs as its own job, `scripts/exp12_read.sh <task>`, and writes `read_output/<label>_<config_id>_<pattern>_<path>_<cache>_s<stage>.h5`, whose attributes and datasets are documented in `scripts/exp12_read.py`. Logs go to `read_logs/<same name>--<job ID>-{out,err}.log`. `scripts/exp12_submit_reads.py` submits the tasks of one modality and stage in schedule order under the same 95 job cap and lock as the writes, so reads and writes running together stay under it, and records each submission in `read_logs/exp12_read_submitted.tsv`. The reads of a modality start once its writes are complete and checked.

For each modality, first submit a smoke test: on ecephys1 or ophys1, the unchunked file; the longest chunks with gzip 4 at a 1 MiB cache (the most decompression per byte read, which for EC1L runs until the time limit stops it) and at the fitted cache (up to 16 GiB for OCtile16L on ophys1, the most memory); for ecephys, the h5py default chunks at 1 MiB (384 chunks across); Blosc2 zstd 9 with bitshuffle through b2h5py; and for ophys, single frames of uncompressed OCframeM and O4 on OCframeL with gzip 4 (each of its 50 samples decompresses the whole window):

```bash
mkdir -p read_logs read_output
MODALITY=ecephys  # or ophys
SMOKE='^(ecephys1_(unchunked_e1_h5py_32mib|EC1L-gzip4_e1_h5py_(1mib|fitted)|ECh5py-gzip4_e1_h5py_1mib|EC32M-b2zstd9-bitshuf_e[14]_b2h5py_32mib)|ophys1_(unchunked_o1_h5py_32mib|OCtile16L-gzip4_o1_h5py_1mib|OCtile16L-gzip4_o2_h5py_fitted|OCtile16M-b2zstd9-bitshuf_o[14]_b2h5py_32mib|OCframeM-none_o3_h5py_32mib|OCframeL-gzip4_o4_h5py_32mib))_s1$'
TASKS=$(awk -F'\t' -v m="$MODALITY" -v re="$SMOKE" 'NR > 1 && $3 ~ "^" m && ($3"_"$5"_"$6"_"$7"_"$8"_s"$2) ~ re {print $1}' scripts/exp12_read_schedule.tsv | paste -sd,)
python scripts/exp12_submit_reads.py submit $MODALITY --tasks $TASKS
```

Once those complete, check their run times, which of them stopped early (`stop_reason`), and their memory use against the 32 GB request, then submit the rest of that modality's stage 1 reads:

```bash
sbatch scripts/exp12_submit_reads.sh $MODALITY
```

To report the state of every task, with the failed ones and the command that resubmits them (a failed read with an output file had a crashed repeat or sample, or a check that did not match or ran out of time; its attributes say which):

```bash
python scripts/exp12_submit_reads.py status --modality $MODALITY --stage 1
```

To summarize the results in one table, one row per task:

```bash
python scripts/exp12_read_results.py read_output read_summary.tsv
```
