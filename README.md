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

The raw binary and TIFF files hold each full recording. The test NWB files (unchunked and every chunking and compression configuration) hold a contiguous window from the middle of each recording, defined in `scripts/exp12_window.py`. Each test file names the series `ElectricalSeries` (ecephys) or `TwoPhotonSeries` (ophys), whatever its name in the source file, and leaves out other series of the same type and series that link to the source series (see `scripts/exp12_series.py`).

- Ecephys: 108,000,000 samples, which is 60 min at 30 kHz (59.998 min for ecephys3 at 30001.065 Hz). Only the AP band is written, from `ElectricalSeries` in ecephys1, `ElectricalSeriesProbe00AP` in ecephys2, and `ElectricalSeriesAP` in ecephys3.
- Ophys: 40,960 frames, which is 45.5 min for ophys1 at 15 Hz and 22.7 min for ophys2 and ophys3 at 30.075 Hz, from `TwoPhotonSeries` in ophys1, `motion_corrected_stack` in ophys2, and `MotionCorrectedTwoPhotonSeries` in ophys3.

Each window is a whole number of chunk time extents for the chunk shapes EC1 to EC5 and OC1 to OC5. The ophys1 frames are 796 pixels wide, which is not a multiple of the 128 and 16 pixel tiles of OC3 to OC5, so those datasets have partly filled edge chunks; their uncompressed files report a compression ratio below 1.

### Chunking and compression configurations

Each dataset has a config table, `scripts/exp12_configs_<label>.tsv`, with one configuration per row. Row N is SLURM array task N. The tables are generated by `scripts/exp12_configs.py`, which documents the columns; to regenerate them:

```bash
python scripts/exp12_configs.py
```

Each table has 198 configurations: 5 chunk shapes times 39 codec settings, plus three library default chunk shapes.

- Chunk shapes: EC1 to EC5 for ecephys and OC1 to OC5 for ophys, as in the paper.
- Codec settings: uncompressed; gzip 1, 4, 6, 9, lzf, lz4, and zstd 3, 11, 22, each with and without the HDF5 shuffle filter; Blosc2 blosclz 1, 9, lz4 1, 9, lz4hc 1, 5, 9, and zstd 2, 6, 9, each with Blosc2's shuffle and with its bitshuffle. Blosc2 zstd levels 2, 6, and 9 run zstd at levels 3, 11, and 22.
- Library defaults, both with gzip 4 and computed for the full-length recording:
  - ECh5py, OCh5py: h5py's chunk guess for `chunks=True`, which hdmf used for `chunks=True` and iterator writes before it computed its own shape. Many existing DANDI files have these chunks.
  - EChdmf, OChdmf: hdmf's default chunk shape, which the hdmf version in `environment.yml` uses for `chunks=True` and iterator writes.
  - ECneuroconv, OCneuroconv: NeuroConv's default chunk shape, about 10 MB per chunk: `(78125, 64)` for the ecephys datasets, `(12, 796, 512)` for ophys1, and `(19, 512, 512)` for ophys2 and ophys3. In a scan of large DANDI dandisets, `(78125, 64)` with gzip 4 is the most common setting for raw Neuropixels data; `neuroconv_chunks` in `scripts/exp12_configs.py` gives the source and the evidence.

The tables record these shapes for the installed h5py and hdmf. To check that they still hold, regenerate the tables and compare:

```bash
python scripts/exp12_configs.py && git diff --exit-code scripts/exp12_configs_*.tsv
```

Each write produces `write_output/<label>_<config_id>.nwb` and a stats table `write_output/stats_<label>_<config_id>.tsv`, whose columns are documented in `scripts/exp12_write_stats.py`. The unchunked writes produce `<label>_unchunked.nwb` and `stats_<label>_unchunked.tsv`. Each write first removes its earlier outputs, writes the NWB file as `<name>.nwb.partial`, and renames it when complete, so an interrupted write leaves no outputs.

To list NWB files that fail to open and write outputs that are missing:

```bash
python scripts/exp12_check_nwb_files.py write_output ecephys1 ecephys2 ecephys3 ophys1 ophys2 ophys3
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

### Generate unchunked NWB files from the raw ophys files

Note that these do not take long to run (<2 hours) and are put into a sbatch script to control the environment and time the operation.

Run:
```bash
mkdir -p write_output
sbatch scripts/exp12_write_ophys1_unchunked.sh
sbatch scripts/exp12_write_ophys2_unchunked.sh
sbatch scripts/exp12_write_ophys3_unchunked.sh

h5ls -v write_output/ophys1_unchunked.nwb/acquisition/TwoPhotonSeries/data
h5ls -v write_output/ophys2_unchunked.nwb/acquisition/TwoPhotonSeries/data
h5ls -v write_output/ophys3_unchunked.nwb/acquisition/TwoPhotonSeries/data
```

### Generate ophys files with different chunk shapes and compression settings

Run sbatch with `#SBATCH --array=1` before doing the full array to make sure the env and script are set up correctly.

```bash
sbatch scripts/exp12_write_ophys1.sh
sbatch scripts/exp12_write_ophys2.sh
sbatch scripts/exp12_write_ophys3.sh

./scripts/exp12_check_jobs.sh 20463593 write_logs scripts/exp12_configs_ophys1.tsv ophys1
./scripts/exp12_check_jobs.sh 20463965 write_logs scripts/exp12_configs_ophys2.tsv ophys2
./scripts/exp12_check_jobs.sh 20463966 write_logs scripts/exp12_configs_ophys3.tsv ophys3
```

### Evaluating read times of ophys files

Run sbatch with `#SBATCH --array=1` before doing the full array to make sure the env and script are set up correctly. 

```bash
mkdir -p read_logs read_output
sbatch scripts/exp12_read_ophys1.sh
sbatch scripts/exp12_read_ophys2.sh
sbatch scripts/exp12_read_ophys3.sh

./scripts/exp12_check_jobs.sh 20465176 read_logs scripts/exp12_configs_ophys1.tsv ophys1
./scripts/exp12_check_jobs.sh 20467784 read_logs scripts/exp12_configs_ophys2.tsv ophys2
./scripts/exp12_check_jobs.sh 20467966 read_logs scripts/exp12_configs_ophys3.tsv ophys3
```

### Ecephys: Generating binary files (BIN) from the ecephys NWB files

This takes about 2 hours each, so we put it in a slurm script.

Run:
```bash
cd $SCRATCH/nwb-io-evaluation
mkdir -p write_logs raw_data
sbatch scripts/exp12_write_ecephys_all_binary.sh
```

### Generate unchunked NWB files from the raw ecephys files

Note that these do not take long to run (<10 min) and are put into a sbatch script to control the environment and time the operation.

Run:
```bash
mkdir -p write_output
sbatch scripts/exp12_write_ecephys1_unchunked.sh
sbatch scripts/exp12_write_ecephys2_unchunked.sh
sbatch scripts/exp12_write_ecephys3_unchunked.sh

h5ls -v write_output/ecephys1_unchunked.nwb/acquisition/ElectricalSeries/data
h5ls -v write_output/ecephys2_unchunked.nwb/acquisition/ElectricalSeries/data
h5ls -v write_output/ecephys3_unchunked.nwb/acquisition/ElectricalSeries/data
```

### Generating ecephys files with different chunk shapes and compression settings

```bash
sbatch scripts/exp12_write_ecephys1.sh
sbatch scripts/exp12_write_ecephys2.sh
sbatch scripts/exp12_write_ecephys3.sh
```

### Evaluating read times of ecephys files

Run sbatch with `#SBATCH --array=1` before doing the full array to make sure the env and script are set up correctly. 

```bash
mkdir -p read_logs read_output
sbatch scripts/exp12_read_ecephys1.sh
sbatch scripts/exp12_read_ecephys2.sh
sbatch scripts/exp12_read_ecephys3.sh
```