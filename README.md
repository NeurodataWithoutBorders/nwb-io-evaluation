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
- Dtype: int16
- Chunks: 13020, 384
- Compression: gzip level 4

#### Ecephys Dataset 3
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000620/versions/0.260127.2208/assets/?path=sub-Elgar/sub-Elgar_ses-2022-08-19_ecephys.nwb"
h5ls -v sub-Elgar_ses-2022-08-19_ecephys.nwb/acquisition/ElectricalSeriesAP/data
```
- Shape: 326714943, 384
- Dtype: int16
- Chunks: 78125, 64
- Compression: gzip level 4

#### Ophys Dataset 1
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000054/versions/0.210819.1547/assets/?path=sub-R6/sub-R6_ses-20200206T210000_behavior+ophys.nwb"
h5ls -v sub-R6_ses-20200206T210000_behavior+ophys.nwb/acquisition/TwoPhotonSeries/data
```
- Shape: 41673, 796, 512
- Dtype: uint16
- Chunks: 2, 100, 64
- Compression: gzip level 4
- This dataset is also used in the NWB Cloud Benchmarks project where it was rechunked to (20, 796, 512) with gzip level 4 compression.

#### Ophys Dataset 2
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000037/versions/0.240209.1623/assets/?path=sub-408021/sub-408021_ses-758519303_behavior+ophys.nwb"
h5ls -v sub-408021_ses-758519303_obj-raw_behavior+image+ophys.nwb/acquisition/motion_corrected_stack/data
```
- Shape: 126741, 512, 512
- Dtype: uint16
- Chunks: 16, 128, 128
- Compression: gzip level 4

#### Ophys Dataset 3
```bash
dandi download "https://api.dandiarchive.org/api/dandisets/000728/versions/0.240827.1809/assets/?path=sub-491604967/sub-491604967_ses-496908818-StimB_ophys.nwb"
h5ls -v sub-491604967_ses-496908818-StimB_ophys.nwb/acquisition/MotionCorrectedTwoPhotonSeries/data
```
- Shape: 113855, 512, 512
- Dtype: uint16
- Chunks: 19, 512, 512
- Compression: gzip level 4


### Ophys: Generate raw ophys files (TIFF) from the ophys NWB files

Note that these do not take long to run (~20-30 min) and could have been put into a sbatch script.

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
h5ls -v write_output/ophys2_unchunked.nwb/acquisition/motion_corrected_stack/data
h5ls -v write_output/ophys3_unchunked.nwb/acquisition/MotionCorrectedTwoPhotonSeries/data
```

### Generate ophys files with different chunk shapes and compression settings

Run sbatch with `#SBATCH --array=1` before doing the full array to make sure the env and script are set up correctly.

```bash
sbatch scripts/exp12_write_ophys1.sh
sbatch scripts/exp12_write_ophys2.sh
sbatch scripts/exp12_write_ophys3.sh

./scripts/exp12_check_jobs.sh 20463593 write_logs scripts/exp12_configs_ophys1.txt ophys1
./scripts/exp12_check_jobs.sh 20463965 write_logs scripts/exp12_configs_ophys2.txt ophys2
./scripts/exp12_check_jobs.sh 20463966 write_logs scripts/exp12_configs_ophys3.txt ophys3
```

Get h5py default chunk sizes (Config 91):
```bash
h5ls -v ophys1_Config091.nwb/acquisition/TwoPhotonSeries/data
# Chunks:    {652, 25, 16} 521600 bytes

h5ls -v ophys2_Config091.nwb/acquisition/motion_corrected_stack/data
# Chunks:    {1981, 8, 16} 507136 bytes

h5ls -v ophys3_Config091.nwb/acquisition/MotionCorrectedTwoPhotonSeries/data
# Chunks:    {1779, 8, 16} 455424 bytes
```

### Evaluating read times of ophys files

Run sbatch with `#SBATCH --array=1` before doing the full array to make sure the env and script are set up correctly. 

```bash
sbatch scripts/exp12_read_ophys1.sh
sbatch scripts/exp12_read_ophys2.sh
sbatch scripts/exp12_read_ophys3.sh

./scripts/exp12_check_jobs.sh 20465176 read_logs scripts/exp12_configs_ophys1.txt ophys1
./scripts/exp12_check_jobs.sh 20467784 read_logs scripts/exp12_configs_ophys2.txt ophys2
./scripts/exp12_check_jobs.sh 20467966 read_logs scripts/exp12_configs_ophys3.txt ophys3
```

### Ecephys: Generating binary files (BIN) from the ecephys NWB files

This takes about 2 hours each, so we put it in a slurm script.

Run:
```bash
conda activate $HOME/envs/nwbio
cd /global/home/users/rly/scratch/nwb-io-evaluation
python scripts/exp12_nwb_to_binary.py data/sub-npI3_ses-20190421_behavior+ecephys.nwb ElectricalSeries raw_data/ecephys1.bin
python scripts/exp12_nwb_to_binary.py data/sub-440956_ses-20190207T120657_behavior+ecephys+image+ogen.nwb ElectricalSeries-1 raw_data/ecephys2.bin
python scripts/exp12_nwb_to_binary.py data/sub-Elgar_ses-2022-08-19_ecephys.nwb ElectricalSeriesAP raw_data/ecephys3.bin
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
h5ls -v write_output/ecephys2_unchunked.nwb/acquisition/ElectricalSeriesProbe00AP/data
h5ls -v write_output/ecephys3_unchunked.nwb/acquisition/ElectricalSeriesAP/data
```

### Generating ecephys files with different chunk shapes and compression settings

```bash
sbatch scripts/exp12_write_ecephys1.sh
sbatch scripts/exp12_write_ecephys2.sh
sbatch scripts/exp12_write_ecephys3.sh
```

Get h5py default chunk sizes (Config 91):
```bash
h5ls -v ecephys1_Config091.nwb/acquisition/ElectricalSeries/data
# Chunks:    {341774, 1} 683548 bytes

h5ls -v ecephys2_Config091.nwb/acquisition/ElectricalSeriesProbe00AP/data
# Chunks:    {224216, 1} 448432 bytes

h5ls -v ecephys3_Config091.nwb/acquisition/ElectricalSeriesAP/data
# Chunks:    {319058, 1} 638116 bytes
```

### Evaluating read times of ecephys files

Run sbatch with `#SBATCH --array=1` before doing the full array to make sure the env and script are set up correctly. 

```bash
sbatch scripts/exp12_read_ecephys1.sh
sbatch scripts/exp12_read_ecephys2.sh
sbatch scripts/exp12_read_ecephys3.sh
```