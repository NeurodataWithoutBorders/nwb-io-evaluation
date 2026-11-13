#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=1:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --job-name=exp12_write_ecephys2_unchunked
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ecephys2_unchunked--%A-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ecephys2_unchunked--%A-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

FILEPATH="$SCRATCH/nwb-io-evaluation/data/sub-CSHL045_ses-034e726f-b35f-41e0-8d6c-a22cc32391fb_desc-raw_ecephys.nwb"
OUTFILEPATH="$SCRATCH/nwb-io-evaluation/write_output/ecephys2_unchunked.nwb"
RAWDIR="$SCRATCH/nwb-io-evaluation/raw_data"

echo "Running: srun $PYTHON scripts/exp12_write_ecephys_bin_to_unchunked_nwb.py $FILEPATH $OUTFILEPATH (4 series)"
srun $PYTHON scripts/exp12_write_ecephys_bin_to_unchunked_nwb.py $FILEPATH $OUTFILEPATH \
    ElectricalSeriesProbe00AP $RAWDIR/ecephys2_00ap.bin \
    ElectricalSeriesProbe00LF $RAWDIR/ecephys2_00lf.bin \
    ElectricalSeriesProbe01AP $RAWDIR/ecephys2_01ap.bin \
    ElectricalSeriesProbe01LF $RAWDIR/ecephys2_01lf.bin
