#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=1:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --job-name=exp12_write_ophys1_unchunked
#SBATCH --output=/global/scratch/users/rly/nwbio/exp12/write_logs/ophys1_unchunked--%A-out.log
#SBATCH --error=/global/scratch/users/rly/nwbio/exp12/write_logs/ophys1_unchunked--%A-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

FILEPATH="$SCRATCH/nwbio/data/sub-R6_ses-20200206T210000_behavior+ophys.nwb"
SERIESNAME="TwoPhotonSeries"
TIFFDIR="$SCRATCH/nwbio/exp12/raw_data/ophys1"
OUTFILEPATH="$SCRATCH/nwbio/exp12/write_output/ophys1_unchunked.nwb"

echo "Running: srun $PYTHON exp12_write_ophys_tiff_to_unchunked_nwb.py $FILEPATH $SERIESNAME $TIFFDIR $OUTFILEPATH"
srun $PYTHON exp12_write_ophys_tiff_to_unchunked_nwb.py $FILEPATH $SERIESNAME $TIFFDIR $OUTFILEPATH
