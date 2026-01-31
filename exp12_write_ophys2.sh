#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=8:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --array=1-126
#SBATCH --job-name=exp12_write_ophys2
#SBATCH --output=/global/scratch/users/rly/nwbio/exp12/write_logs/ophys2--%A_%03a-out.log
#SBATCH --error=/global/scratch/users/rly/nwbio/exp12/write_logs/ophys2--%A_%03a-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

CONFIG="exp12_configs_ophys2.txt"
JOB="$SLURM_ARRAY_TASK_ID"
FILEPATH="$SCRATCH/nwbio/data/sub-408021_ses-758519303_obj-raw_behavior+image+ophys.nwb"
SERIESNAME="motion_corrected_stack"
TIFFDIR="$SCRATCH/nwbio/exp12/raw_data/ophys2"
OUTLABEL="ophys2"
OUTDIR="$SCRATCH/nwbio/exp12/write_output/"

echo "Running: srun $PYTHON exp12_write_ophys_tiff_to_nwb.py $CONFIG $JOB $FILEPATH $SERIESNAME $TIFFDIR $OUTLABEL $OUTDIR"
srun $PYTHON exp12_write_ophys_tiff_to_nwb.py $CONFIG $JOB $FILEPATH $SERIESNAME $TIFFDIR $OUTLABEL $OUTDIR