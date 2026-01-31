#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=3:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --array=1-126
#SBATCH --job-name=exp12_read_ophys1
#SBATCH --output=/global/scratch/users/rly/nwbio/exp12/read_logs/ophys1--%A_%03a-out.log
#SBATCH --error=/global/scratch/users/rly/nwbio/exp12/read_logs/ophys1--%A_%03a-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

CONFIGNUM="$SLURM_ARRAY_TASK_ID"
INPUTDIR="$SCRATCH/nwbio/exp12/write_output/"
SERIESNAME="TwoPhotonSeries"
OUTLABEL="ophys1"
OUTDIR="$SCRATCH/nwbio/exp12/read_output/"

echo "Running: srun $PYTHON exp12_read_ophys.py $CONFIGNUM $INPUTDIR $SERIESNAME $OUTLABEL $OUTDIR"
srun $PYTHON exp12_read_ophys.py $CONFIGNUM $INPUTDIR $SERIESNAME $OUTLABEL $OUTDIR
