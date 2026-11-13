#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=8:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --array=1-91
#SBATCH --job-name=exp12_write_ophys3
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ophys3--%A_%03a-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ophys3--%A_%03a-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

CONFIG="exp12_configs_ophys3.txt"
JOB="$SLURM_ARRAY_TASK_ID"
FILEPATH="$SCRATCH/nwb-io-evaluation/data/sub-491604967_ses-496908818-StimB_ophys.nwb"
SERIESNAME="MotionCorrectedTwoPhotonSeries"
TIFFDIR="$SCRATCH/nwb-io-evaluation/raw_data/ophys3"
OUTLABEL="ophys3"
OUTDIR="$SCRATCH/nwb-io-evaluation/write_output/"

echo "Running: srun $PYTHON scripts/exp12_write_ophys_tiff_to_nwb.py $CONFIG $JOB $FILEPATH $SERIESNAME $TIFFDIR $OUTLABEL $OUTDIR"
srun $PYTHON scripts/exp12_write_ophys_tiff_to_nwb.py $CONFIG $JOB $FILEPATH $SERIESNAME $TIFFDIR $OUTLABEL $OUTDIR