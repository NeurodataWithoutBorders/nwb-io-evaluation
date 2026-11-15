#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --export=NONE
#SBATCH --array=1-198
#SBATCH --job-name=exp12_write_ophys2
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ophys2--%A_%03a-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ophys2--%A_%03a-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

# --export=NONE starts the job from a clean login environment wherever it was submitted (an Open
# OnDemand Jupyter session exports SLURM_EXPORT_ENV=NONE). It also makes srun default to
# --export=NONE, so srun passes --export=ALL to give the step this script's environment.

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

CONFIG="scripts/exp12_configs_ophys2.tsv"
ROW="$SLURM_ARRAY_TASK_ID"
FILEPATH="$SCRATCH/nwb-io-evaluation/data/sub-408021_ses-758519303_obj-raw_behavior+image+ophys.nwb"
SERIESNAME="motion_corrected_stack"
TIFFDIR="$SCRATCH/nwb-io-evaluation/raw_data/ophys2"
OUTLABEL="ophys2"
OUTDIR="$SCRATCH/nwb-io-evaluation/write_output/"

echo "Running: srun --export=ALL $PYTHON scripts/exp12_write_ophys_tiff_to_nwb.py $CONFIG $ROW $FILEPATH $SERIESNAME $TIFFDIR $OUTLABEL $OUTDIR"
srun --export=ALL $PYTHON scripts/exp12_write_ophys_tiff_to_nwb.py $CONFIG $ROW $FILEPATH $SERIESNAME $TIFFDIR $OUTLABEL $OUTDIR