#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=2:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --export=NONE
#SBATCH --job-name=exp12_write_ophys1_unchunked
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ophys1_unchunked--%A-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ophys1_unchunked--%A-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

# --export=NONE starts the job from a clean login environment wherever it was submitted (an Open
# OnDemand Jupyter session exports SLURM_EXPORT_ENV=NONE). It also makes srun default to
# --export=NONE, so srun passes --export=ALL to give the step this script's environment.

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

FILEPATH="$SCRATCH/nwb-io-evaluation/data/sub-R6_ses-20200206T210000_behavior+ophys.nwb"
SERIESNAME="TwoPhotonSeries"
TIFFDIR="$SCRATCH/nwb-io-evaluation/raw_data/ophys1"
OUTFILEPATH="$SCRATCH/nwb-io-evaluation/write_output/ophys1_unchunked.nwb"

echo "Running: srun --export=ALL $PYTHON scripts/exp12_write_ophys_tiff_to_unchunked_nwb.py $FILEPATH $SERIESNAME $TIFFDIR $OUTFILEPATH"
srun --export=ALL $PYTHON scripts/exp12_write_ophys_tiff_to_unchunked_nwb.py $FILEPATH $SERIESNAME $TIFFDIR $OUTFILEPATH
