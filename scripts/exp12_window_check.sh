#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=8:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=16G
#SBATCH --export=NONE

# The window check (scripts/exp12_window_check.py) for one dataset:
#
#     sbatch --job-name=exp12_window_check_<label> \
#         --output=write_logs/window_check_<label>--%j-out.log \
#         --error=write_logs/window_check_<label>--%j-err.log \
#         scripts/exp12_window_check.sh <label>
#
# --export=NONE starts the job from a clean login environment wherever it was submitted (an Open
# OnDemand Jupyter session exports SLURM_EXPORT_ENV=NONE). It also makes srun default to
# --export=NONE, so srun passes --export=ALL to give the step this script's environment.

source ~/.bashrc
conda activate ~/envs/nwbio

set -euo pipefail

cd "$SCRATCH/nwb-io-evaluation"
LABEL="$1"
PYTHON=$(which python)  # Capture the full python path

echo "Running: srun --export=ALL $PYTHON scripts/exp12_window_check.py run $LABEL window_check/$LABEL.tsv"
srun --export=ALL "$PYTHON" scripts/exp12_window_check.py run "$LABEL" "window_check/$LABEL.tsv"
