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

# One read from scripts/exp12_read_schedule.tsv:
#
#     sbatch scripts/exp12_read.sh <task>
#
# scripts/exp12_submit_reads.py submits the tasks and sets each job's name and log files.
#
# --export=NONE starts the job from a clean login environment wherever it was submitted (an Open
# OnDemand Jupyter session exports SLURM_EXPORT_ENV=NONE). It also makes srun default to
# --export=NONE, so srun passes --export=ALL to give the step this script's environment.

source ~/.bashrc
conda activate ~/envs/nwbio

set -euo pipefail

cd "$SCRATCH/nwb-io-evaluation"

TASK="$1"
PYTHON=$(which python)  # Capture the full python path

echo "Running: srun --export=ALL $PYTHON scripts/exp12_read.py $TASK"
srun --export=ALL "$PYTHON" scripts/exp12_read.py "$TASK"
