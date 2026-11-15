#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=3-00:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2G
#SBATCH --export=NONE
#SBATCH --job-name=exp12_submit_writes
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/write_logs/submit_writes--%j-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/write_logs/submit_writes--%j-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=END,FAIL

# Submits the writes of one modality in scripts/write/exp12_write_schedule.tsv under the job cap (see
# scripts/write/exp12_submit_writes.py). Arguments are passed to its submit command, for example
#     sbatch scripts/write/exp12_submit_writes.sh ecephys
#     sbatch scripts/write/exp12_submit_writes.sh ophys --max-jobs 90
#
# --export=NONE starts the job from a clean login environment wherever it was submitted (an Open
# OnDemand Jupyter session exports SLURM_EXPORT_ENV=NONE). It also makes srun default to
# --export=NONE, so srun passes --export=ALL to give the step this script's environment.

source ~/.bashrc
conda activate ~/envs/nwbio

cd "$SCRATCH/nwb-io-evaluation"

PYTHON=$(which python)  # Capture the full python path

echo "Running: srun --export=ALL $PYTHON scripts/write/exp12_submit_writes.py submit $*"
srun --export=ALL "$PYTHON" scripts/write/exp12_submit_writes.py submit "$@"
