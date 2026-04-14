#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=71:59:59
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --array=1-91
#SBATCH --job-name=exp12_read_ecephys1
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/read_logs/ecephys1--%A_%03a-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/read_logs/ecephys1--%A_%03a-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

ARRAYTASKID="$SLURM_ARRAY_TASK_ID"
INPUTDIR="$SCRATCH/nwb-io-evaluation/write_output/"
SERIESNAME="ElectricalSeries"
OUTLABEL="ecephys1"
OUTDIR="$SCRATCH/nwb-io-evaluation/read_output/"
USECASE="${USECASE:-all}"  # submit per-pattern with e.g. --export=ALL,USECASE=e2

echo "Running: srun $PYTHON scripts/exp12_read_ecephys.py $ARRAYTASKID $INPUTDIR $SERIESNAME $OUTLABEL $OUTDIR $USECASE"
srun $PYTHON scripts/exp12_read_ecephys.py $ARRAYTASKID $INPUTDIR $SERIESNAME $OUTLABEL $OUTDIR $USECASE
