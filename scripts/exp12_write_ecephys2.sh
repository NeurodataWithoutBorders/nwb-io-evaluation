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
#SBATCH --job-name=exp12_write_ecephys2
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ecephys2--%A_%03a-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ecephys2--%A_%03a-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

CONFIG="exp12_configs_ecephys.txt"
JOB="$SLURM_ARRAY_TASK_ID"
FILEPATH="$SCRATCH/nwb-io-evaluation/data/sub-CSHL045_ses-034e726f-b35f-41e0-8d6c-a22cc32391fb_desc-raw_ecephys.nwb"
OUTLABEL="ecephys2"
OUTDIR="$SCRATCH/nwb-io-evaluation/write_output/"
RAWDIR="$SCRATCH/nwb-io-evaluation/raw_data"

echo "Running: srun $PYTHON scripts/exp12_write_ecephys_bin_to_nwb.py $CONFIG $JOB $FILEPATH $OUTLABEL $OUTDIR (4 series)"
srun $PYTHON scripts/exp12_write_ecephys_bin_to_nwb.py $CONFIG $JOB $FILEPATH $OUTLABEL $OUTDIR \
    ElectricalSeriesProbe00AP $RAWDIR/ecephys2_00ap.bin \
    ElectricalSeriesProbe00LF $RAWDIR/ecephys2_00lf.bin \
    ElectricalSeriesProbe01AP $RAWDIR/ecephys2_01ap.bin \
    ElectricalSeriesProbe01LF $RAWDIR/ecephys2_01lf.bin
