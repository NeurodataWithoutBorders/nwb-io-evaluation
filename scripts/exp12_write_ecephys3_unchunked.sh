#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=1:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --job-name=exp12_write_ecephys3_unchunked
#SBATCH --output=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ecephys3_unchunked--%A-out.log
#SBATCH --error=/global/scratch/users/rly/nwb-io-evaluation/write_logs/ecephys3_unchunked--%A-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

FILEPATH="$SCRATCH/nwb-io-evaluation/data/sub-Elgar_ses-2022-08-19_ecephys.nwb"
OUTFILEPATH="$SCRATCH/nwb-io-evaluation/write_output/ecephys3_unchunked.nwb"

echo "Running: srun $PYTHON scripts/exp12_write_ecephys_bin_to_unchunked_nwb.py $FILEPATH $OUTFILEPATH ElectricalSeriesAP $SCRATCH/nwb-io-evaluation/raw_data/ecephys3.bin"
srun $PYTHON scripts/exp12_write_ecephys_bin_to_unchunked_nwb.py $FILEPATH $OUTFILEPATH ElectricalSeriesAP $SCRATCH/nwb-io-evaluation/raw_data/ecephys3.bin
