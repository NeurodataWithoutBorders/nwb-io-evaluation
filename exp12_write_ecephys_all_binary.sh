#!/bin/bash
#SBATCH --qos=lr_normal
#SBATCH --account=pc_rlyneuro
#SBATCH --time=8:00:00
#SBATCH --nodes=1
#SBATCH --partition=lr7
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=32G
#SBATCH --job-name=exp12_write_ecephys_all_binary
#SBATCH --output=/global/scratch/users/rly/nwbio/exp12/write_logs/ecephys_all_binary--%A-out.log
#SBATCH --error=/global/scratch/users/rly/nwbio/exp12/write_logs/ecephys_all_binary--%A-err.log
#SBATCH --mail-user=rly@lbl.gov
#SBATCH --mail-type=ALL

source ~/.bashrc
conda activate ~/envs/nwbio

PYTHON=$(which python)  # Capture the full python path

srun $PYTHON exp12_nwb_to_binary.py ../../data/sub-npI3_ses-20190421_behavior+ecephys.nwb ElectricalSeries ../raw_data/ecephys1.bin
srun $PYTHON exp12_nwb_to_binary.py ../../data/sub-CSHL045_ses-034e726f-b35f-41e0-8d6c-a22cc32391fb_desc-raw_ecephys.nwb ElectricalSeriesProbe00AP ../raw_data/ecephys2.bin
srun $PYTHON exp12_nwb_to_binary.py ../../data/sub-Elgar_ses-2022-08-19_ecephys.nwb ElectricalSeriesAP ../raw_data/ecephys3.bin
