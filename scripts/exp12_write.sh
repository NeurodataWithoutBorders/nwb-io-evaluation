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

# One write from scripts/exp12_write_schedule.tsv:
#
#     sbatch scripts/exp12_write.sh <task>
#
# scripts/exp12_submit_writes.py submits the tasks and sets each job's name and log files.
#
# --export=NONE starts the job from a clean login environment wherever it was submitted (an Open
# OnDemand Jupyter session exports SLURM_EXPORT_ENV=NONE). It also makes srun default to
# --export=NONE, so srun passes --export=ALL to give the step this script's environment.

source ~/.bashrc
conda activate ~/envs/nwbio

set -euo pipefail

REPO="$SCRATCH/nwb-io-evaluation"
cd "$REPO"

TASK="$1"
SCHEDULE="scripts/exp12_write_schedule.tsv"
ENTRY=$(awk -F'\t' -v t="$TASK" 'NR > 1 && $1 == t {print $2, $3, $4}' "$SCHEDULE")
if [ -z "$ENTRY" ]; then
    echo "Task $TASK is not in $SCHEDULE" >&2
    exit 1
fi
read -r LABEL ROW CONFIG_ID <<< "$ENTRY"

case "$LABEL" in
    ecephys1)
        SOURCE="$REPO/data/sub-npI3_ses-20190421_behavior+ecephys.nwb"
        SERIES="ElectricalSeries"
        RAW="$REPO/raw_data/ecephys1.bin" ;;
    ecephys2)
        SOURCE="$REPO/data/sub-CSHL045_ses-034e726f-b35f-41e0-8d6c-a22cc32391fb_desc-raw_ecephys.nwb"
        SERIES="ElectricalSeriesProbe00AP"
        RAW="$REPO/raw_data/ecephys2_00ap.bin" ;;
    ecephys3)
        SOURCE="$REPO/data/sub-Elgar_ses-2022-08-19_ecephys.nwb"
        SERIES="ElectricalSeriesAP"
        RAW="$REPO/raw_data/ecephys3.bin" ;;
    ophys1)
        SOURCE="$REPO/data/sub-R6_ses-20200206T210000_behavior+ophys.nwb"
        SERIES="TwoPhotonSeries"
        RAW="$REPO/raw_data/ophys1" ;;
    ophys2)
        SOURCE="$REPO/data/sub-408021_ses-758519303_obj-raw_behavior+image+ophys.nwb"
        SERIES="motion_corrected_stack"
        RAW="$REPO/raw_data/ophys2" ;;
    ophys3)
        SOURCE="$REPO/data/sub-491604967_ses-496908818-StimB_ophys.nwb"
        SERIES="MotionCorrectedTwoPhotonSeries"
        RAW="$REPO/raw_data/ophys3" ;;
    *)
        echo "Unknown dataset label $LABEL in task $TASK" >&2
        exit 1 ;;
esac

OUTDIR="$REPO/write_output"
CONFIG="scripts/exp12_configs_$LABEL.tsv"

if [ "$CONFIG_ID" = "unchunked" ]; then
    case "$LABEL" in
        ecephys*) CMD=(scripts/exp12_write_ecephys_bin_to_unchunked_nwb.py "$SOURCE"
                       "$OUTDIR/${LABEL}_unchunked.nwb" "$SERIES" "$RAW") ;;
        ophys*) CMD=(scripts/exp12_write_ophys_tiff_to_unchunked_nwb.py "$SOURCE" "$SERIES" "$RAW"
                     "$OUTDIR/${LABEL}_unchunked.nwb") ;;
    esac
else
    TABLE_ID=$(awk -F'\t' -v r="$ROW" 'NR == r + 1 {print $1}' "$CONFIG")
    if [ "$TABLE_ID" != "$CONFIG_ID" ]; then
        echo "Row $ROW of $CONFIG is $TABLE_ID, but $SCHEDULE lists $CONFIG_ID for task $TASK." \
             "Regenerate the schedule with python scripts/exp12_write_schedule.py" >&2
        exit 1
    fi
    case "$LABEL" in
        ecephys*) CMD=(scripts/exp12_write_ecephys_bin_to_nwb.py "$CONFIG" "$ROW" "$SOURCE" "$LABEL"
                       "$OUTDIR" "$SERIES" "$RAW") ;;
        ophys*) CMD=(scripts/exp12_write_ophys_tiff_to_nwb.py "$CONFIG" "$ROW" "$SOURCE" "$SERIES" "$RAW"
                     "$LABEL" "$OUTDIR") ;;
    esac
fi

PYTHON=$(which python)  # Capture the full python path

echo "Task $TASK: $LABEL $CONFIG_ID"
echo "Running: srun --export=ALL $PYTHON ${CMD[*]}"
srun --export=ALL "$PYTHON" "${CMD[@]}"
