#!/bin/bash
# Report results from an sbatch array job in a table format.
# Usage: ./exp12_check_jobs.sh <job_id> <log_folder> <config_file> <experiment_name>
# Example: ./exp12_check_jobs.sh 20463593 write_logs exp12_configs_ophys1.txt ophys1

if [ $# -ne 4 ]; then
    echo "Usage: $0 <job_id> <log_folder> <config_file> <experiment_name>"
    exit 1
fi

JOB_ID=$1
LOG_FOLDER=$2
CONFIG_FILE=$3
EXPERIMENT=$4

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Count number of configs (non-header lines)
NUM_CONFIGS=$(tail -n +2 "$CONFIG_FILE" | grep -c .)

# Read config file into array (skip header)
mapfile -t CONFIG_LINES < <(tail -n +2 "$CONFIG_FILE")

# Print header
printf "%-6s %-15s %-14s %-12s %-10s %-12s %s\n" "Task" "Chunk" "Compression" "Level" "Status" "Duration" "Errors"
printf "%-6s %-15s %-14s %-12s %-10s %-12s %s\n" "----" "-----" "-----------" "-----" "------" "--------" "------"

FAILED_TASKS=()
RUNNING_TASKS=()

for i in $(seq 1 $NUM_CONFIGS); do
    PADDED=$(printf "%03d" $i)
    OUT_FILE="${LOG_FOLDER}/${EXPERIMENT}--${JOB_ID}_${PADDED}-out.log"
    ERR_FILE="${LOG_FOLDER}/${EXPERIMENT}--${JOB_ID}_${PADDED}-err.log"

    # Get config info (0-indexed array, so use i-1)
    CONFIG_LINE="${CONFIG_LINES[$((i-1))]}"
    CHUNK=$(echo "$CONFIG_LINE" | awk '{print $1}')
    COMPR=$(echo "$CONFIG_LINE" | awk '{print $2}')
    LEVEL=$(echo "$CONFIG_LINE" | awk '{print $5}')

    # Get job status and duration from sacct
    SACCT_OUTPUT=$(sacct -j "${JOB_ID}_${i}" --format=State,Elapsed --noheader 2>/dev/null | head -1)
    STATUS=$(echo "$SACCT_OUTPUT" | awk '{print $1}')
    DURATION=$(echo "$SACCT_OUTPUT" | awk '{print $2}')

    if [ -z "$STATUS" ]; then
        STATUS="UNKNOWN"
    fi
    if [ -z "$DURATION" ]; then
        DURATION="-"
    fi

    if [ "$STATUS" = "RUNNING" ] || [ "$STATUS" = "PENDING" ]; then
        RUNNING_TASKS+=($i)
    elif [ "$STATUS" != "COMPLETED" ]; then
        FAILED_TASKS+=($i)
    fi

    # Get error log contents, filtering out the HDF5 warning
    ERRORS=""
    if [ -f "$ERR_FILE" ]; then
        FILTERED=$(grep -v "compression may not be available on all installations of HDF5" "$ERR_FILE" | grep -v "data = H5DataIO" | head -1)
        if [ -n "$FILTERED" ]; then
            # Truncate to 50 chars
            ERRORS="${FILTERED:0:50}"
        fi
    fi

    # Set color based on status
    if [ "$STATUS" = "COMPLETED" ]; then
        COLOR=$GREEN
    elif [ "$STATUS" = "RUNNING" ] || [ "$STATUS" = "PENDING" ]; then
        COLOR=$YELLOW
    else
        COLOR=$RED
    fi

    printf "${COLOR}%-6s %-15s %-14s %-12s %-10s %-12s %s${NC}\n" "$i" "$CHUNK" "$COMPR" "$LEVEL" "$STATUS" "$DURATION" "$ERRORS"
done

# Print summary
echo ""
COMPLETED_COUNT=$(($NUM_CONFIGS - ${#FAILED_TASKS[@]} - ${#RUNNING_TASKS[@]}))
echo "Summary: ${COMPLETED_COUNT}/${NUM_CONFIGS} completed"
if [ ${#RUNNING_TASKS[@]} -gt 0 ]; then
    echo "Running/Pending tasks: ${RUNNING_TASKS[*]}"
fi
if [ ${#FAILED_TASKS[@]} -gt 0 ]; then
    echo "Failed tasks: ${FAILED_TASKS[*]}"
fi
