#!/bin/bash
# Rename files to use 3-digit zero-padded config numbers
# e.g., exp12_ophys1_Config1.nwb -> exp12_ophys1_Config001.nwb

if [ $# -eq 0 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

dir="$1"

for file in "$dir"/exp12_*_Config[0-9]*.nwb "$dir"/stats_exp12_*_Config[0-9]*.txt; do
    [ -e "$file" ] || continue

    # Extract the config number from the filename
    if [[ "$file" =~ Config([0-9]+)\.(nwb|txt)$ ]]; then
        config_num="${BASH_REMATCH[1]}"
        extension="${BASH_REMATCH[2]}"

        # Skip if already 3 digits
        if [ ${#config_num} -eq 3 ]; then
            continue
        fi

        # Zero-pad to 3 digits
        new_config_num=$(printf "%03d" "$config_num")

        # Build new filename
        new_file="${file/Config${config_num}.${extension}/Config${new_config_num}.${extension}}"

        echo "Renaming: $(basename "$file") -> $(basename "$new_file")"
        mv "$file" "$new_file"
    fi
done
