import h5py
import numpy as np
import os
import re

ERROR_LOG = "h5_errors.log"
INPUT_DIR = "read_ophys2_Configs"
OUTPUT_FILE = "h5_summary_Ophys2.txt"

TARGET_KEYS = ["repeat_total_times", "read_times", "location_total_times"]

STAT_ORDER = ("median", "min", "max")


def compute_stats(arr):
    arr = np.ravel(arr)
    return np.median(arr), np.min(arr), np.max(arr)


def column_sort_key(col):
    for idx, stat in enumerate(STAT_ORDER):
        suffix = f"_{stat}"
        if col.endswith(suffix):
            return (idx, col[: -len(suffix)])
    return (len(STAT_ORDER), col)


def extract_config_no_and_use_case(filename):
    """
    Extract config number and optional use case suffix from a read-result filename.
    Examples:
        read_ophys1_Config042.h5       -> ("042", "")
        read_ophys1_Config042_o2.h5    -> ("042", "o2")
        read_ophys1_Config042_all.h5   -> ("042", "all")
    """
    match = re.search(r'Config(\d+)(?:_([a-z0-9]+))?\.', filename)
    if not match:
        return filename, ""
    return match.group(1), match.group(2) or ""


def traverse_groups(h5obj, path=""):
    """
    Recursively yield (group_path, group_object)
    """
    for key in h5obj.keys():
        item = h5obj[key]
        new_path = f"{path}/{key}" if path else key

        if isinstance(item, h5py.Group):
            yield new_path, item
            yield from traverse_groups(item, new_path)


def process_file(filepath, target_keys, error_log_handle):
    result = {}

    try:
        # Try more tolerant open (helps with some NWB/HDF5 cases)
        with h5py.File(filepath, 'r') as f:

            for group_path, group in traverse_groups(f):

                for key in target_keys:
                    if key in group:
                        try:
                            data = group[key][()]
                            median, minimum, maximum = compute_stats(data)

                            result[f"{group_path}_{key}_median"] = median
                            result[f"{group_path}_{key}_min"] = minimum
                            result[f"{group_path}_{key}_max"] = maximum

                        except Exception as inner_e:
                            msg = f"Dataset error in {filepath} :: {group_path}/{key} :: {inner_e}\n"
                            print(f"{msg.strip()}")
                            error_log_handle.write(msg)

    except Exception as e:
        msg = f"FILE ERROR: {filepath} :: {e}\n"
        print(f" {msg.strip()}")
        error_log_handle.write(msg)
        return None  # skip entire file

    return result


def main():

    all_rows = []
    all_columns = set()

    files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith(".h5")])

    with open(ERROR_LOG, "w") as elog:

        for fname in files:
            filepath = os.path.join(INPUT_DIR, fname)

            print(f"Processing: {fname}")

            row = {}

            config_no, use_case = extract_config_no_and_use_case(fname)
            row["config_no"] = config_no
            row["use_case"] = use_case

            stats = process_file(filepath, TARGET_KEYS, elog)

            if stats is None:
                # skip bad file but keep record
                continue

            row.update(stats)

            all_rows.append(row)
            all_columns.update(row.keys())

    # Ensure consistent column order
    leading = ["config_no", "use_case"]
    all_columns = leading + sorted(
        (c for c in all_columns if c not in leading), key=column_sort_key
    )

    # Write final output
    with open(OUTPUT_FILE, "w") as out:

        # Header
        out.write(",".join(all_columns) + "\n")

        # Rows
        for row in all_rows:
            values = [str(row.get(col, "")) for col in all_columns]
            out.write(",".join(values) + "\n")

    print(f"\n Done.")
    print(f" Output: {OUTPUT_FILE}")
    print(f" Errors logged in: {ERROR_LOG}")


if __name__ == "__main__":
    main()

