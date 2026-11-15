"""
Placing the benchmarked series in a test NWB file.

Every test file holds the benchmarked series under one name per modality, ElectricalSeries for
ecephys and TwoPhotonSeries for ophys, whatever its name in the source file. The new series takes
the place of the source series in /acquisition, and the test file leaves out two kinds of series:

- other series of the same type (for example a second probe or an LF band), which export copies
  in full from the source file, adding to the write time and file size;
- series whose timestamps or data link to the source series, whose links point at a path that is
  not in the test file.
"""

from typing import Any

ECEPHYS_SERIES_NAME = "ElectricalSeries"
OPHYS_SERIES_NAME = "TwoPhotonSeries"


def replace_series(nwbfile: Any, source_name: str, new_series: Any) -> None:
    """Put new_series in /acquisition in place of the series named source_name."""
    source = nwbfile.acquisition[source_name]
    for dependent in set(source.timestamp_link or ()) | set(source.data_link or ()):
        if nwbfile.acquisition.get(dependent.name) is not dependent:
            raise ValueError(
                f"{dependent.name} links to the timestamps or data of {source_name} and is not in "
                "/acquisition, so it cannot be left out of the test file"
            )
        print(f"Leaving out {dependent.name}, which links to {source_name}")
        nwbfile.acquisition.pop(dependent.name)
    nwbfile.acquisition.pop(source_name)
    for name, obj in list(nwbfile.acquisition.items()):
        if isinstance(obj, type(new_series)):
            print(f"Leaving out {name}")
            nwbfile.acquisition.pop(name)
    nwbfile.add_acquisition(new_series)
