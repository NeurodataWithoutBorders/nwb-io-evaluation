"""
Placing the benchmarked series in a test NWB file.

Every test file holds the benchmarked series under one name per modality, ElectricalSeries for
ecephys and TwoPhotonSeries for ophys, whatever its name in the source file. The new series takes
the place of the source series in /acquisition, and the test file leaves out two kinds of series:

- other series of the same type, in /acquisition or in a processing module (for example a second
  probe, an LF band, or LFP), which export copies in full from the source file, adding to the
  write time and file size. A processing module interface that holds such series (for example
  LFP) is left out whole;
- series in /acquisition whose timestamps or data link to a series that is left out, whose links
  would point at a path that is not in the test file.
"""

from typing import Any

ECEPHYS_SERIES_NAME = "ElectricalSeries"
OPHYS_SERIES_NAME = "TwoPhotonSeries"


def _check_dependents(nwbfile: Any, series: Any, removed: set[int]) -> list[str]:
    """Names of the /acquisition series that link to the timestamps or data of series.

    Raises ValueError if a series that links to it is neither in /acquisition nor already left out.
    """
    names = []
    for dependent in set(series.timestamp_link or ()) | set(series.data_link or ()):
        if id(dependent) in removed:
            continue
        if nwbfile.acquisition.get(dependent.name) is not dependent:
            raise ValueError(
                f"{dependent.name} links to the timestamps or data of {series.name} and is not in "
                "/acquisition, so it cannot be left out of the test file"
            )
        names.append(dependent.name)
    return names


def replace_series(nwbfile: Any, source_name: str, new_series: Any) -> None:
    """Put new_series in /acquisition in place of the series named source_name."""
    series_type = type(new_series)
    removed: set[int] = set()

    def drop_acquisition(name: str, reason: str) -> None:
        series = nwbfile.acquisition.pop(name)
        removed.add(id(series))
        print(f"Leaving out {name}{reason}")
        for dependent in _check_dependents(nwbfile, series, removed):
            drop_acquisition(dependent, f", which links to {name}")

    drop_acquisition(source_name, ", the source series")
    for name, obj in list(nwbfile.acquisition.items()):
        # A series dropped earlier in this loop as a dependent is no longer in /acquisition
        if name in nwbfile.acquisition and isinstance(obj, series_type):
            drop_acquisition(name, "")

    for module in nwbfile.processing.values():
        for name, interface in list(module.data_interfaces.items()):
            members = [interface] if isinstance(interface, series_type) else list(interface.children)
            series = [m for m in members if isinstance(m, series_type)]
            if not series:
                continue
            if len(series) != len(members):
                raise ValueError(
                    f"{module.name}/{name} holds {series_type.__name__} and other objects, so it cannot "
                    "be left out of the test file whole"
                )
            module.data_interfaces.pop(name)
            print(f"Leaving out {module.name}/{name}")
            for s in series:
                removed.add(id(s))
            for s in series:
                for dependent in _check_dependents(nwbfile, s, removed):
                    drop_acquisition(dependent, f", which links to {module.name}/{name}")

    nwbfile.add_acquisition(new_series)
