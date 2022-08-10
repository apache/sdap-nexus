import pytest


def pytest_addoption(parser):
    parser.addoption("--skip-matchup", action="store_true")
    parser.addoption("--force-subset", action="store_true")

def pytest_collection_modifyitems(config, items):
    skip_matchup = config.getoption("--skip-matchup")

    if skip_matchup:
        skip = pytest.mark.skip(reason="Manually skipped")
        for item in items:
            if "matchup_spark" in item.name:
                item.add_marker(skip)

    force = config.getoption("--force-subset")

    if not force:
        skip = pytest.mark.skip(reason="Waiting for Zarr integration before this case is run")
        for item in items:
            if "cdmssubset" in item.name:
                item.add_marker(skip)
