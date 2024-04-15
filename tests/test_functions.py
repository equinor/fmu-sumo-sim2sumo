"""Test utility ecl2csv"""

import sys
import os
from time import sleep
from datetime import datetime
from subprocess import Popen, PIPE
import logging
import yaml
from pathlib import Path
import shutil
import pandas as pd
import pyarrow as pa
import pytest
from sumo.wrapper import SumoClient
from fmu.sumo.sim2sumo.common import fix_suffix
from fmu.sumo.sim2sumo import tables
from fmu.sumo.sim2sumo import grid3d
from fmu.sumo.sim2sumo._special_treatments import (
    _define_submodules,
    convert_to_arrow,
)
from xtgeo import Grid


REEK_ROOT = Path(__file__).parent / "data/reek"
REAL_PATH = "realization-0/iter-0/"
REEK_REAL0 = REEK_ROOT / "realization-0/iter-0/"
REEK_REAL1 = REEK_ROOT / "realization-1/iter-0/"
REEK_BASE = "2_R001_REEK"
REEK_ECL_MODEL = REEK_REAL0 / "eclipse/model/"
REEK_DATA_FILE = REEK_ECL_MODEL / f"{REEK_BASE}-0.DATA"
CONFIG_OUT_PATH = REEK_REAL0 / "fmuconfig/output/"
CONFIG_PATH = CONFIG_OUT_PATH / "global_variables.yml"


logging.basicConfig(
    level=logging.info, format=" %(name)s :: %(levelname)s :: %(message)s"
)
LOGGER = logging.getLogger(__file__)


def check_sumo(case_uuid, tag_prefix, correct, class_type, sumo):
    # There has been instances when this fails, probably because of
    # some time delay, have introduced a little sleep to get it to be quicker
    sleep(2)
    query = f'fmu.case.uuid:{case_uuid} AND class:{class_type} AND data.tagname:"{tag_prefix}*"'

    print(query)
    results = sumo.get(
        "/search",
        {
            "$query": query,
            "$size": 0,
        },
    ).json()
    LOGGER.debug(results["hits"])
    returned = results["hits"]["total"]["value"]
    LOGGER.debug("This is returned %s", returned)
    assert (
        returned == correct
    ), f"Supposed to upload {correct}, but actual were {returned}"

    # delete_objects(case_uuid, sumo, tag_prefix)


def write_ert_config_and_run(runpath):
    ert_config_path = "sim2sumo.ert"
    encoding = "utf-8"
    with open(runpath / ert_config_path, "w", encoding=encoding) as stream:

        stream.write(f"RUNPATH {runpath}\nFORWARD_MODEL SIM2SUMO")
    process = Popen(
        ["ert", "test_run", ert_config_path], stdout=PIPE, stderr=PIPE
    )
    stdout, stderr = process.communicate()
    if stdout:
        print(stdout.decode(encoding))
    if stderr:
        print(stderr.decode(encoding))
    assert Path(
        "OK"
    ).is_file(), f"running {ert_config_path}, ended with errors"


def delete_objects(case_uuid, sumo, tag_prefix):

    sumo.delete(
        "/objects('{objectid}')/children",
        {"objectid": case_uuid, "$filter": f'data.tagname":"{tag_prefix}*"'},
    )
    sleep(2)


def _assert_right_len(checks, key, to_messure, name):
    """Assert length when reading config

    Args:
        checks (dict): the answers
        key (str): the answer to check
        to_messure (list): the generated answer
        name (str): name of the file to check against
    """
    # Helper for test_read_config
    right_len = checks[key]
    actual_len = len(to_messure)
    assert (
        actual_len == right_len
    ), f"For {name}-{key} actual length is {actual_len}, but should be {right_len}"


def check_expected_exports(expected_exports, shared_grid, prefix):
    print("Looking in ", shared_grid)
    parameters = list(shared_grid.glob(f"*--{prefix.lower()}-*.roff"))
    print(parameters)
    meta = list(shared_grid.glob(f"*--{prefix.lower()}-*.roff.yml"))
    nr_parameter = len(parameters)
    nr_meta = len(meta)
    assert nr_parameter == nr_meta
    assert (
        nr_parameter == expected_exports
    ), f"exported {nr_parameter} params, should be {expected_exports}"
    assert (
        nr_meta == expected_exports
    ), f"exported {nr_meta} metadata objects, should be {expected_exports}"


def test_fix_suffix():

    test_path = "simulator.banana"
    corrected_path = fix_suffix(test_path)
    assert corrected_path.endswith(".DATA"), f"Didn't correct {corrected_path}"


@pytest.mark.parametrize("real,nrdfiles", [(REEK_REAL0, 6), (REEK_REAL1, 1)])
def test_find_datafiles_reek(real, nrdfiles):
    os.chdir(real)
    datafiles = tables.find_datafiles(None, {})
    expected_tools = ["eclipse", "opm", "ix", "pflotran"]
    assert (
        len(datafiles) == nrdfiles
    ), f"Haven't found correct nr of datafiles {nrdfiles} files but {len(datafiles)} ({datafiles})"
    for datafile in datafiles:
        found_path = datafile
        parent = found_path.parent.parent.name
        assert parent in expected_tools, f"|{parent}| not in {expected_tools}"
        correct_suff = ".DATA"
        if parent == "ix":
            correct_suff = ".afi"
        if parent == "pflotran":
            correct_suff = ".in"
        assert found_path.suffix == correct_suff


def test_submodules_dict():
    """Test generation of submodule list"""
    sublist, submods = _define_submodules()
    LOGGER.info(submods)
    assert isinstance(sublist, tuple)
    assert isinstance(submods, dict)
    for submod_name, submod_dict in submods.items():
        LOGGER.info(submod_name)
        LOGGER.info(submod_dict)
        assert isinstance(submod_name, str)
        assert (
            "/" not in submod_name
        ), f"Left part of folder path for {submod_name}"
        assert isinstance(submod_dict, dict), f"{submod_name} has no subdict"
        assert (
            "options" in submod_dict.keys()
        ), f"{submod_name} does not have any options"

        assert isinstance(
            submod_dict["options"], tuple
        ), f"options for {submod_name} not tuple"


@pytest.mark.parametrize(
    "submod",
    (name for name in tables.SUBMODULES if name != "wellcompletiondata"),
)
# Skipping wellcompletion data, since this needs zonemap, which none of the others do
def test_get_results(submod):
    """Test fetching of dataframe"""
    extras = {}
    if submod == "wellcompletiondata":
        extras["zonemap"] = "data/reek/zones.lyr"
    frame = tables.get_results(REEK_DATA_FILE, submod)
    assert isinstance(
        frame, pa.Table
    ), f"Call for get_dataframe should produce dataframe, but produces {type(frame)}"
    frame = tables.get_results(REEK_DATA_FILE, submod, arrow=True)
    assert isinstance(
        frame, pa.Table
    ), f"Call for get_dataframe with arrow=True should produce pa.Table, but produces {type(frame)}"
    if submod == "summary":
        assert (
            frame.schema.field("FOPT").metadata is not None
        ), "Metdata not carried across for summary"


@pytest.mark.parametrize(
    "submod",
    (name for name in tables.SUBMODULES if name != "wellcompletiondata"),
)
def test_export_results(tmp_path, submod):
    """Test writing of csv file"""
    os.chdir(tmp_path)
    export_path = (
        tmp_path / f"share/results/tables/{REEK_BASE}--{submod}.arrow".lower()
    )
    meta_path = export_path.parent / f".{export_path.name}.yml"
    actual_path = tables.export_results(
        REEK_DATA_FILE,
        submod,
        CONFIG_PATH,
    )
    LOGGER.info(actual_path)
    assert isinstance(
        actual_path,
        str,
    ), "No string returned for path"
    assert export_path.exists(), f"No export of data to {export_path}"
    assert meta_path.exists(), f"No export of metadata to {meta_path}"


def test_export_results_w_options(tmp_path, submod="summary"):
    """Test writing of csv file"""
    os.chdir(tmp_path)
    export_path = (
        tmp_path / f"share/results/tables/{REEK_BASE}--{submod}.arrow".lower()
    )
    key_args = {
        "time_index": "daily",
        "start_date": "2002-01-02",
        "end_date": "2003-01-02",
    }

    meta_path = export_path.parent / f".{export_path.name}.yml"
    actual_path = tables.export_results(
        REEK_DATA_FILE, submod, CONFIG_PATH, **key_args
    )
    LOGGER.info(actual_path)
    assert isinstance(
        actual_path,
        str,
    ), "No string returned for path"
    assert export_path.exists(), f"No export of data to {export_path}"
    assert "arrow" in str(
        export_path
    ), f"No arrow in path, should be, path is {export_path}"
    assert meta_path.exists(), f"No export of metadata to {meta_path}"


# Extra checks to be used with parametrize below
CHECK_DICT = {
    "global_variables_w_eclpath.yml": {
        "nrdatafile": 1,
        "nrsubmods": 3,
        "nroptions": 1,
        "arrow": True,
    },
    "global_variables_w_eclpath_and_extras.yml": {
        "nrdatafile": 1,
        "nrsubmods": 3,
        "nroptions": 4,
        "arrow": False,
    },
    "global_variables.yml": {
        "nrdatafile": 6,
        "nrsubmods": 3,
        "nroptions": 1,
        "arrow": True,
    },
}


@pytest.mark.parametrize("config_path", CONFIG_OUT_PATH.glob("*.yml"))
def test_read_config(config_path):
    """Test reading of config file via read_config function"""
    os.chdir(REEK_REAL0)
    LOGGER.info(config_path)
    config = tables.yaml_load(config_path)
    assert isinstance(config, (dict, bool))
    dfiles, submods, opts = tables.read_config(config)
    name = config_path.name
    checks = CHECK_DICT[name]
    LOGGER.info(config)
    LOGGER.info(dfiles)
    LOGGER.info(submods)
    LOGGER.info(opts)
    _assert_right_len(checks, "nrdatafile", dfiles, name)
    _assert_right_len(checks, "nrsubmods", submods, name)
    _assert_right_len(checks, "nroptions", opts, name)

    assert (
        opts["arrow"] == checks["arrow"]
    ), f"Wrong choice for arrow for {name}"


@pytest.mark.parametrize("config_path", CONFIG_OUT_PATH.glob("*.yml"))
def test_export_w_config(tmp_path, config_path):
    """Test function export with config"""
    # Make exec path, needs to be at real..-0/iter-0
    exec_path = tmp_path / REAL_PATH
    exec_path.mkdir(parents=True)
    # Symlink in case meta at root of run
    case_share_meta = "share/metadata/"
    (tmp_path / case_share_meta).mkdir(parents=True)
    case_meta_path = "share/metadata/fmu_case.yml"
    (tmp_path / case_meta_path).symlink_to(REEK_ROOT / case_meta_path)
    # Run tests from exec path to get metadata in ship shape
    os.chdir(exec_path)
    # The lines below is needed for test to work when definition of datafile
    #  not in config symlink to model folder, code autodetects
    sim_path = tmp_path / REAL_PATH / "eclipse"
    sim_path.mkdir(parents=True)
    (sim_path / "model").symlink_to(REEK_ECL_MODEL)
    # Symlink in config, this is also autodetected
    conf_path = tmp_path / REAL_PATH / "fmuconfig/output/"
    conf_path.mkdir(parents=True)
    (conf_path / config_path.name).symlink_to(config_path)
    # THE TEST
    tables.export_with_config(config_path)


def test_convert_to_arrow():
    """Test function convert_to_arrow"""
    dframe = pd.DataFrame(
        {
            "DATE": ["2020-01-01", "1984-12-06", "1972-07-16"],
            "NUM": [1, 2, 4],
            "string": ["A", "BE", "SEE"],
        }
    )
    dframe["DATE"] = dframe["DATE"].astype("datetime64[ms]")
    table = convert_to_arrow(dframe)
    assert isinstance(table, pa.Table), "Did not convert to table"


def test_get_xtgeo_egrid(eightcells_datafile):
    egrid = grid3d.get_xtgeo_egrid(eightcells_datafile)
    assert isinstance(egrid, Grid), f"Expected xtgeo.Grid, got {type(egrid)}"


def test_export_init(xtgeogrid, scratch_files, case_uuid, sumo):
    real0, eight_datafile, config_path = scratch_files
    prefix = "INIT"
    init_path = fix_suffix(eight_datafile, f".{prefix}")
    expected_exports = 29
    grid3d.export_init(init_path, xtgeogrid, config_path)
    shared_grid = real0 / "share/results/grids"
    check_expected_exports(expected_exports, shared_grid, prefix)
    sleep(2)
    check_sumo(case_uuid, "INIT", expected_exports, "cpgrid_property", sumo)


def test_export_restart(xtgeogrid, scratch_files, case_uuid, sumo):
    real0, eight_datafile, config_path = scratch_files
    prefix = "UNRST"
    expected_exports = 12
    restart_path = fix_suffix(eight_datafile, f".{prefix}")
    grid3d.export_restart(
        restart_path,
        xtgeogrid,
        grid3d.get_timesteps(restart_path, xtgeogrid),
        config_path,
    )
    shared_grid = real0 / "share/results/grids"
    check_expected_exports(expected_exports, shared_grid, prefix)
    sleep(2)
    check_sumo(case_uuid, "UNRST", expected_exports, "cpgrid_property", sumo)
