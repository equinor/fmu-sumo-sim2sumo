"""Test utility ecl2csv"""
import sys
import os
from time import sleep
import logging
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pytest
from sumo.wrapper import SumoClient
from fmu.sumo import sim2sumo
from fmu.sumo.uploader import CaseOnDisk, SumoConnection


REEK_ROOT = Path(__file__).parent / "data/reek"
REAL_PATH = "realization-0/iter-0/"
REEK_REAL = REEK_ROOT / REAL_PATH
REEK_BASE = "2_R001_REEK"
REEK_ECL_MODEL = REEK_REAL / "eclipse/model/"
REEK_DATA_FILE = REEK_ECL_MODEL / f"{REEK_BASE}-0.DATA"
CONFIG_OUT_PATH = REEK_REAL / "fmuconfig/output/"
CONFIG_PATH = CONFIG_OUT_PATH / "global_variables.yml"


logging.basicConfig(
    level=logging.info, format=" %(name)s :: %(levelname)s :: %(message)s"
)
LOGGER = logging.getLogger(__file__)


def test_submodules_dict():
    """Test generation of submodule list"""
    sublist, submods = sim2sumo._define_submodules()
    LOGGER.info(submods)
    assert isinstance(sublist, tuple)
    assert isinstance(submods, dict)
    for submod_name, submod_dict in submods.items():
        LOGGER.info(submod_name)
        LOGGER.info(submod_dict)
        assert isinstance(submod_name, str)
        assert "/" not in submod_name, f"Left part of folder path for {submod_name}"
        assert isinstance(submod_dict, dict), f"{submod_name} has no subdict"
        assert (
            "options" in submod_dict.keys()
        ), f"{submod_name} does not have any options"

        assert isinstance(
            submod_dict["options"], tuple
        ), f"options for {submod_name} not tuple"


@pytest.mark.parametrize(
    "submod",
    (name for name in sim2sumo.SUBMODULES if name != "wellcompletiondata"),
)
# Skipping wellcompletion data, since this needs zonemap, which none of the others do
def test_get_results(submod):
    """Test fetching of dataframe"""
    extras = {}
    if submod == "wellcompletiondata":
        extras["zonemap"] = "data/reek/zones.lyr"
    frame = sim2sumo.get_results(REEK_DATA_FILE, submod)
    assert isinstance(
        frame, pa.Table
    ), f"Call for get_dataframe should produce dataframe, but produces {type(frame)}"
    frame = sim2sumo.get_results(REEK_DATA_FILE, submod, arrow=True)
    assert isinstance(
        frame, pa.Table
    ), f"Call for get_dataframe with arrow=True should produce pa.Table, but produces {type(frame)}"


@pytest.mark.parametrize(
    "submod",
    (name for name in sim2sumo.SUBMODULES if name != "wellcompletiondata"),
)
def test_export_results(tmp_path, submod):
    """Test writing of csv file"""
    os.chdir(tmp_path)
    export_path = tmp_path / f"share/results/tables/{REEK_BASE}--{submod}.arrow".lower()
    meta_path = export_path.parent / f".{export_path.name}.yml"
    actual_path = sim2sumo.export_results(
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
    export_path = tmp_path / f"share/results/tables/{REEK_BASE}--{submod}.arrow".lower()
    key_args = {
        "time_index": "daily",
        "start_date": "2002-01-02",
        "end_date": "2003-01-02",
    }

    meta_path = export_path.parent / f".{export_path.name}.yml"
    actual_path = sim2sumo.export_results(
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
        "nrsubmods": 16,
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
        "nrdatafile": 2,
        "nrsubmods": 3,
        "nroptions": 1,
        "arrow": True,
    },
}


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


@pytest.mark.parametrize("config_path", CONFIG_OUT_PATH.glob("*.yml"))
def test_read_config(config_path):
    """Test reading of config file via read_config function"""
    os.chdir(REEK_REAL)
    LOGGER.info(config_path)
    config = sim2sumo.yaml_load(config_path)
    assert isinstance(config, (dict, bool))
    dfiles, submods, opts = sim2sumo.read_config(config)
    name = config_path.name
    checks = CHECK_DICT[name]
    LOGGER.info(config)
    LOGGER.info(dfiles)
    LOGGER.info(submods)
    LOGGER.info(opts)
    _assert_right_len(checks, "nrdatafile", dfiles, name)
    _assert_right_len(checks, "nrsubmods", submods, name)
    _assert_right_len(checks, "nroptions", opts, name)

    assert opts["arrow"] == checks["arrow"], f"Wrong choice for arrow for {name}"


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
    sim2sumo.export_with_config(config_path)


# def test_upload(token):
#     """Test the upload function"""
#     sumo_env = "dev"
#     sumo = SumoClient(sumo_env, token)
#     sumocon = SumoConnection(sumo_env, token)
#     case_metadata_path = REEK_ROOT / "share/metadata/fmu_case.yml"
#     LOGGER.info("This is the case metadata %s", case_metadata_path)
#     LOGGER.info("Using token %s", token)
#     case = CaseOnDisk(
#         case_metadata_path=case_metadata_path,
#         sumo_connection=sumocon,
#         verbosity="DEBUG",
#     )

#     case_uuid = case.register()
#     sim2sumo.upload(
#         str(REEK_ROOT / "realization-0/iter-0/share/results/tables"),
#         ["csv"],
#         sumo_env,
#     )
#     # There has been instances when this fails, probably because of
#     # some time delay, have introduced a little sleep to get it to be quicker
#     sleep(2)
#     results = sumo.get(
#         "/search", query=f"fmu.case.uuid:{case_uuid} AND class:table", size=0
#     )
#     LOGGER.debug(results["hits"])
#     correct = 2
#     returned = results["hits"]["total"]["value"]
#     LOGGER.debug("This is returned ", returned)
#     assert (
#         returned == correct
#     ), f"Tried to upload {correct}, but only managed {returned}"
#     path = f"/objects('{case_uuid}')"

#     sumo.delete(path)


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
    print(dframe.dtypes)
    table = sim2sumo.convert_to_arrow(dframe)
    assert isinstance(table, pa.Table), "Did not convert to table"


if __name__ == "__main__":
    test_convert_to_arrow()
