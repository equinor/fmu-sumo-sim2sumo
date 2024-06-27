"""Test utility ecl2csv"""

import logging
import os
from pathlib import Path
from numpy.ma import allclose, allequal
from shutil import copytree
from subprocess import PIPE, Popen
from time import sleep
from io import BytesIO
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from xtgeo import Grid, GridProperty, gridproperty_from_file

from fmu.sumo.sim2sumo.common import (
    find_datafiles,
    prepare_for_sendoff,
    nodisk_upload,
    Dispatcher,
    find_datefield,
    find_datafile_paths,
    find_datafiles_no_seedpoint,
    filter_options,
)
from fmu.sumo.sim2sumo import grid3d, tables
from fmu.sumo.sim2sumo._special_treatments import (
    _define_submodules,
    convert_to_arrow,
    SUBMODULES,
)
from fmu.sumo.sim2sumo.common import fix_suffix, get_case_uuid
from fmu.sumo.uploader import SumoConnection

REEK_ROOT = Path(__file__).parent / "data/reek"
REAL_PATH = "realization-0/iter-0/"
REEK_REAL0 = REEK_ROOT / "realization-0/iter-0/"
REEK_REAL1 = REEK_ROOT / "realization-1/iter-0/"
REEK_BASE = "2_R001_REEK"
REEK_ECL_MODEL = REEK_REAL0 / "eclipse/model/"
REEK_DATA_FILE = REEK_ECL_MODEL / f"{REEK_BASE}-0.DATA"
CONFIG_OUT_PATH = REEK_REAL0 / "fmuconfig/output/"
CONFIG_PATH = CONFIG_OUT_PATH / "global_variables.yml"


LOGGER = logging.getLogger(__file__)
SLEEP_TIME = 3


def check_sumo(case_uuid, tag_prefix, correct, class_type, sumo):
    # There has been instances when this fails, probably because of
    # some time delay, have introduced a little sleep to make it not fail
    sleep(SLEEP_TIME)
    if tag_prefix == "*":
        search_pattern = "*"
    elif tag_prefix.endswith("*"):
        search_pattern = tag_prefix
    else:
        search_pattern = tag_prefix + "*"

    path = f"/objects('{case_uuid}')/children"
    query = f"$filter=data.tagname:{search_pattern}"

    if class_type != "*":
        query += f" AND class:{class_type}"
        check_nr = correct
    else:
        # The plus one is because we are always uploading the parameters.txt automatically
        check_nr = correct + 1
    print(query)

    results = sumo.get(path, query).json()

    LOGGER.debug(results["hits"])
    returned = results["hits"]["total"]["value"]
    LOGGER.debug("This is returned %s", returned)
    assert (
        returned == check_nr
    ), f"Supposed to upload {correct}, but actual were {returned}"

    print(f"**************\nFound {correct} {class_type} objects")

    sumo.delete(
        path,
        "$filter=*",
    )
    sleep(SLEEP_TIME)

    sumo.delete(path, query)


def write_ert_config_and_run(runpath):
    ert_config_path = "sim2sumo.ert"
    encoding = "utf-8"
    ert_full_config_path = runpath / ert_config_path
    print(f"Running with path {ert_full_config_path}")
    with open(ert_full_config_path, "w", encoding=encoding) as stream:

        stream.write(
            f"DEFINE <SUMO_ENV> dev\nNUM_REALIZATIONS 1\nMAX_SUBMIT 1\nRUNPATH {runpath}\nFORWARD_MODEL SIM2SUMO"
        )
    with Popen(
        ["ert", "test_run", str(ert_full_config_path)],
        stdout=PIPE,
        stderr=PIPE,
    ) as process:
        stdout, stderr = process.communicate()

    print(
        f"After ert run all these files where found at runpath {list(Path(runpath).glob('*'))}"
    )
    if stdout:
        print("stdout:", stdout.decode(encoding), sep="\n")
    if stderr:
        print("stderr:", stderr.decode(encoding), sep="\n")
    try:
        error_content = Path(runpath / "ERROR").read_text(encoding=encoding)
    except FileNotFoundError:
        error_content = ""
    assert (
        not error_content
    ), f"ERROR file found with content:\n{error_content}"
    assert Path(
        runpath / "OK"
    ).is_file(), f"running {ert_full_config_path}, No OK file"


def test_sim2sumo_with_ert(scratch_files, case_uuid, sumo, monkeypatch):
    monkeypatch.chdir(scratch_files[0])
    real0 = scratch_files[0]
    write_ert_config_and_run(real0)
    expected_exports = 88
    path = f"/objects('{case_uuid}')/children"
    results = sumo.get(path).json()
    returned = results["hits"]["total"]["value"]
    LOGGER.debug("This is returned %s", returned)
    assert (
        returned == expected_exports
    ), f"Supposed to upload {expected_exports}, but actual were {returned}"
