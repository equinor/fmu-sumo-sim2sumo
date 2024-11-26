import os
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path

import uuid
import pytest
import yaml
from fmu.config.utilities import yaml_load
from fmu.sumo.uploader import CaseOnDisk
from httpx import HTTPStatusError
from sumo.wrapper import SumoClient

from xtgeo import grid_from_file, gridproperty_from_file
from fmu.sumo.sim2sumo._special_treatments import convert_to_arrow

REEK_ROOT = Path(__file__).parent / "data/reek"
REEK_REAL0 = REEK_ROOT / "realization-0/iter-0/"
REEK_REAL1 = REEK_ROOT / "realization-1/iter-0/"
REEK_ECL_MODEL = REEK_REAL0 / "eclipse/model/"
REEK_DATA_FILE = REEK_ECL_MODEL / "2_R001_REEK-0.DATA"
CONFIG_PATH = REEK_REAL0 / "fmuconfig/output/global_variables.yml"
EIGHTCELLS_DATAFILE = REEK_ECL_MODEL / "EIGHTCELLS.DATA"


def set_up_tmp(path):
    reek_tmp = path / "reek_tmp"
    shutil.copytree(REEK_ROOT, reek_tmp, copy_function=shutil.copy)
    real0 = reek_tmp / "realization-0/iter-0"
    config_path = real0 / "fmuconfig/output/global_variables.yml"
    eight_datafile = real0 / "eclipse/model/EIGHTCELLS.DATA"
    return real0, eight_datafile, config_path


@pytest.fixture(scope="function", name="ert_run_scratch_files")
def _fix_ert_run_scratch_files(tmp_path):
    # tmp_path is a fixture provided by pytest
    return set_up_tmp(tmp_path / "scratch")


@pytest.fixture(scope="session", name="scratch_files")
def _fix_scratch_files(tmp_path_factory):
    # tmp_path_factory is a fixture provided by pytest
    return set_up_tmp(tmp_path_factory.mktemp("scratch"))


@pytest.fixture(scope="session", name="token")
def _fix_token():
    token = os.environ.get("ACCESS_TOKEN")
    return token if token and len(token) else None


@pytest.fixture(scope="session", name="eightfipnum")
def _fix_fipnum():
    return gridproperty_from_file(
        Path(__file__).parent / "data/eightcells--init-fipnum.roff"
    )


@pytest.fixture(scope="session", name="reekrft")
def _fix_rft_reek():
    return convert_to_arrow(
        pd.read_csv(Path(__file__).parent / "data/2_r001_reek--rft.csv")
    )


@pytest.fixture(scope="session", name="drogonrft")
def _fix_rft_drogon():
    return pd.read_csv(Path(__file__).parent / "data/drogon/rft.csv")


@pytest.fixture(scope="session", name="config")
def _fix_config():
    return yaml_load(CONFIG_PATH)


@pytest.fixture(scope="session", name="sumo")
def _fix_sumo(token):
    return SumoClient(env="dev", token=token)


@pytest.fixture(autouse=True, scope="function", name="set_ert_env")
def _fix_ert_env(monkeypatch):
    monkeypatch.setenv("_ERT_REALIZATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_ITERATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_RUNPATH", "./")


@pytest.fixture(scope="session", name="case_uuid")
def _fix_register(scratch_files, sumo):
    root = scratch_files[0].parents[1]
    case_metadata_path = root / "share/metadata/fmu_case.yml"
    case_metadata = yaml_load(case_metadata_path)
    case_metadata["fmu"]["case"]["uuid"] = str(uuid.uuid4())
    case_metadata["tracklog"][0] = {
        "datetime": datetime.now().isoformat(),
        "user": {
            "id": "sim2sumo_test",
        },
        "event": "created",
    }
    print(case_metadata)
    with open(case_metadata_path, "w", encoding="utf-8") as stream:
        yaml.safe_dump(case_metadata, stream)
    case = CaseOnDisk(
        case_metadata_path,
        sumo,
        verbosity="DEBUG",
    )
    # Register the case in Sumo
    sumo_uuid = case.register()
    print("Generated ", sumo_uuid)
    return sumo_uuid


@pytest.fixture(scope="function", name="ert_run_case_uuid")
def _fix_ert_run_case_uuid(ert_run_scratch_files, sumo):
    root = ert_run_scratch_files[0].parents[1]
    case_metadata_path = root / "share/metadata/fmu_case.yml"
    case_metadata = yaml_load(case_metadata_path)
    case_metadata["fmu"]["case"]["uuid"] = str(uuid.uuid4())
    case_metadata["tracklog"][0] = {
        "datetime": datetime.now().isoformat(),
        "user": {
            "id": "sim2sumo_test",
        },
        "event": "created",
    }
    with open(case_metadata_path, "w", encoding="utf-8") as stream:
        yaml.safe_dump(case_metadata, stream)
    case = CaseOnDisk(
        case_metadata_path,
        sumo,
        verbosity="DEBUG",
    )
    # Register the case in Sumo
    sumo_uuid = case.register()
    yield sumo_uuid

    # Teardown
    try:
        sumo.delete(f"/objects('{sumo_uuid}')")
    except HTTPStatusError:
        print(f"{sumo_uuid} Already gone..")


@pytest.fixture(scope="session", name="xtgeogrid")
def _fix_xtgeogrid():
    """Export egrid file to sumo

    Args:
        datafile (str): path to datafile
    """
    egrid_path = str(EIGHTCELLS_DATAFILE).replace(".DATA", ".EGRID")
    egrid = grid_from_file(egrid_path)

    return egrid


@pytest.fixture(name="teardown", autouse=True, scope="session")
def fixture_teardown(sumo, case_uuid, request):
    """Remove all test case when all tests are run

    Args:
    case_uuid (str): uuid of test case
    sumo (SumoClient): Client to given sumo environment
    """

    def kill():
        try:
            sumo.delete(f"/objects('{case_uuid}')")
        except HTTPStatusError:
            print(f"{case_uuid} Already gone..")

    request.addfinalizer(kill)
