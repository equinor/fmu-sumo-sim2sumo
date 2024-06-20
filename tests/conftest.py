import os
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path

import os
import uuid
import pytest
import yaml
from fmu.config.utilities import yaml_load
from fmu.sumo.uploader import CaseOnDisk, SumoConnection
from sumo.wrapper import SumoClient

from xtgeo import gridproperty_from_file
from fmu.sumo.sim2sumo import grid3d
from fmu.sumo.sim2sumo._special_treatments import convert_to_arrow

REEK_ROOT = Path(__file__).parent / "data/reek"
REAL_PATH = "realization-0/iter-0/"
REEK_REAL0 = REEK_ROOT / "realization-0/iter-0/"
REEK_REAL1 = REEK_ROOT / "realization-1/iter-0/"
REEK_BASE = "2_R001_REEK"
REEK_ECL_MODEL = REEK_REAL0 / "eclipse/model/"
REEK_DATA_FILE = REEK_ECL_MODEL / f"{REEK_BASE}-0.DATA"
CONFIG_OUT_PATH = REEK_REAL0 / "fmuconfig/output/"
CONFIG_PATH = CONFIG_OUT_PATH / "global_variables.yml"
EIGHTCELLS_DATAFILE = REEK_ECL_MODEL / "EIGHTCELLS.DATA"


def set_up_tmp(path):
    reek_tmp = path / "reek_tmp"
    shutil.copytree(REEK_ROOT, reek_tmp, copy_function=shutil.copy)
    real0 = reek_tmp / "realization-0/iter-0"
    config_path = real0 / "fmuconfig/output/global_variables.yml"
    eight_datafile = real0 / "eclipse/model/EIGHTCELLS.DATA"
    return real0, eight_datafile, config_path


@pytest.fixture(scope="session", name="token")
def _fix_token():
    token = os.environ.get("ACCESS_TOKEN")
    return token if token and len(token) else None


@pytest.fixture(scope="session", name="eightcells_datafile")
def _fix_eight():
    return EIGHTCELLS_DATAFILE


@pytest.fixture(scope="session", name="eightfipnum")
def _fix_fipnum():
    return gridproperty_from_file(
        Path(__file__).parent / "data/eightcells--init-fipnum.roff"
    )


@pytest.fixture(scope="session", name="reekrft")
def _fix_rft():
    return convert_to_arrow(
        pd.read_csv(Path(__file__).parent / "data/2_r001_reek--rft.csv")
    )


@pytest.fixture(scope="session", name="config")
def _fix_config():
    return yaml_load(CONFIG_PATH)


@pytest.fixture(scope="session", name="sumo")
def _fix_sumo(token):
    return SumoClient(env="dev", token=token)


@pytest.fixture(scope="session", name="scratch_files")
def _fix_scratch_files(tmp_path_factory):

    return set_up_tmp(tmp_path_factory.mktemp("scratch"))


@pytest.fixture(autouse=True, scope="function", name="set_ert_env")
def _fix_ert_env(monkeypatch):
    monkeypatch.setenv("_ERT_REALIZATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_ITERATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_RUNPATH", "./")


@pytest.fixture(autouse=True, scope="session", name="case_uuid")
def _fix_register(scratch_files, token):

    root = scratch_files[0].parents[1]
    case_metadata_path = root / "share/metadata/fmu_case.yml"
    case_metadata = yaml_load(case_metadata_path)
    case_metadata["fmu"]["case"]["uuid"] = str(uuid.uuid4())
    case_metadata["tracklog"][0] = {
        "datetime": datetime.now().isoformat(),
        "user": {
            "id": "dbs",
        },
        "event": "created",
    }
    print(case_metadata)
    with open(case_metadata_path, "w", encoding="utf-8") as stream:
        yaml.safe_dump(case_metadata, stream)
    sumo_conn = SumoConnection(env="dev", token=token)
    case = CaseOnDisk(
        case_metadata_path=case_metadata_path,
        sumo_connection=sumo_conn,
        verbosity="DEBUG",
    )
    # Register the case in Sumo
    sumo_uuid = case.register()
    print("Generated ", sumo_uuid)
    return sumo_uuid


@pytest.fixture(scope="session", name="xtgeogrid")
def _fix_xtgeogrid(eightcells_datafile):

    return grid3d.get_xtgeo_egrid(eightcells_datafile)


@pytest.fixture(name="teardown", autouse=True, scope="session")
def fixture_teardown(case_uuid, sumo, request):
    """Remove case when all tests are run

    Args:
    case_uuid (str): uuid of test case
    sumo (SumoClient): Client to given sumo environment
    """

    def kill():
        print(f"Killing object {case_uuid}!")
        path = f"/objects('{case_uuid}')"

        sumo.delete(path)

    request.addfinalizer(kill)
