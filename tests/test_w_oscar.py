from pathlib import Path
import pytest
from fmu.sumo.sim2sumo._special_treatments import vfp_to_arrow_dict
from fmu.sumo.sim2sumo.tables import upload_vfp_tables_from_simulation_run
from fmu.sumo.sim2sumo.common import Dispatcher, prepare_for_sendoff
import shutil
from fmu.config.utilities import yaml_load
from fmu.sumo.uploader import CaseOnDisk, SumoConnection
from sumo.wrapper import SumoClient
import uuid
from datetime import datetime
import yaml


OSCAR = Path(__file__).parent / "data/oscar/"
OSCAR_REAL = OSCAR / "realization-0/iter-0/"
OSCAR_DATAFILE = OSCAR_REAL / "eclipse/model/OSCAR-0.DATA"


@pytest.mark.parametrize(
    "options,keyword,nrtables",
    [
        ({}, "VFPPROD", 9),
        ({"keyword": "VFPINJ"}, "VFPINJ", 1),
    ],
)
def test_vfp_to_arrow(options, keyword, nrtables):

    returned_keyword, arrow_dict = vfp_to_arrow_dict(OSCAR_DATAFILE, options)
    dict_length = len(arrow_dict)

    print(dict_length)
    assert (
        returned_keyword == keyword
    ), f"Returned keyword {returned_keyword}, should be {keyword}"
    assert (
        dict_length == nrtables
    ), f"Returned {dict_length} tables, but should be {nrtables}"


def test_vfp_tables_from_simulation_run(tmp_path, token, config, monkeypatch):
    oscar_tmp = tmp_path / "oscar_tmp"
    shutil.copytree(OSCAR, oscar_tmp, copy_function=shutil.copy)

    case_metadata_path = OSCAR / "share/metadata/fmu_case.yml"
    case_metadata = yaml_load(case_metadata_path)
    case_metadata["fmu"]["case"]["uuid"] = str(uuid.uuid4())
    case_metadata["tracklog"][0] = {
        "datetime": datetime.now().isoformat(),
        "user": {
            "id": "oscar",
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
    real0 = oscar_tmp / "realization-0/iter-0"
    datafile = real0 / "eclipse/model/OSCAR-0.DATA"
    monkeypatch.chdir(real0)

    config = yaml_load(real0 / "fmuconfig/output/global_variables.yml")
    simconfig = prepare_for_sendoff(config, datafile)

    disp = Dispatcher(datafile, "dev")

    upload_vfp_tables_from_simulation_run(OSCAR_DATAFILE, {}, simconfig, disp)
    disp.finish()
