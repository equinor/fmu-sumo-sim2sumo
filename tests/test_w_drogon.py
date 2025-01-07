from pathlib import Path

import pytest
from test_functions import check_sumo

from fmu.sumo.sim2sumo._special_treatments import vfp_to_arrow_dict
from fmu.sumo.sim2sumo.common import Dispatcher
from fmu.sumo.sim2sumo.tables import (
    upload_vfp_tables_from_simulation_run,
)

DROGON = Path(__file__).parent / "data/drogon/"
DROGON_REAL = DROGON / "realization-0/iter-0/"
DROGON_DATAFILE = DROGON_REAL / "eclipse/model/DROGON-0.DATA"


@pytest.mark.parametrize(
    "options,keycombo,nrkeys,nrtables",
    [
        ({}, ["VFPPROD", "VFPINJ"], 2, 5),
        ({"keyword": "VFPINJ"}, ["VFPINJ"], 1, 1),
        ({"keyword": ["VFPPROD", "VFPINJ"]}, ["VFPPROD", "VFPINJ"], 2, 5),
        ({"vfpnumbers": "1,2,4"}, ["VFPPROD", "VFPINJ"], 2, 3),
    ],
)
def test_vfp_to_arrow(options, keycombo, nrkeys, nrtables):
    arrow_dict = vfp_to_arrow_dict(DROGON_DATAFILE, options)
    assert len(arrow_dict) == nrkeys
    nr_tables = 0
    for value in arrow_dict.values():
        nr_tables += len(value)

    assert (
        nr_tables == nrtables
    ), f"Returned {nr_tables} tables, but should be {nrtables}"
    assert set(arrow_dict.keys()) == set(
        keycombo
    ), f"Returned keys {arrow_dict.keys()}, should be {keycombo}"


def test_vfp_tables_from_simulation_run(
    scratch_files, config, sumo, case_uuid, monkeypatch
):
    monkeypatch.chdir(scratch_files[0])
    disp = Dispatcher(scratch_files[2], "dev")

    upload_vfp_tables_from_simulation_run(DROGON_DATAFILE, {}, config, disp)
    disp.finish()
    check_sumo(case_uuid, "vfp", 5, "table", sumo)
