from pathlib import Path
from fmu.sumo.sim2sumo._special_treatments import (
    vfp_to_arrow_dict,
    add_md_to_rft,
)
from fmu.sumo.sim2sumo.tables import upload_vfp_tables_from_simulation_run
from fmu.sumo.sim2sumo.common import Dispatcher
from fmu.sumo.sim2sumo.tables import get_table
from test_functions import check_sumo
import pytest

DROGON = Path(__file__).parent / "data/drogon/"
DROGON_REAL = DROGON / "realization-0/iter-0/"
DROGON_DATAFILE = DROGON_REAL / "eclipse/model/DROGON-0.DATA"


@pytest.mark.parametrize(
    "options,keycombo,nrkeys,nrtables",
    [
        ({}, "VFPPROD", 1, 4),
        ({"keyword": "VFPINJ"}, "VFPINJ", 1, 1),
        ({"keyword": ["VFPPROD", "VFPINJ"]}, "VFPPRODVFPINJ", 2, 5),
        ({"vfpnumbers": "1,2,4"}, "VFPPROD", 1, 3),
    ],
)
def test_vfp_to_arrow(options, keycombo, nrkeys, nrtables):

    arrow_dict = vfp_to_arrow_dict(DROGON_DATAFILE, options)
    assert len(arrow_dict) == nrkeys
    nr_tables = 0
    returned_keycombo = ""
    for key, value in arrow_dict.items():
        nr_tables += len(value)
        returned_keycombo += key

    assert (
        nr_tables == nrtables
    ), f"Returned {nr_tables} tables, but should be {nrtables}"
    assert (
        returned_keycombo == keycombo
    ), f"Returned keycombo {returned_keycombo}, should be {keycombo}"


def test_vfp_tables_from_simulation_run(
    scratch_files, config, sumo, case_uuid, monkeypatch
):
    monkeypatch.chdir(scratch_files[0])
    disp = Dispatcher(scratch_files[2], "dev")

    upload_vfp_tables_from_simulation_run(DROGON_DATAFILE, {}, config, disp)
    disp.finish()
    check_sumo(case_uuid, "vfp", 4, "table", sumo)


def test_add_md_to_rft(drogonrft):
    merged_rft = add_md_to_rft(
        drogonrft, DROGON_REAL / "rms/output/wells/blocked_md_and_zonelog.csv"
    )

    assert merged_rft.dropna().shape[0] > 0, "No rows left after merge"


def test_get_rft_table_w_md_log():
    MD_LOG_FILE = DROGON_REAL / "rms/output/wells/blocked_md_and_zonelog.csv"
    table = get_table(DROGON_DATAFILE, "rft", md_log_file=MD_LOG_FILE)
    assert (
        table.shape[1] == 21
    ), "Don't have 21 columns as expected when merging with md log"
