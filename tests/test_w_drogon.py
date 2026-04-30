from enum import unique
from pathlib import Path

import pyarrow.compute as pc
import pytest

from fmu.sumo.sim2sumo.tables import (
    get_table,
)

DROGON = Path(__file__).parent / "data/drogon/"
DROGON_REAL = DROGON / "realization-0/iter-0/"
DROGON_DATAFILE = DROGON_REAL / "eclipse/model/DROGON-0.DATA"


def test_get_table_vfp():
    table = get_table(str(DROGON_DATAFILE), "vfp")

    unique_vfp_type = pc.unique(table["VFP_TYPE"])  # VFP_PROD & VFP_INJ
    unique_table_number = pc.unique(
        table["TABLE_NUMBER"]
    )  # 1, 2, 3, 4 (all VFP_PROD) & 13 (VFP_INJ)

    print(unique_vfp_type, unique_table_number)

    assert len(unique_vfp_type) == 2, (
        f"Returned {len(unique_vfp_type)} ({unique_vfp_type}), but should be 2."
    )
    assert len(unique_table_number) == 5, (
        f"Returned {len(unique_table_number)} ({unique_table_number}), but should be 5."
    )
