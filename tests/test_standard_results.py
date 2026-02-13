from pathlib import Path

import pandas as pd
import pytest
import yaml
from fmu.dataio._definitions import STANDARD_TABLE_INDEX_COLUMNS
from fmu.datamodels.fmu_results.enums import Content
from fmu.datamodels.standard_results import StandardResultName

from fmu.sumo.sim2sumo.tables import SUBMOD_CONTENT, generate_table_meta


@pytest.mark.parametrize("tagname", SUBMOD_CONTENT.keys())
@pytest.mark.filterwarnings("ignore:Could not detect the case metadata")
def test_table_standard_result_metadata(
    scratch_files: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    tagname: str,
) -> None:
    realization, datafile, config_path = scratch_files

    with open(config_path) as f:
        global_config_dict = yaml.safe_load(f)

    config_dict = {"fmuconfig": global_config_dict}

    content_str = SUBMOD_CONTENT.get(tagname)
    content_enum = Content(content_str)
    std_columns = STANDARD_TABLE_INDEX_COLUMNS.get(content_enum)

    if content_enum is Content.lift_curves:
        # Standard index columns for lift curves don't exist yet
        assert std_columns is None
        return

    table = pd.DataFrame({col: [1, 2, 3] for col in std_columns.columns})

    metadata = generate_table_meta(datafile, table, tagname, config_dict)
    assert metadata["data"]["standard_result"] is not None
    assert (
        metadata["data"]["standard_result"]["name"]
        == StandardResultName(content_str).value
    )
