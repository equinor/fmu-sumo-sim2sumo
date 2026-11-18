from pathlib import Path

import pyarrow as pa
import pytest

from fmu.sumo.sim2sumo import __version__
from fmu.sumo.sim2sumo.config import Sim2SumoConfig
from fmu.sumo.sim2sumo.tables import (
    SUBMOD_CONTENT,
    _get_standard_result_name,
    _get_table_index_for_standard_result,
    generate_table_meta,
)


@pytest.mark.parametrize("tagname", SUBMOD_CONTENT.keys())
@pytest.mark.filterwarnings("ignore:Could not detect the case metadata")
def test_table_standard_result_metadata(
    scratch_files: tuple[Path, Path, Path],
    tagname: str,
) -> None:
    _, datafile, config_path, _ = scratch_files
    config = Sim2SumoConfig.from_global_variables(config_path)

    content_str = SUBMOD_CONTENT.get(tagname)
    standard_result_name = _get_standard_result_name(content_str)
    std_columns = _get_table_index_for_standard_result(standard_result_name)

    table = pa.Table.from_pydict({col: [1, 2, 3] for col in std_columns})

    metadata = generate_table_meta(datafile, table, tagname, config)
    assert metadata["data"]["standard_result"] is not None
    assert (
        metadata["data"]["standard_result"]["name"]
        == standard_result_name.value
    )
    assert metadata["tracklog"][0]["sysinfo"]["source"] == {
        "name": "fmu-sumo-sim2sumo",
        "version": __version__,
    }


def test_get_table_index_for_standard_result_raises_on_unknown() -> None:
    """Test that _get_table_index_for_standard_result raises for unknown standard result."""
    with pytest.raises(ValueError):
        _get_table_index_for_standard_result("not_a_standard_result")


def test_get_standard_result_name_returns_none_for_unknown_content() -> None:
    """Test that _get_standard_result_name returns None for unknown content."""
    assert _get_standard_result_name("not_a_standard_result") is None
