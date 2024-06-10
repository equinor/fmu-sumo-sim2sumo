"""Test toolkit module for sim2sumo"""

from fmu.sumo.sim2sumo import toolkit
import pytest

TEST_DOCSTRING = '"""\nkkkk\n"""'


@pytest.mark.parametrize("instring", ['"""kkkk"""', TEST_DOCSTRING])
def test_fetch_top_docstring(instring):
    print(instring)
    assert toolkit.fetch_top_docstring(instring) == "kkkk"


def test_list_tools():

    print(toolkit.list_tools())
