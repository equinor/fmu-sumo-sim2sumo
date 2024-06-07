"""Test toolkit module for sim2sumo"""

from fmu.sumo.sim2sumo import toolkit


def test_fetch_top_docstring():
    test_text = '"""kkkk"""'

    assert toolkit.fetch_top_docstring(test_text) == "kkkk"


def test_list_tools():

    print(toolkit.list_tools())
