from fmu.sumo.sim2sumo import _dataclasses as dc
from pathlib import Path
from shutil import copytree
import pytest

REEK_ROOT = Path(__file__).parent / "data/reek"
REEK_REAL1 = REEK_ROOT / "realization-1/iter-0/"


def test_give_name():
    name = dc.give_name("banan.txt")
    print(name)


@pytest.mark.parametrize("seedtext", ["2_R001_REEK", "OOGRE", "BANANA"])
def test_simulatorfile(seedtext, monkeypatch, tmp_path):

    copytree(REEK_REAL1, tmp_path, dirs_exist_ok=True)
    monkeypatch.chdir(tmp_path)
    simfile = dc.SimulatorFile(seedtext)
    print("------------")
    print(simfile.full_path)
    print("**************")
    print(simfile.name)


# def test_simfile():
#     sf = dc.SimulatorFile("banana")
#     print(sf.full_path)


def test_glob4datafiles():
    dc.glob4datafiles()


def test_select_from_results():

    dc.select_from_results("banana")


# def test_simulatorfile():
#     simfile = dc.SimulatorFile.model_validate({"seedtext": "banana"})
#     print(simfile)


@pytest.mark.parametrize(
    "sim2sumoconfig",
    [
        {"datafiles": "2_R001_REEK", "datatypes": ["rft", "equil"]},
        {
            "datafiles": ["2_R001_REEK", "OOGRE_IX"],
            "datatypes": ["rft", "equil"],
        },
        {"datafile": "2_R001_REEK", "datatypes": ["rft", "equil"]},
        {"datatypes": ["rft", "equil"]},
        {},
    ],
)
def test_splitdict(sim2sumoconfig, monkeypatch, tmp_path):
    """Test pydantic object SplitDict

    Args:
        config (dict): a sim2sumo config obect
    """
    copytree(REEK_REAL1, tmp_path, dirs_exist_ok=True)
    monkeypatch.chdir(tmp_path)
    validated = dc.SplitDict.model_validate(sim2sumoconfig)
    print(validated)
    for datafile in validated.full_paths:
        print("-----------------")
        print(datafile.full_path)
        # print(datafile.name)
