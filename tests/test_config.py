"""Tests for fmu.sumo.sim2sumo.config."""

from shutil import copytree

import pytest
from conftest import CONFIG_PATH, REEK_REAL1
from fmu.dataio import ExportData
from fmu.datamodels.fmu_results.global_configuration import GlobalConfiguration

from fmu.sumo.sim2sumo._special_treatments import (
    DEFAULT_RST_PROPS,
    DEFAULT_SUBMODULES,
)
from fmu.sumo.sim2sumo.config import Sim2SumoConfig, _build_sim2sumo_config

NR_DEFAULT_SUBMODULES = len(DEFAULT_SUBMODULES)


@pytest.mark.parametrize(
    "config,nrdatafiles,nrsubmodules",
    [
        ({}, 5, NR_DEFAULT_SUBMODULES),
        (
            {"datafile": [{"3_R001_REEK": ["summary"]}]},
            1,
            1,
        ),
        (
            {"datafile": [{"3_R001_REEK": ["summary", "rft"]}]},
            1,
            2,
        ),
        (
            {"datafile": ["3_R001_REEK", "OOGRE_PF.in"]},
            2,
            NR_DEFAULT_SUBMODULES,
        ),
        ({"datafile": ["3_R001_REEK"]}, 1, NR_DEFAULT_SUBMODULES),
        ({"datafile": ["3_R001_REEK-1.DATA"]}, 1, NR_DEFAULT_SUBMODULES),
        ({"datafile": ["OOGRE_IX.afi"]}, 1, NR_DEFAULT_SUBMODULES),
        ({"datafile": ["opm/model/OOGRE_OPM.DATA"]}, 1, NR_DEFAULT_SUBMODULES),
        ({"datatypes": ["grid"]}, 5, 1),
    ],
)
def test_create_build_sim2sumo_config(
    config, nrdatafiles, nrsubmodules, tmp_path, monkeypatch
):
    real1 = tmp_path / "realization-1/iter-0"
    copytree(REEK_REAL1, real1)
    monkeypatch.chdir(real1)

    sim2sumoconfig = _build_sim2sumo_config(config)
    assert len(sim2sumoconfig) == nrdatafiles, (
        f"{sim2sumoconfig.keys()} expected to have len {nrdatafiles} datafiles"
    )
    for submod, subdict in sim2sumoconfig.items():
        assert len(subdict) == nrsubmodules, (
            f"{subdict} for {submod} expected to have {nrsubmodules} submodules"
        )


def test_create_config_returns_sim2sumo_config(scratch_files, monkeypatch):
    """from_global_variables returns a Sim2SumoConfig with a GlobalConfiguration."""
    monkeypatch.chdir(scratch_files[0])
    config = Sim2SumoConfig.from_global_variables(CONFIG_PATH)

    assert isinstance(config, Sim2SumoConfig)
    assert isinstance(config.global_config, GlobalConfiguration)
    assert config.global_config.model.name == "ff"
    assert len(config.sim2sumo) > 0


def test_create_config_global_config_has_expected_fields(
    scratch_files, monkeypatch
):
    """GlobalConfiguration loaded via Sim2SumoConfig has masterdata & access."""
    monkeypatch.chdir(scratch_files[0])
    config = Sim2SumoConfig.from_global_variables(CONFIG_PATH)

    gc = config.global_config
    assert gc.masterdata is not None
    assert gc.access is not None
    assert gc.access.asset.name == "Drogon"
    assert gc.model.name == "ff"


def test_create_config_with_dot_fmu_global_config_has_expected_fields(
    ert_run_scratch_files, monkeypatch
) -> None:
    """GlobalConfiguration loaded via Sim2SumoConfig has masterdata & access.

    Uses 'ert_run_scratch_files' because it's function scoped."""
    from fmu.settings._drogon import create_drogon_fmu_dir

    fmu_dir = create_drogon_fmu_dir(ert_run_scratch_files[0])
    monkeypatch.chdir(ert_run_scratch_files[0])

    fmu_dir_config = fmu_dir.config.load()
    config = Sim2SumoConfig.from_global_variables(CONFIG_PATH)
    global_config = config.global_config

    assert fmu_dir_config.masterdata == global_config.masterdata
    assert fmu_dir_config.model == global_config.model
    assert global_config.model.name == "Drogon"  # .fmu/ not set as 'ff'


def test_create_config_sim2sumo_options_match_yaml(scratch_files, monkeypatch):
    """The sim2sumo options extracted by Sim2SumoConfig match what's in the YAML."""
    monkeypatch.chdir(scratch_files[0])
    config = Sim2SumoConfig.from_global_variables(CONFIG_PATH)

    for _datafile, submods in config.sim2sumo.items():
        assert set(submods.keys()) == {"summary", "rft", "satfunc", "grid"}
        assert submods["grid"]["rstprops"] == DEFAULT_RST_PROPS


def test_create_config_global_config_passed_to_exportdata(
    scratch_files, monkeypatch
):
    """GlobalConfiguration is accepted by ExportData without error."""
    monkeypatch.chdir(scratch_files[0])
    config = Sim2SumoConfig.from_global_variables(CONFIG_PATH)

    exd = ExportData(
        config=config.global_config,
        name="test",
        tagname="test",
        content="depth",
    )
    assert exd._export_config.config == config.global_config
