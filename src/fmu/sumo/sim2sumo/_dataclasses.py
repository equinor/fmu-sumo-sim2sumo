"""Dataclass(es) for sim2sumo"""

import logging
from enum import Enum
from typing import Union
from pathlib import Path
from typing import Dict, List
from pydantic import (
    BaseModel,
    RootModel,
    ConfigDict,
    Field,
    AliasChoices,
    computed_field,
)

from fmu.sumo.sim2sumo._special_treatments import SUBMODULES


LOGGER = logging.getLogger(__file__ + "._dataclasses")

Res2DfType = Enum("Res2dfType", {mod.upper(): mod for mod in SUBMODULES})


class SimulatorFile(RootModel):
    LOGGER.debug("Init")
    root: str

    @computed_field
    @property
    def full_path(self) -> Path:
        """Full path to the file

        Returns:
            Path: the fullpath
        """
        cwd = Path().cwd()
        if self.root is None:
            glob_pattern = "*/*/*.*"
        else:
            glob_pattern = f"*/*/{self.root}*.*"
        LOGGER.debug("Globbing for results with %s", self.root)
        results = list(cwd.glob(glob_pattern))

        try:
            return results.pop()
        except IndexError:
            return None

    @computed_field
    @property
    def name(self) -> str:
        if self.full_path is None:
            return {}
        return {self.full_path: give_name(self.full_path)}


# class Res2DfType(Enum):
#     """The valid datatypes in a config file

#     Args:
#         Enum (Enum): Enumerator
#     """

#     GRID = "grid"  # grid data with properties
#     SUMMARY = "summary"  # summary data
#     NNC = "nnc"  # NNC data from EGRID file
#     FAULTS = "faults"  # data from the FAULTS keyword
#     TRANS = "trans"  # transmissibilities from EGRID file
#     PILLARS = "pillars"  # Compute data pr. cornerpoint pillar
#     PVT = "pvt"  # PVT data
#     RFT = "rft"  # RFT data from simulator binary output files.
#     FIPREPORTS = (
#         "fipreports"  # FIPxxxxx REPORT REGION data from PRT output file.
#     )
#     SATFUNC = "satfunc"  # SWOF/SGOF/etc data
#     COMPDAT = "compdat"  # COMPDAT data
#     EQUIL = "equil"  # EQUIL data
#     GRUPTREE = "gruptree"  # GRUPTREE data
#     WELLCOMPLETIONDATA = "wellcompletiondata"  # well completion data
#     VFP = "vfp"  # VFPINJ/VFPPROD data
#     WELLCONNSTATUS = "wellconnstatus"  # well connection status
#     WCON = "wcon"  # well control data


class SimulatorSuffix(Enum):
    """Valid suffixes

    Args:
        Enum (Enum): Class for defining enums
    """

    COMMON = ".DATA"
    IX = ".afi"
    PFLOTRAN = ".in"


def select_from_results(seed_text):

    globbed_results = glob4datafiles()
    full_path = [
        file_path
        for file_path in globbed_results
        if seed_text in str(file_path)
    ]
    if len(full_path) > 1:
        LOGGER.warning(
            "Several files matches pattern: %s, (%s) you might get wrong results!",
            seed_text,
            full_path,
        )

    if len(full_path) == 0:
        LOGGER.warning("No corresponding results to %s", seed_text)
        return None
    selected = full_path.pop()
    LOGGER.info("Returning %s", selected)

    return selected


def is_datafile(results: Path) -> bool:
    """Filter results based on suffix

    Args:
        results (Path): path to file

    Returns:
        bool: true if correct suffix
    """
    try:

        return hasattr(SimulatorSuffix, SimulatorSuffix(results.suffix).name)
    except ValueError:
        return False


def give_name(datafile_path: str) -> str:
    """Return name to assign in metadata

    Args:
        datafile_path (str): path to the simulator datafile

    Returns:
        str: derived name
    """
    LOGGER.info("Giving name from path %s", datafile_path)
    datafile_path_posix = Path(datafile_path)
    base_name = datafile_path_posix.name.replace(
        datafile_path_posix.suffix, ""
    )
    while base_name[-1].isdigit() or base_name.endswith("-"):
        base_name = base_name[:-1]
    LOGGER.info("Returning name %s", base_name)
    return base_name


def glob4datafiles():
    """Find datafiles relative to an ert runpath

    Returns:
        list: The datafiles found
    """
    cwd = Path().cwd()
    LOGGER.info("Looking for files in %s", cwd)
    datafiles = list(filter(is_datafile, cwd.glob("*/*/*.*")))
    LOGGER.debug("Found the following datafiles %s", datafiles)
    return datafiles


class SplitDict(BaseModel):
    """Pydantic model for sim2sumo config when split by datatypes, only default options

    Args:
        BaseModel (pydantic.BaseModel): parent
    """

    model_config = ConfigDict(extra="forbid", use_enum_values=True)
    datafiles: Union[SimulatorFile, List[SimulatorFile], None] = Field(
        default=None,
        description="Datafiles to extract from",
        validation_alias=AliasChoices("datafiles", "datafile"),
    )
    datatypes: Union[Res2DfType, List[Res2DfType]] = Field(
        description="Type(s) defined by res2df",
        default=["rft", "summary", "satfunc"],
    )
    grid3d: bool = Field(
        description="Extract 3d grid information from init or restart file",
        default=False,
    )

    @computed_field
    @property
    def full_paths(self) -> List[Path]:
        """Full path to the file

        Returns:
            Path: the fullpath
        """
        if self.datafiles is None:
            return [SimulatorFile(str(item)) for item in glob4datafiles()]
        elif isinstance(self.datafiles, SimulatorFile):
            return [self.datafiles]
        else:
            return self.datafiles


class FullDict(BaseModel):

    datafile: Dict[str, Dict[Res2DfType, Dict[str, str]]]


# # datatypes:
# #     - summary
# #     - grid
# #     - rft
# class DataTypeAsList(BaseModel):
#     datafile: Union[str, list]
#     datatypes: Union[str, List[Res2DfType]]
