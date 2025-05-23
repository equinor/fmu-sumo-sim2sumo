"""Common functions for the other sim2sumo modules"""

import logging
import re
from pathlib import Path

import psutil
import yaml
from sumo.wrapper import SumoClient

from fmu.sumo.sim2sumo._special_treatments import (
    DEFAULT_SUBMODULES,
    SUBMODULES,
)
from fmu.sumo.uploader._upload_files import upload_files


def yaml_load(file_name):
    """Load yaml config file into dict

    Args:
        file_name (str): name of yaml file

    Returns:
        dict: the read results
    """
    logger = logging.getLogger(__file__ + ".yaml_load")
    config = {}
    try:
        with open(file_name, "r", encoding="utf-8") as yam:
            config = yaml.safe_load(yam)
    except OSError:
        logger.warning("Cannot open file, will return empty dict")
    return config


def get_case_uuid(file_path, parent_level=4):
    """Get case uuid from case metadata file

    Args:
        file_path (str): path to file in ensemble
        parent_level (int, optional): nr of levels to move down to root.
                                        Defaults to 4.

    Returns:
        str: the case uuid
    """
    case_meta_path = (
        Path(file_path).parents[parent_level] / "share/metadata/fmu_case.yml"
    )
    case_meta = yaml_load(case_meta_path)
    uuid = case_meta["fmu"]["case"]["uuid"]
    return uuid


def find_datafiles(seedpoint=None):
    """Find datafiles relative to seedpoint or the current working directory.

    Args:
        seedpoint (str|Path|list, optional): Where to search for datafiles.
            Either a specific file, list of directories, or single directory.

    Returns:
        list: The datafiles found with unique stem names, as full paths.
    """
    logger = logging.getLogger(__file__ + ".find_datafiles")
    valid_filetypes = [".DATA", ".afi", ".in"]
    datafiles = []
    cwd = Path().cwd()  # Get the current working directory

    if isinstance(seedpoint, list):
        # Convert all elements to Path objects
        seedpoint = [Path(sp) for sp in seedpoint]
    elif seedpoint:
        seedpoint = [seedpoint]

    if seedpoint:
        for sp in seedpoint:
            full_path = (
                cwd / sp if not sp.is_absolute() else sp
            )  # Make the path absolute
            if full_path.suffix in valid_filetypes:
                if full_path.is_file():
                    # Add the file if it has a valid filetype
                    datafiles.append(full_path)
                else:
                    datafiles.extend(
                        list(full_path.parent.rglob(f"{full_path.name}"))
                    )
            else:
                for filetype in valid_filetypes:
                    if not full_path.is_dir():
                        # Search for valid files within the directory
                        datafiles.extend(
                            list(
                                full_path.parent.rglob(
                                    f"{full_path.name}*{filetype}"
                                )
                            )
                        )
                    else:
                        # Search for valid files within the directory
                        datafiles.extend(list(full_path.rglob(f"*{filetype}")))
    else:
        # Search the current working directory if no seedpoint is provided
        for filetype in valid_filetypes:
            datafiles.extend(list(cwd.rglob(f"*/*/*{filetype}")))
    # Filter out files with duplicate stems, keeping the first occurrence
    unique_stems = set()
    unique_datafiles = []
    for datafile in datafiles:
        stem = datafile.with_suffix("").stem
        if stem not in unique_stems:
            unique_stems.add(stem)
            unique_datafiles.append(datafile.resolve())  # Resolve to full path

    logger.info(f"Using datafiles: {str(unique_datafiles)} ")
    return unique_datafiles


def create_config_dict(config):
    """Read config settings and make dictionary for use when exporting.

    Args:
        config (dict): the settings for export of simulator results.

    Returns:
        dict: dictionary with key as path to datafile, value as dict of
              submodule and option.
    """
    simconfig = config.get("sim2sumo", {})
    validate_sim2sumo_config(simconfig)

    # Use the provided datafile or datatype if given, otherwise use simconfig
    datafile = simconfig.get("datafile", None)
    datatype = simconfig.get("datatypes", None)

    if datatype is None:
        default_submods = DEFAULT_SUBMODULES
    elif "all" in datatype:
        default_submods = SUBMODULES
    elif isinstance(datatype, list):
        default_submods = datatype
    else:
        default_submods = [datatype]

    submods = default_submods

    # Initialize the dictionary to hold the configuration for each datafile
    sim2sumoconfig = {}
    paths = []

    if datafile:
        for file in datafile:
            if isinstance(file, dict):
                (((filepath, file_submods)),) = file.items()
                submods = file_submods or default_submods
            else:
                filepath = file

            path = Path(filepath)
            if path.is_file():
                paths += [path]
            else:
                paths += find_datafiles(path)
    else:
        paths += find_datafiles(datafile)

    for datafile_path in paths:
        sim2sumoconfig[datafile_path] = {}
        for submod in submods:
            sim2sumoconfig[datafile_path][submod] = {"arrow": True}

    return sim2sumoconfig


class Dispatcher:
    """Controls upload to sumo"""

    def __init__(
        self,
        datafile,
        env,
        config_path="fmuconfig/output/global_variables.yml",
        token=None,
    ):
        self._logger = logging.getLogger(__name__ + ".Dispatcher")
        self._limit_percent = 0.5
        self._parentid = get_case_uuid(datafile.resolve())
        self._conn = SumoClient(env=env, token=token, case_uuid=self._parentid)
        self._mem_limit = (
            psutil.virtual_memory().available * self._limit_percent
        )
        self._config_path = config_path

        self._mem_count = 0
        self._count = 0
        self._objects = []
        self._logger.info(
            "Init, parent is %s, and env is %s", self.parentid, env
        )

    @property
    def parentid(self):
        """Return parentid"""
        return self._parentid

    @property
    def mem_frac(self):
        """Calculate fraction of memory

        Returns:
            float: fraction of available memory
        """
        return self._mem_count / self._mem_limit

    def add(self, file):
        """Add file

        Args:
            file (SumoFile): file to add
        """
        if file is not None:
            self._mem_count += file.size
            self._objects.append(file)
            self._count += 1
            self._mem_limit = (
                psutil.virtual_memory().available * self._limit_percent
            )

            if (self.mem_frac > 1) or (self._count > 100):
                self._logger.info(
                    "Uploading (mem frac %s, and count is %s)",
                    self.mem_frac,
                    self._count,
                )
                self._upload()
        else:
            self._logger.warning("File is None, not adding")

    def _upload(self):
        self._logger.debug("%s files to upload", len(self._objects))
        nodisk_upload(
            self._objects,
            self._parentid,
            self._config_path,
            connection=self._conn,
        )
        self._objects = []
        self._mem_count = 0

    def finish(self):
        """Cleanup"""
        self._upload()


def find_datefield(text_string):
    """Extract possible date at end of string

    Args:
        text_string (str): string with possible date

    Returns:
        str| None: date as string or None
    """
    datesearch = re.search(".*_([0-9]{8})$", text_string)
    date = datesearch.group(1) if datesearch is not None else None
    return date


def nodisk_upload(files, parent_id, config_path, env="prod", connection=None):
    """Upload files to sumo

    Args:
        files (list): should contain only SumoFile objects
        parent_id (str): uuid of parent object
        connection (str): client to upload with
    """
    logger = logging.getLogger(__name__ + ".nodisk_upload")
    if len(files) > 0:
        logger.info("Uploading %s files to parent %s", len(files), parent_id)
        if connection is None:
            connection = SumoClient(env=env, case_uuid=parent_id)
        status = upload_files(
            files, parent_id, connection, config_path=config_path
        )
        print("Status after upload: ", end="\n--------------\n")
        for state, obj_status in status.items():
            print(f"{state}: {len(obj_status)}")
    else:
        logger.info("No passed files, nothing to do here")


def give_name(datafile_path: str) -> str:
    """Return name to assign in metadata

    Args:
        datafile_path (str): path to the simulator datafile

    Returns:
        str: derived name
    """
    datafile_path_posix = Path(datafile_path)
    base_name = datafile_path_posix.name.replace(
        datafile_path_posix.suffix, ""
    )
    while base_name[-1].isdigit() or base_name.endswith("-"):
        base_name = base_name[:-1]
    return base_name


DOCS_BASE_URL = (
    "https://fmu-sumo-sim2sumo.readthedocs.io/en/latest/sim2sumo.html"
)


def validate_sim2sumo_config(config):
    datafiles = config.get("datafile", [])
    if not isinstance(datafiles, list):
        raise ValueError(
            "Config error: datafile must be a list."
            " See documentation for examples: "
            f" {DOCS_BASE_URL}#datafile"
        )

    datatypes = config.get("datatypes", [])
    if not isinstance(datatypes, list):
        raise ValueError(
            "Config error: datatypes must be a list."
            " See documentation for examples: "
            f" {DOCS_BASE_URL}#datatypes"
        )
