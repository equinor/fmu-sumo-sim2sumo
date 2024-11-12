"""Common functions for the other sim2sumo modules"""

import logging
import re
from pathlib import Path

import psutil
import yaml

from fmu.dataio import ExportData
from fmu.sumo.uploader import SumoConnection
from fmu.sumo.uploader._upload_files import upload_files
from fmu.sumo.sim2sumo._special_treatments import (
    SUBMOD_DICT,
    SUBMODULES,
)

from res2df.common import convert_lyrlist_to_zonemap, parse_lyrfile


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
        parent_level (int, optional): nr of levels to move down to root. Defaults to 4.

    Returns:
        str: the case uuid
    """
    case_meta_path = (
        Path(file_path).parents[parent_level] / "share/metadata/fmu_case.yml"
    )
    case_meta = yaml_load(case_meta_path)
    uuid = case_meta["fmu"]["case"]["uuid"]
    return uuid


def filter_options(submod, kwargs):
    """Filter options sent to res2df per given submodule

    Args:
        submod (str): the submodule to call
        kwargs (dict): the options passed

    Returns:
        dict: options relevant for given submod
    """
    logger = logging.getLogger(__file__ + ".filter_options")
    submod_options = SUBMOD_DICT[submod]["options"]
    filtered = {
        key: value
        for key, value in kwargs.items()
        if (key in submod_options) or key in ["arrow", "md_log_file"]
    }
    filtered["arrow"] = kwargs.get(
        "arrow", True
    )  # defaulting of arrow happens here
    non_options = [key for key in kwargs if key not in filtered]
    if len(non_options) > 0:
        logger.warning(
            "Skipping invalid options %s for %s.",
            non_options,
            submod,
        )

    if "zonemap" in filtered:
        filtered["zonemap"] = convert_lyrlist_to_zonemap(
            parse_lyrfile(filtered["zonemap"])
        )
    return filtered


def find_datafiles(seedpoint=None):
    """Find datafiles relative to an optional seedpoint or the current working directory.

    Args:
        seedpoint (str|Path|list, optional): Specific file, list of directories, or single directory to search for datafiles.

    Returns:
        list: The datafiles found with unique stem names, as full paths.
    """
    logger = logging.getLogger(__file__ + ".find_datafiles")
    valid_filetypes = [".DATA", ".afi", ".in"]
    datafiles = []
    cwd = Path().cwd()  # Get the current working directory

    # TODO: Be stricter on seedpoint type. Should be either a list or None. If something else: raise ValueError("datafile should be a list or None")
    if isinstance(seedpoint, dict):
        # Extract the values (paths) from the dictionary and treat them as a list
        seedpoint = list(seedpoint.values())
    elif isinstance(seedpoint, list):
        # If seedpoint is a list, ensure all elements are strings or Path objects
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
                        [
                            f
                            for f in full_path.parent.rglob(
                                f"{full_path.name}"
                            )
                        ]
                    )
            else:
                for filetype in valid_filetypes:
                    if not full_path.is_dir():
                        # Search for valid files within the directory with partly filename
                        datafiles.extend(
                            [
                                f
                                for f in full_path.parent.rglob(
                                    f"{full_path.name}*{filetype}"
                                )
                            ]
                        )
                    else:
                        # Search for valid files within the directory
                        datafiles.extend(
                            [f for f in full_path.rglob(f"*{filetype}")]
                        )
    else:
        # Search the current working directory if no seedpoint is provided
        for filetype in valid_filetypes:
            datafiles.extend([f for f in cwd.rglob(f"*/*/*{filetype}")])
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

    grid3d = simconfig.get("grid3d", False)

    # Use the provided datafile or datatype if given, otherwise use simconfig
    datafile = simconfig.get("datafile", [])
    datatype = simconfig.get("datatypes", None)

    if datatype is None:
        default_submods = ["summary", "rft", "satfunc"]
    elif datatype == "all":
        default_submods = SUBMODULES
    elif isinstance(datatype, list):
        default_submods = datatype
    else:
        default_submods = [datatype]

    # Initialize the dictionary to hold the configuration for each datafile
    sim2sumoconfig = {}

    if datafile:
        for file in datafile:
            if isinstance(file, str):
                # If datafile is a string
                path = Path(file)

                if path.is_file():
                    # If the path is a file, use it directly, not checking filetype
                    datafiles = [path]
                # If the path is a directory or part of filename, find all matches
                else:
                    datafiles = find_datafiles(path)
                for datafile_path in datafiles:
                    sim2sumoconfig[datafile_path] = {}
                    for submod in default_submods or []:
                        options = simconfig.get("options", {"arrow": True})
                        sim2sumoconfig[datafile_path][submod] = filter_options(
                            submod, options
                        )
                    sim2sumoconfig[datafile_path]["grid3d"] = grid3d
            else:
                # datafile is a dict
                for filepath, submods in datafile.items():
                    path = Path(filepath)
                    if path.is_file():
                        # If the path is a file, use it directly, not checking filetype
                        datafiles = [path]
                    # If the path is a directory or part of filename, find all matches
                    else:
                        datafiles = find_datafiles(path)

                    # Create config entries for each datafile
                    for datafile_path in datafiles:
                        sim2sumoconfig[datafile_path] = {}
                        for submod in submods:
                            # Use the global options or default to {"arrow": True}
                            options = simconfig.get("options", {"arrow": True})
                            sim2sumoconfig[datafile_path][submod] = (
                                filter_options(submod, options)
                            )
                        sim2sumoconfig[datafile_path]["grid3d"] = grid3d
    else:
        # If datafile is not specified, find all valid files in the current directory
        datafiles_paths = find_datafiles(datafile)
        for datafile_path in datafiles_paths:
            sim2sumoconfig[datafile_path] = {}
            for submod in default_submods or []:
                options = simconfig.get("options", {"arrow": True})
                sim2sumoconfig[datafile_path][submod] = filter_options(
                    submod, options
                )
            sim2sumoconfig[datafile_path]["grid3d"] = grid3d

    # # If datafile is a dictionary, iterate over its items
    # if isinstance(datafile, dict):
    #     for filepath, submods in datafile.items():
    #         # Convert the filepath to a Path object
    #         path = Path(filepath)

    #         if path.is_file():
    #             # If the path is a file, use it directly, not checking filetype
    #             datafiles = [path]
    #         # If the path is a directory or part of filename, find all matches
    #         else:
    #             datafiles = find_datafiles(path)

    #         # Create config entries for each datafile
    #         for datafile_path in datafiles:
    #             sim2sumoconfig[datafile_path] = {}
    #             for submod in submods:
    #                 # Use the global options or default to {"arrow": True}
    #                 options = simconfig.get("options", {"arrow": True})
    #                 sim2sumoconfig[datafile_path][submod] = filter_options(
    #                     submod, options
    #                 )
    #             sim2sumoconfig[datafile_path]["grid3d"] = grid3d
    # else:
    #     # If datafile is not a dictionary, use the existing logic
    #     datafiles_paths = find_datafiles(datafile)
    #     for datafile_path in datafiles_paths:
    #         sim2sumoconfig[datafile_path] = {}
    #         for submod in submods or []:
    #             options = simconfig.get("options", {"arrow": True})
    #             sim2sumoconfig[datafile_path][submod] = filter_options(
    #                 submod, options
    #             )
    #         sim2sumoconfig[datafile_path]["grid3d"] = grid3d

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
        self._conn = SumoConnection(env=env, token=token)
        self._env = env
        self._mem_limit = (
            psutil.virtual_memory().available * self._limit_percent
        )
        self._config_path = config_path

        self._mem_count = 0
        self._count = 0
        self._objects = []
        self._logger.info(
            "Init, parent is %s, and env is %s", self.parentid, self.env
        )

    @property
    def parentid(self):
        """Return parentid"""
        return self._parentid

    @property
    def env(self):
        """Return env"""
        return self._env

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

            self._logger.debug(
                "Count is %s, and mem frac is %f1.1",
                self._count,
                self.mem_frac,
            )
            if (self.mem_frac > 1) or (self._count > 100):
                self._logger.info(
                    "Time to upload (mem frac %s, and count is %s)",
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
    if datesearch is not None:
        date = datesearch.group(1)
    else:
        date = None
    return date


def generate_meta(config, datafile_path, tagname, obj, content):
    """Generate metadata for object

    Args:
        config (dict): the metadata required
        datafile_path (str): path to datafile or relative
        tagname (str): the tagname
        obj (object): object eligible for dataio

    Returns:
        dict: the metadata to export
    """
    name = give_name(datafile_path)
    exp_args = {
        "config": config,
        "name": name,
        "tagname": tagname,
        "content": content,
    }

    datefield = find_datefield(tagname)
    if datefield is not None:
        exp_args["timedata"] = [[datefield]]

    exd = ExportData(**exp_args)
    metadata = exd.generate_metadata(obj)
    relative_parent = str(Path(datafile_path).parents[2]).replace(
        str(Path(datafile_path).parents[4]), ""
    )
    metadata["file"] = {
        "relative_path": f"{relative_parent}/{name}--{tagname}".lower()
    }
    return metadata


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
            connection = SumoConnection(env=env)
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


def validate_sim2sumo_config(config):
    datafiles = config.get("datafile", [])
    if not isinstance(datafiles, list):
        raise ValueError("Config error: datafile must be a list")
    # TODO: Uncomment and use this if it should be required that each file is a dict
    # for file in datafiles:
    #     if not isinstance(file, dict):
    #         raise ValueError(
    #             "Config error: each datafile should be a dictionary"
    #         )
