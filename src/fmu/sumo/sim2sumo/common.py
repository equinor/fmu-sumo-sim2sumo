"""Common functions for the other sim2sumo modules"""

import logging
import re
from pathlib import Path

import psutil
import yaml


from fmu.dataio import ExportData
from fmu.sumo.uploader import SumoConnection
from fmu.sumo.uploader._fileonjob import FileOnJob
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
    logger = logging.getLogger(__name__ + ".get_case_uuid")
    logger.debug("Asked for parent %s for %s", parent_level, file_path)
    case_meta_path = (
        Path(file_path).parents[parent_level] / "share/metadata/fmu_case.yml"
    )
    logger.debug("Case meta path: %s", case_meta_path)
    case_meta = yaml_load(case_meta_path)
    uuid = case_meta["fmu"]["case"]["uuid"]
    logger.info("Case uuid: %s", uuid)
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
    logger.debug("Available options for %s are %s", submod, submod_options)
    logger.debug("Input: %s", kwargs)
    filtered = {
        key: value
        for key, value in kwargs.items()
        if (key in submod_options) or key in ["arrow", "md_log_file"]
    }
    filtered["arrow"] = kwargs.get(
        "arrow", True
    )  # defaulting of arrow happens here
    logger.debug("After filtering options for %s: %s", submod, filtered)
    non_options = [key for key in kwargs if key not in filtered]
    if len(non_options) > 0:
        logger.warning(
            "Filtered out options %s for %s, these are not valid",
            non_options,
            submod,
        )

    if "zonemap" in filtered:
        filtered["zonemap"] = convert_lyrlist_to_zonemap(
            parse_lyrfile(filtered["zonemap"])
        )
    return filtered


def find_full_path(datafile, paths):
    """Find full path for datafile from dictionary

    Args:
        datafile (str): path or name of path
        paths (dict): dictionary of file paths

    Returns:
        Path: path to the full datafile
    """
    logger = logging.getLogger(__file__ + ".find_full_path")
    data_name = give_name(datafile)
    try:
        return paths[data_name]
    except KeyError:
        mess = (
            "Datafile %s, with derived name %s, not found in %s,"
            " have to skip"
        )
        logger.warning(mess, datafile, data_name, paths)
        return None


def find_datafile_paths():
    """Find all simulator paths

    Returns:
        dict: key is name to use in sumo, value full path to file
    """
    logger = logging.getLogger(__file__ + ".find_datafile_paths")
    paths = {}
    for data_path in find_datafiles_no_seedpoint():
        name = give_name(data_path)

        if name not in paths:
            paths[name] = data_path
        else:
            logger.warning(
                "Name %s from file %s allready used", name, data_path
            )

    return paths


def create_config_dict(config, datafile=None, datatype=None):
    """Read config settings and make dictionary for use when exporting

    Args:
        config (dict): the settings for export of simulator results
        datafile (str, None): overule with one datafile
        datatype (str, None): overule with one datatype

    Returns:
        dict: dictionary with key as path to datafile, value as dict of
              submodule and option
    """
    # datafile can be read as list, or string which can be either folder or filepath
    logger = logging.getLogger(__file__ + ".read_config")
    logger.debug("Using extras %s", [datafile, datatype])
    logger.debug("Input config keys are %s", config.keys())

    simconfig = config.get("sim2sumo", {})
    if len(simconfig) == 0:
        logger.warning("We are starting from scratch")
    else:
        logger.debug("This is the starting point %s", simconfig)
    grid3d = simconfig.get("grid3d", False)
    if isinstance(simconfig, bool):
        simconfig = {}
    datafiles = find_datafiles(datafile, simconfig)
    paths = find_datafile_paths()
    logger.debug("Datafiles %s", datafiles)
    if isinstance(datafiles, dict):
        outdict = create_config_dict_from_dict(datafiles, paths, grid3d)
    else:
        outdict = create_config_dict_from_list(
            datatype, simconfig, datafiles, paths, grid3d
        )
    logger.debug("Returning %s", outdict)
    return outdict


def create_config_dict_from_list(
    datatype, simconfig, datafiles, paths, grid3d
):
    """Prepare dictionary from list of datafiles and simconfig

    Args:
        datatype (str): datatype to overule input
        simconfig (dict): dictionary with input for submods and options
        datafiles (list): list of datafiles
        paths (dict): list of all relevant datafiles

    Returns:
        dict: results as one unified dictionary
    """
    logger = logging.getLogger(__file__ + ".prepare_list_for_sendoff")
    logger.debug("Simconfig input is: %s", simconfig)

    if datatype is None:
        submods = simconfig.get("datatypes", ["summary", "rft", "satfunc"])

        if submods == "all":
            submods = SUBMODULES
    else:
        submods = [datatype]

    logger.debug("Submodules to extract with: %s", submods)
    outdict = {}
    options = simconfig.get("options", {"arrow": True})

    for datafile in datafiles:
        datafile_path = find_full_path(datafile, paths)
        if datafile_path is None:
            continue
        outdict[datafile_path] = {}
        try:
            suboptions = submods.values()
        except AttributeError:
            suboptions = options
        for submod in submods:
            outdict[datafile_path][submod] = filter_options(submod, suboptions)
        outdict[datafile_path]["grid3d"] = grid3d

    return outdict


def create_config_dict_from_dict(datafiles, paths, grid3d):
    """Prepare dictionary containing datafile information

    Args:
        datafiles (dict): the dictionary of datafiles
        paths (dict): list of all relevant datafiles

    Returns:
        dict: results as one unified dictionary
    """
    logger = logging.getLogger(__file__ + ".prepare_dict_for_sendoff")

    outdict = {}
    for datafile in datafiles:
        datafile_path = find_full_path(datafile, paths)
        if datafile_path not in paths.values():
            logger.warning("%s not contained in paths", datafile_path)
        if datafile_path is None:
            continue
        outdict[datafile_path] = {}
        if datafile_path is None:
            continue
        try:
            for submod, options in datafiles[datafile].items():
                logger.debug(
                    "%s submod %s:\noptions: %s",
                    datafile_path,
                    submod,
                    options,
                )
                outdict[datafile_path][submod] = filter_options(
                    submod, options
                )
        except AttributeError:
            for submod in datafiles[datafile]:
                outdict[datafile_path][submod] = {}
        outdict[datafile_path]["grid3d"] = grid3d
    logger.debug("Returning %s", outdict)
    return outdict


def find_datafiles(seedpoint, simconfig):
    """Find all relevant paths that can be datafiles

    Args:
        seedpoint (str, list): path of datafile, or list of folders where one can find one
        simconfig (dict): the sim2sumo config settings

    Returns:
        list: list of datafiles to interrogate
    """

    logger = logging.getLogger(__file__ + ".find_datafiles")
    datafiles = []
    seedpoint = simconfig.get("datafile", seedpoint)
    if seedpoint is None:
        datafiles = find_datafiles_no_seedpoint()

    elif isinstance(seedpoint, (str, Path)):
        logger.debug("Using this string %s to find datafile(s)", seedpoint)
        datafiles.append(seedpoint)
    elif isinstance(seedpoint, list):
        logger.debug("%s is list", seedpoint)
        datafiles.extend(seedpoint)
    else:
        datafiles = seedpoint
    logger.debug("Datafile(s) to use %s", datafiles)
    return datafiles


def find_datafiles_no_seedpoint():
    """Find datafiles relative to an ert runpath

    Returns:
        list: The datafiles found
    """
    logger = logging.getLogger(__file__ + ".find_datafiles_no_seedpoint")
    cwd = Path().cwd()
    logger.info("Looking for files in %s", cwd)
    valid_filetypes = [".afi", ".DATA", ".in"]
    datafiles = list(
        filter(
            lambda file: file.suffix in valid_filetypes, cwd.glob("*/*/*.*")
        )
    )
    logger.debug("Found the following datafiles %s", datafiles)
    return datafiles


class Dispatcher:
    """Controls upload to sumo"""

    def __init__(self, datafile, env, token=None):
        self._logger = logging.getLogger(__name__ + ".Dispatcher")
        self._limit_percent = 0.5
        self._parentid = get_case_uuid(datafile)
        self._conn = SumoConnection(env=env, token=token)
        self._env = env
        self._mem_limit = (
            psutil.virtual_memory().available * self._limit_percent
        )

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
        nodisk_upload(self._objects, self._parentid, connection=self._conn)
        self._objects = []
        self._mem_count = 0

    def finish(self):
        """Cleanup"""
        self._logger.info("Final stretch")
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
    logger = logging.getLogger(__name__ + ".generate_meta")
    logger.info("Obj of type: %s", type(obj))
    logger.info("Generating metadata")
    logger.info("Content: %s", content)
    logger.debug("Config: %s", config)
    logger.debug("datafile_path: %s", datafile_path)
    logger.info("tagname: %s", tagname)
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
    logger.debug("Generated metadata are:\n%s", metadata)
    return metadata



def convert_2_sumo_file(obj, converter, metacreator, meta_args):
    """Convert object to sumo file

    Args:
        obj (object): the object
        converter (func): function to convert to bytestring
        metacreator (func): the function that creates the metadata
        meta_args (iterable): arguments for generating metadata

    Returns:
        SumoFile: file containing obj
    """
    logger = logging.getLogger(__name__ + ".convert_2_sumo_file")
    logger.debug("Obj type: %s", type(obj))
    logger.debug("Convert function %s", converter)
    logger.debug("Meta function %s", metacreator)
    logger.debug("Arguments for creating metadata %s", meta_args)
    if obj is None:
        logger.warning("Nothing to do with None object")
        return obj
    bytestring = converter(obj)
    metadata = metacreator(*meta_args)
    logger.debug("Metadata created")
    assert isinstance(
        metadata, dict
    ), f"meta should be dict, but is {type(metadata)}"
    assert isinstance(
        bytestring, bytes
    ), f"bytestring should be bytes, but is {type(bytestring)}"
    sumo_file = FileOnJob(bytestring, metadata)
    logger.debug("Init of sumo file")
    sumo_file.path = metadata["file"]["relative_path"]
    sumo_file.metadata_path = ""
    sumo_file.size = len(sumo_file.byte_string)
    logger.debug("Returning from func")
    return sumo_file


def nodisk_upload(files, parent_id, env="prod", connection=None):
    """Upload files to sumo

    Args:
        files (list): should contain only SumoFile objects
        parent_id (str): uuid of parent object
        connection (str): client to upload with
    """
    logger = logging.getLogger(__name__ + ".nodisk_upload")
    logger.info("%s files to upload", len(files))
    logger.info("Uploading to parent %s", parent_id)
    if len(files) > 0:
        if connection is None:
            connection = SumoConnection(env=env)
        status = upload_files(files, parent_id, connection)
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
    logger = logging.getLogger(__name__ + ".give_name")
    logger.info("Giving name from path %s", datafile_path)
    datafile_path_posix = Path(datafile_path)
    base_name = datafile_path_posix.name.replace(
        datafile_path_posix.suffix, ""
    )
    while base_name[-1].isdigit() or base_name.endswith("-"):
        base_name = base_name[:-1]
    logger.info("Returning name %s", base_name)
    return base_name
