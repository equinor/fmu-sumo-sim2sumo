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
    convert_options,
    SUBMOD_DICT,
    SUBMODULES,
)


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
    """Filter options sendt to res2df per given submodule

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
        if (key in submod_options) or key == "arrow"
    }
    filtered["arrow"] = kwargs.get("arrow", True)
    # Arrow is not an argument to df functions utilized, therefore
    # it needs to be re added here
    non_opions = [key for key in kwargs if key not in filtered]
    if len(non_opions) > 0:
        logger.warning(
            "Filtered out options %s for %s, these are not valid",
            non_opions,
            submod,
        )
    return convert_options(filtered)


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
        logger.warning(
            f"Datafile {datafile} with name {data_name} not found in {paths}"
        )
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
            logger.warning(f"Name {name} from file {data_path} already used")

    return paths


def prepare_for_sendoff(config, datafile=None, datatype=None):
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
    simconfig = config.get("sim2sumo", {})
    grid3d = simconfig.get("grid3d", False)
    if isinstance(simconfig, bool):
        simconfig = {}
    datafiles = find_datafiles(datafile, simconfig)
    paths = find_datafile_paths()
    if isinstance(datafiles, dict):
        outdict = prepare_dict_for_sendoff(datafiles, paths, grid3d)
    else:
        outdict = prepare_list_for_sendoff(
            datatype, simconfig, datafiles, paths, grid3d
        )
    return outdict


def prepare_list_for_sendoff(datatype, simconfig, datafiles, paths, grid3d):
    """Prepare dictionary from list of datafiles and simconfig

    Args:
        datatype (str): datatype to overule input
        simconfig (dict): dictionary with input for submods and options
        datafiles (list): list of datafiles
        paths (dict): list of all relevant datafiles

    Returns:
        dict: results as one unified dictionary
    """
    submods = find_datatypes(datatype, simconfig)
    outdict = {}
    options = simconfig.get("options", {"arrow": True})

    for datafile in datafiles:
        datafile_path = find_full_path(datafile, paths)
        if datafile_path is None:
            continue
        outdict[datafile_path] = {}
        for submod in submods:
            outdict[datafile_path][submod] = filter_options(submod, options)

        outdict[datafile_path]["grid3d"] = grid3d

    return outdict


def prepare_dict_for_sendoff(datafiles, paths, grid3d):
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
            logger.warning(f"{datafile_path} not contained in paths")
        if datafile_path is None:
            continue
        outdict[datafile_path] = {}
        if datafile_path is None:
            continue
        try:
            for submod, options in datafiles[datafile].items():
                outdict[datafile_path][submod] = filter_options(
                    submod, options
                )
        except AttributeError:
            for submod in datafiles[datafile]:
                outdict[datafile_path][submod] = {}
        outdict[datafile_path]["grid3d"] = grid3d
    return outdict


def find_datatypes(datatype, simconfig):
    """Find datatypes to extract

    Args:
        datatype (str or None): datatype to extract
        simconfig (dict): the config file settings

    Returns:
        list or dict: data types to extract
    """

    if datatype is None:
        submods = simconfig.get("datatypes", ["summary", "rft", "satfunc"])

        if submods == "all":
            submods = SUBMODULES
    else:
        submods = [datatype]
    return submods


def is_datafile(results: Path) -> bool:
    """Filter results based on suffix

    Args:
        results (Path): path to file

    Returns:
        bool: true if correct suffix
    """
    valid = [".afi", ".DATA", ".in"]
    return results.suffix in valid


def find_datafiles(seedpoint, simconfig):
    """Find all relevant paths that can be datafiles

    Args:
        seedpoint (str, list): path of datafile, or list of folders where one can find one
        simconfig (dict): the sim2sumo config settings

    Returns:
        list: list of datafiles to interrogate
    """
    datafiles = []
    seedpoint = simconfig.get("datafile", seedpoint)
    if seedpoint is None:
        datafiles = find_datafiles_no_seedpoint()

    elif isinstance(seedpoint, (str, Path)):
        datafiles.append(seedpoint)
    elif isinstance(seedpoint, list):
        datafiles.extend(seedpoint)
    else:
        datafiles = seedpoint
    return datafiles


def find_datafiles_no_seedpoint():
    """Find datafiles relative to an ert runpath

    Returns:
        list: The datafiles found
    """
    cwd = Path().cwd()
    datafiles = list(filter(is_datafile, cwd.glob("*/*/*.*")))
    return datafiles


class Dispatcher:
    """Controls upload to sumo"""

    def __init__(self, datafile, env, token=None):
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
            if (self.mem_frac > 1) or (self._count > 100):
                self._upload()
        else:
            logger = logging.getLogger(__name__ + ".Dispatcher")
            logger.warning("File is None, not adding")

    def _upload(self):
        nodisk_upload(self._objects, self._parentid, connection=self._conn)
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
    if obj is None:
        logger.warning("Nothing to do with None object")
        return obj
    bytestring = converter(obj)
    metadata = metacreator(*meta_args)
    assert isinstance(
        metadata, dict
    ), f"meta should be dict, but is {type(metadata)}"
    assert isinstance(
        bytestring, bytes
    ), f"bytestring should be bytes, but is {type(bytestring)}"
    sumo_file = FileOnJob(bytestring, metadata)
    sumo_file.path = metadata["file"]["relative_path"]
    sumo_file.metadata_path = ""
    sumo_file.size = len(sumo_file.byte_string)
    return sumo_file


def nodisk_upload(files, parent_id, env="prod", connection=None):
    """Upload files to sumo

    Args:
        files (list): should contain only SumoFile objects
        parent_id (str): uuid of parent object
        connection (str): client to upload with
    """
    logger = logging.getLogger(__name__ + ".nodisk_upload")
    if len(files) > 0:
        if connection is None:
            connection = SumoConnection(env=env)
        return upload_files(files, parent_id, connection)
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


def fix_suffix(datafile_path: str, suffix=".DATA"):
    """Check if suffix is .DATA, if not change to

    Args:
        datafile_path (PosixPath): path to check
        suffix (str): desired suffix

    Returns:
        str: the corrected path
    """
    string_datafile_path = str(datafile_path)
    assert "." in suffix, f"suffix: needs to start with . (is {suffix})"
    if "." not in string_datafile_path:
        string_datafile_path += suffix
    if not string_datafile_path.endswith(suffix):
        corrected_path = re.sub(r"\..*", suffix, string_datafile_path)
        datafile_path = corrected_path
    return datafile_path
