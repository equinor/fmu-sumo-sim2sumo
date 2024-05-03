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
        self._logger.debug("Final stretch")
        self._upload()


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
    logger.debug("Config: %s", config)
    logger.debug("datafile_path: %s", datafile_path)
    logger.debug("tagname: %s", tagname)
    logger.debug("Obj of type: %s", type(obj))
    logger.debug("Content: %s", content)
    name = give_name(datafile_path)
    exd = ExportData(
        config=config,
        name=name,
        tagname=tagname,
        content=content,
    )
    metadata = exd.generate_metadata(obj)
    relative_parent = str(Path(datafile_path).parents[2]).replace(
        str(Path(datafile_path).parents[4]), ""
    )
    metadata["file"] = {
        "relative_path": f"{relative_parent}/{name}--{tagname}".lower()
    }
    return metadata


def convert_to_bytestring(converter, obj):
    """Convert what comes out of a function to byte stream

    Args:
        converter (func): the function to convert to bytestring
       obj (object): the object to be converted

    Returns:
        bytestring: the converted bytes
    """
    return converter(obj)


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
    logger.debug("Obj: %s", obj)
    logger.debug("Convert function %s", converter)
    logger.debug("Meta function %s", metacreator)
    logger.debug("Arguments for creating metadata %s", meta_args)
    bytestring = convert_to_bytestring(converter, obj)
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
    logger.debug("%s files to upload", len(files))
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
    logger.debug("Giving name from path %s", datafile_path)
    datafile_path_posix = Path(datafile_path)
    base_name = datafile_path_posix.name.replace(
        datafile_path_posix.suffix, ""
    )
    while base_name[-1].isdigit() or base_name.endswith("-"):
        base_name = base_name[:-1]
    logger.debug("Returning name %s", base_name)
    return base_name


def fix_suffix(datafile_path: str, suffix=".DATA"):
    """Check if suffix is .DATA, if not change to

    Args:
        datafile_path (PosixPath): path to check
        suffix (str): desired suffix

    Returns:
        str: the corrected path
    """
    logger = logging.getLogger(__file__ + ".fix_suffix")
    string_datafile_path = str(datafile_path)
    if not string_datafile_path.endswith(suffix):
        corrected_path = re.sub(r"\..*", suffix, string_datafile_path)
        logger.debug("Changing %s to %s", string_datafile_path, corrected_path)
        datafile_path = corrected_path
    return datafile_path
