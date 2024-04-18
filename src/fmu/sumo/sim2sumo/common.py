"""Common functions for the other sim2sumo modules"""

import logging
import re
from pathlib import Path

import hashlib
import yaml


from fmu.dataio import ExportData
from fmu.sumo.uploader import SumoConnection
from fmu.sumo.uploader.scripts.sumo_upload import sumo_upload_main
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


def split_list(list_to_split: list, size: int) -> list:
    """Split list into segments

    Args:
        list_to_split (list): the list to split
        size (int): the size of each sublist

    Returns:
        list: the list of lists
    """
    list_list = []
    while len(list_to_split) > size:
        piece = list_to_split[:size]
        list_list.append(piece)
        list_to_split = list_to_split[size:]
    list_list.append(list_to_split)
    return list_list


def md5sum(bytes_string: bytes) -> str:
    """Make checksum from bytestring
    args:
    bytes_string (bytes): byte string
    returns (str): checksum
    """
    logger = logging.getLogger(__name__ + ".md5sum")
    hash_md5 = hashlib.md5()
    hash_md5.update(bytes_string)
    checksum = hash_md5.hexdigest()
    logger.debug("Checksum %s", checksum)

    return checksum


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
    exd = ExportData(
        config=config,
        name=name,
        tagname=tagname,
        content=content,
    )
    metadata = exd.generate_metadata(obj)
    metadata["file"] = {
        "relative_path": f"{str(datafile_path.parents[2])}/{name}--{tagname}"
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


def convert_2_sumo_file(obj, metadata, converter):
    """Convert object to sumo file

    Args:
        obj (object): the object
        metadata (dict): the metadata
        converter (func): function to convert to bytestring

    Returns:
        SumoFile: file containing obj
    """
    sumo_file = FileOnJob(convert_to_bytestring(converter, obj), metadata)
    sumo_file.path = metadata["file"]["relative_path"]
    sumo_file.metadata_path = ""
    sumo_file.size = len(sumo_file.byte_string)
    return sumo_file


def nodisk_upload(files, parent_id, env):
    """Upload files to sumo

    Args:
        files (list): should contain only SumoFile objects
        parent_id (str): uuid of parent object
        env (str): what environment to upload to
    """
    connection = SumoConnection(env=env)
    upload_files(files, parent_id, connection)


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


def export_object(datafile_path, tagname, config, obj, content):
    """Export object with fmu.dataio

    Args:
        datafile_path (str): path to datafile
        tagname (str): tagname to use
        config (dict): config with metadata
        obj (object): object fit for dataio
        contents (str): content to set for dataio

    Returns:
        str: path for exported path
    """
    logger = logging.getLogger(__file__ + ".export_object")
    name = give_name(datafile_path)
    if obj is not None:
        logger.debug("Reading global variables from %s", config)
        exp = ExportData(
            config=config,
            name=name,
            tagname=tagname,
            content=content,
        )
        exp_path = exp.export(obj)
        logger.info("Exported %s to path %s", type(obj), exp_path)
    else:
        exp_path = "Nothing produced"
        logger.warning(
            "Something went wrong, so no export happened for %s, %s",
            name,
            tagname,
        )

    return exp_path


def upload(
    upload_folder,
    suffixes,
    search_element,
    env="prod",
    threads=5,
    start_del="real",
    config_path="fmuconfig/output/global_variables.yml",
):
    """Upload to sumo

    Args:
        upload_folder (str): folder to upload from
        suffixes (set, list, tuple): suffixes to include in upload
        search_element(str): string to be part of search string
        env (str, optional): sumo environment to upload to. Defaults to "prod".
        threads (int, optional): Threads to use in upload. Defaults to 5.
    """
    logger = logging.getLogger(__file__ + ".upload")
    logger.debug("Sending in path %s", str(upload_folder))
    logger.debug("Config file to marry with data %s", config_path)

    case_path = Path(re.sub(rf"\/{start_del}.*", "", str(upload_folder)))
    logger.info("Case to upload from %s", case_path)
    case_meta_path = case_path / "share/metadata/fmu_case.yml"
    logger.info("Case meta object %s", case_meta_path)

    try:
        for suffix in suffixes:
            logger.info(suffix)
            upload_search = f"{upload_folder}/{search_element}*{suffix}"
            logger.info("Upload folder %s", upload_search)
            sumo_upload_main(
                case_path,
                upload_search,
                env,
                case_meta_path,
                threads,
                config_path,
            )
            logger.debug("Uploaded")
    except TypeError:
        logger.warning("Nothing to export..")
