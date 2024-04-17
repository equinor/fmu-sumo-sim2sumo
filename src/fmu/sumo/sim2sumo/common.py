"""Common functions for the other sim2sumo modules"""

import logging
import re
from pathlib import Path

import yaml
from fmu.dataio import ExportData
from fmu.sumo.uploader.scripts.sumo_upload import sumo_upload_main


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


def export_object(datafile_path, tagname, config_file, obj, content):
    """Export object with fmu.dataio

    Args:
        datafile_path (str): path to datafile
        tagname (str): tagname to use
        config_file (str): config file with metadata
        obj (object): object fit for dataio
        contents (str): content to set for dataio

    Returns:
        str: path for exported path
    """
    logger = logging.getLogger(__file__ + ".export_object")
    name = give_name(datafile_path)
    if obj is not None:
        logger.debug("Reading global variables from %s", config_file)
        cfg = yaml_load(config_file)
        exp = ExportData(
            config=cfg,
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
