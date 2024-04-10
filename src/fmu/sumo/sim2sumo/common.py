"""Common functions for the other sim2sumo modules"""

import logging
import re
from pathlib import Path
import yaml


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
    datafile_path_posix = Path(datafile_path)
    base_name = datafile_path_posix.name.replace(
        datafile_path_posix.suffix, ""
    )
    while base_name[-1].isdigit() or base_name.endswith("-"):
        base_name = base_name[:-1]
    return base_name


def fix_suffix(datafile_path: str):
    """Check if suffix is .DATA, if not change to

    Args:
        datafile_path (PosixPath): path to check

    Returns:
        str: the corrected path
    """
    logger = logging.getLogger(__file__ + ".fix_suffix")
    string_datafile_path = str(datafile_path)
    if not string_datafile_path.endswith(".DATA"):
        corrected_path = re.sub(r"\..*", ".DATA", string_datafile_path)
        logger.debug("Changing %s to %s", string_datafile_path, corrected_path)
        datafile_path = corrected_path
    return datafile_path
