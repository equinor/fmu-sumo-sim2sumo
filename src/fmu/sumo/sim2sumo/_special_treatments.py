"""Special treatment of some options used in ecl2df"""
from inspect import signature
import importlib
import logging
from pathlib import Path
import ecl2df
from ecl2df.common import convert_lyrlist_to_zonemap, parse_lyrfile


def _define_submodules():
    """Fetch all submodules

    Returns:
        list: list of submodules
    """

    logger = logging.getLogger(__file__ + "define_submodules")
    package_path = Path(ecl2df.__file__).parent

    submodules = {}
    for submod_path in package_path.glob("*.py"):
        submod = str(submod_path.name.replace(".py", ""))
        try:
            func = importlib.import_module("ecl2df." + submod).df
        except AttributeError:
            logger.debug("No df function in %s", submod_path)
            continue
        submodules[submod] = {"extract": func}
        submodules[submod]["options"] = tuple(
            name
            for name in signature(func).parameters.keys()
            if name not in {"deck", "eclfiles"}
        )
        submodules[submod]["doc"] = func.__doc__
        try:
            submodules[submod]["arrow_convertor"] = importlib.import_module(
                "ecl2df." + submod
            )._df2pyarrow
        except AttributeError:
            logger.info(
                "No premade function for converting to arrow in %s",
                submod_path,
            )

        logger.debug("Assigning %s to %s", submodules[submod], submod)

    logger.debug("Returning the submodule names as a list: %s ", submodules.keys())
    logger.debug("Returning the submodules extra args as a dictionary: %s ", submodules)

    return tuple(submodules.keys()), submodules


def convert_options(options):
    """Convert dictionary options further

    Args:
        options (dict): the input options

    Returns:
        dict: options after special treatment
    """
    if "zonemap" in options:
        options["zonemap"] = convert_lyrlist_to_zonemap(
            parse_lyrfile(options["zonemap"])
        )
    return options

SUBMODULES, SUBMOD_DICT = _define_submodules()

