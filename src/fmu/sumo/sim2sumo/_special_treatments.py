"""Special treatment of some options used in ecl2df"""
from inspect import signature
import importlib
import logging
from pathlib import Path
import ecl2df
from ecl2df.common import convert_lyrlist_to_zonemap, parse_lyrfile
import pandas as pd
import pyarrow as pa

logging.getLogger(__name__).setlevel(logging.DEBUG)

def convert_to_arrow(frame):
    """Convert pd.DataFrame to arrow

    Args:
        frame (pd.DataFrame): the frame to convert

    Returns:
        pa.Table: the converted dataframe
    """
    logger = logging.getLogger(__file__ + ".convert_to_arrow")
    logger.debug("!!!!Using convert to arrow!!!")
    standard = {"DATE": pa.timestamp("ms")}
    if "DATE" in frame.columns:
        frame["DATE"] = pd.to_datetime(frame["DATE"], infer_datetime_format=True)
    scheme = []
    for column_name in frame.columns:
        if pd.api.types.is_string_dtype(frame[column_name]):
            scheme.append((column_name, pa.string()))
        else:
            scheme.append((column_name, standard.get(column_name, pa.float32())))
    logger.debug(scheme)
    table = pa.Table.from_pandas(frame, schema=pa.schema(scheme))
    return table


def find_arrow_convertor(path):
    """Find function for converting pandas dataframe to arrow

    Args:
        path (str): path to where to look for function

    Returns:
        function: function for converting to arrow
    """
    logger = logging.getLogger(__file__ + ".find_arrow_convertor")
    try:
        func = importlib.import_module(path)._df2pyarrow
    except AttributeError:
        logger.info(
            "No premade function for converting to arrow in %s",
            path,
        )
        func = convert_to_arrow

    return func


def find_functions_and_docstring(submod):
    """Find functions for extracting and converting from eclipse native

    Args:
        submod (str): path to where to look for function

    Returns:
        dictionary: includes functions and doc string
    """
    logger = logging.getLogger(__file__ + ".find_func_and_info")

    import_path = "ecl2df." + submod
    func = importlib.import_module(import_path).df
    logger.debug("Assigning %s to %s", func.__name__, submod)
    returns = {
        "extract": func,
        "options": tuple(
            name
            for name in signature(func).parameters.keys()
            if name not in {"deck", "eclfiles"}
        ),
        "arrow_convertor": find_arrow_convertor(import_path),
        "doc": func.__doc__,
    }

    return returns


def _define_submodules():
    """Fetch all submodules

    Returns:
        list: list of submodules
    """

    logger = logging.getLogger(__file__ + "define_submodules")
    package_path = Path(ecl2df.__file__).parent

    submodules = {}
    submod_paths = list(package_path.glob("*.py"))
    # vfp breakes the pattern
    submod_paths.append("_vfp.py")
    for submod_path in submod_paths:
        try:
            submod_string = str(submod_path.name.replace(".py", ""))
            submod = submod_string
        except AttributeError:
            submod_string = "vfp._vfp"
            submod = "vfp"
        try:
            submodules[submod] = find_functions_and_docstring(submod_string)
            logger.debug("Assigning %s to %s", submodules[submod], submod)
        except AttributeError:
            logger.debug("No df function in %s", submod_path)

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


def tidy(frame):
    """Utility function to tidy up mess from ecl2df for rft

    Args:
        frame (pd.DataFrame): the dataframe fixed with no WELLETC
    """
    # Ecl2df creates three files for rft data, see unwanted list below
    logger = logging.getLogger(__file__ + ".tidy")
    unwanteds = ["seg.csv", "con.csv", "icd.csv"]
    cwd = Path().cwd()
    for unwanted in unwanteds:
        unwanted_posix = cwd / unwanted
        if unwanted_posix.is_file():
            logger.info(
                "Deleting unwanted file from rft export %s",
                str(unwanted_posix),
            )
            unwanted_posix.unlink()
    if "WELLETC" in frame.columns:
        frame.drop(["WELLETC"], axis=1, inplace=True)

    return frame


SUBMODULES, SUBMOD_DICT = _define_submodules()
