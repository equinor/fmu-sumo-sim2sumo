"""Special treatment of some options used in res2df"""

import importlib
import logging
from inspect import signature
from pathlib import Path

import pandas as pd
import pyarrow as pa
import res2df

logging.getLogger(__name__).setLevel(logging.DEBUG)


def convert_to_arrow(frame):
    """Convert pd.DataFrame to arrow

    Args:
        frame (pd.DataFrame): the frame to convert

    Returns:
        pa.Table: the converted dataframe
    """
    standard = {"DATE": pa.timestamp("ms")}
    if "DATE" in frame.columns:
        frame["DATE"] = pd.to_datetime(
            frame["DATE"], infer_datetime_format=True
        )
    scheme = []
    for column_name in frame.columns:
        if pd.api.types.is_string_dtype(frame[column_name]):
            scheme.append((column_name, pa.string()))
        else:
            scheme.append(
                (column_name, standard.get(column_name, pa.float32()))
            )
    table = pa.Table.from_pandas(frame, schema=pa.schema(scheme))
    return table


def find_arrow_convertor(path):
    """Find function for converting pandas dataframe to arrow

    Args:
        path (str): path to where to look for function

    Returns:
        function: function for converting to arrow
    """
    try:
        func = importlib.import_module(path)._df2pyarrow
    except AttributeError:
        func = convert_to_arrow

    return func


def find_functions_and_docstring(submod):
    """Find functions for extracting and converting from eclipse native

    Args:
        submod (str): path to where to look for function

    Returns:
        dictionary: includes functions and doc string
    """
    import_path = "res2df." + submod
    func = importlib.import_module(import_path).df
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
    package_path = Path(res2df.__file__).parent

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
        except AttributeError:
            logger.debug("No df function in %s", submod_path)

    return tuple(submodules.keys()), submodules


def tidy(frame):
    """Utility function to tidy up mess from res2df for rft

    Args:
        frame (pd.DataFrame): the dataframe fixed with no WELLETC
    """
    # res2df creates three files for rft data, see unwanted list below
    unwanteds = ["seg.csv", "con.csv", "icd.csv"]
    cwd = Path().cwd()
    for unwanted in unwanteds:
        unwanted_posix = cwd / unwanted
        if unwanted_posix.is_file():
            unwanted_posix.unlink()
    if "WELLETC" in frame.columns:
        frame.drop(["WELLETC"], axis=1, inplace=True)

    return frame


SUBMODULES, SUBMOD_DICT = _define_submodules()


def vfp_to_arrow_dict(datafile, options):
    """Generate dictionary with vfp arrow tables

    Args:
        datafile (str): The datafile to extract from
        options (dict): options for extraction

    Returns:
        tuple: vfp keyword, then dictionary with key: table_name, value: table
    """
    resdatafiles = res2df.ResdataFiles(datafile)
    keyword = options.get("keyword", "VFPPROD")
    vfpnumbers = options.get("vfpnumbers", None)
    arrow_tables = res2df.vfp._vfp.pyarrow_tables(
        resdatafiles.get_deck(), keyword=keyword, vfpnumbers_str=vfpnumbers
    )
    return keyword, arrow_tables


def give_help(submod, only_general=False):
    """Give descriptions of variables available for submodule

    Args:
        submod (str): submodule

    Returns:
        str: description of submodule input
    """
    general_info = """
    This utility uses the library ecl2csv, but uploads directly to sumo. Required options are:
    A config file in yaml format, where you specifiy the variables to extract. What is required
    is a keyword in the config called "sim2simo". under there you have three optional arguments:
    * datafile: this can be a string, a list, or it can be absent altogether
    * datatypes: this needs to be a list, or non existent
    * options: The options are listed below in the original documentation from ecl2csv. The eclfiles
               option is replaced with what is under datafile

    """
    if submod is None:
        only_general = True
    if only_general:
        text_to_return = general_info
    else:
        try:
            text_to_return = general_info + SUBMOD_DICT[submod]["doc"]
        except KeyError:
            text_to_return = (
                f"subtype {submod} does not exist!!, existing options:\n"
                + "\n".join(SUBMODULES)
            )

    return text_to_return
