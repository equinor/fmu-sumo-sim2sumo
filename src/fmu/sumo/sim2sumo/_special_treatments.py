"""Special treatment of some options used in res2df"""

import contextlib
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
        frame["DATE"] = pd.to_datetime(frame["DATE"])
    scheme = []
    for column_name in frame.columns:
        if pd.api.types.is_numeric_dtype(frame[column_name]):
            scheme.append((column_name, pa.float32()))
        else:
            scheme.append(
                (column_name, standard.get(column_name, pa.string()))
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
    import_path = "res2df." + submod
    func = importlib.import_module(import_path).df
    returns = {
        "extract": func,
        "options": tuple(
            name
            for name in signature(func).parameters
            if name not in {"deck", "eclfiles"}
        ),
        "arrow_convertor": find_arrow_convertor(import_path),
    }

    return returns


def _define_submodules():
    """Fetch all submodules

    Returns:
        list: list of submodules
    """
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
        with contextlib.suppress(AttributeError):
            submodules[submod] = find_functions_and_docstring(submod_string)

    return tuple(submodules.keys()), submodules


def delete_unwanted_rft_files(frame):
    """Utility function to tidy up mess from res2df for rft

    Args:
        frame (pd.DataFrame): the dataframe fixed with no WELLETC
    """
    # res2df creates three files for rft data, see unwanted list below
    logger = logging.getLogger(__file__ + ".delete_unwanted_rft_files")
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
        frame = frame.drop(["WELLETC"], axis=1)

    return frame


SUBMODULES, SUBMOD_DICT = _define_submodules()
DEFAULT_SUBMODULES = ["summary", "rft", "satfunc"]
DEFAULT_RST_PROPS = ["SGAS", "SOIL", "SWAT", "PRESSURE"]


def vfp_to_arrow_dict(datafile, options):
    """Generate dictionary with vfp arrow tables

    Args:
        datafile (str): The datafile to extract from
        options (dict): options for extraction

    Returns:
        tuple: vfp keyword, then dictionary with key: table_name, value: table
    """
    filepath_no_suffix = Path(datafile).with_suffix("")
    resdatafiles = res2df.ResdataFiles(filepath_no_suffix)
    vfp_dict = {}
    keyword = options.get("keyword", ["VFPPROD", "VFPINJ"])
    vfpnumbers = options.get("vfpnumbers", None)
    keywords = [keyword] if isinstance(keyword, str) else keyword

    for keyword in keywords:
        vfp_dict[keyword] = res2df.vfp._vfp.pyarrow_tables(
            resdatafiles.get_deck(), keyword=keyword, vfpnumbers_str=vfpnumbers
        )
    return vfp_dict
