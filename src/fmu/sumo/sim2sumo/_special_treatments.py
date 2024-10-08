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
    logger = logging.getLogger(__file__ + ".convert_to_arrow")
    logger.debug("!!!!Using convert to arrow!!!")
    standard = {"DATE": pa.timestamp("ms")}
    if "DATE" in frame.columns:
        frame["DATE"] = pd.to_datetime(frame["DATE"])
    scheme = []
    for column_name in frame.columns:
        if pd.api.types.is_string_dtype(frame[column_name]):
            scheme.append((column_name, pa.string()))
        else:
            scheme.append(
                (column_name, standard.get(column_name, pa.float32()))
            )
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

    import_path = "res2df." + submod
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
            logger.debug("Assigning %s to %s", submodules[submod], submod)
        except AttributeError:
            logger.debug("No df function in %s", submod_path)

    logger.debug(
        "Returning the submodule names as a list: %s ", submodules.keys()
    )
    logger.debug(
        "Returning the submodules extra args as a dictionary: %s ", submodules
    )

    return tuple(submodules.keys()), submodules


def find_md_log(submod, options):
    """Search options for md_log_file

    Args:
        submod (str): submodule
        options (dict): the dictionary to check

    Returns:
        str|None: whatever contained in md_log_file
    """
    logger = logging.getLogger(__file__ + ".find_md_log")
    if submod != "rft":
        return None
    # Special treatment of argument md_log_file
    md_log_file = options.get("md_log_file", None)
    try:
        del options["md_log_file"]
    except KeyError:
        logger.debug("No md log provided")

    return md_log_file


def complete_rft(frame, md_log_file):
    """Utility function to tidy up mess from res2df for rft

    Args:
        frame (pd.DataFrame): the dataframe fixed with no WELLETC
        md_log_file (str): file with md log file
    """
    # res2df creates three files for rft data, see unwanted list below
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

    if md_log_file is not None:
        frame = add_md_to_rft(frame, md_log_file)

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
    filepath_no_suffix = Path(datafile).with_suffix("")
    resdatafiles = res2df.ResdataFiles(filepath_no_suffix)
    vfp_dict = {}
    keyword = options.get("keyword", ["VFPPROD", "VFPINJ"])
    vfpnumbers = options.get("vfpnumbers", None)
    if isinstance(keyword, str):
        keywords = [keyword]
    else:
        keywords = keyword

    for keyword in keywords:
        vfp_dict[keyword] = res2df.vfp._vfp.pyarrow_tables(
            resdatafiles.get_deck(), keyword=keyword, vfpnumbers_str=vfpnumbers
        )
    return vfp_dict


def add_md_to_rft(rft_table, md_file_path):
    """Merge md data with rft table

    Args:
        rft_table (pd.DataFrame): the rft dataframe
        md_file_path (str): path to file with md data

    Raises:
        FileNotFoundError: if md_file_path does not point to existing file

    Returns:
        pd.Dataframe: the merged results
    """
    logger = logging.getLogger(__file__ + ".add_md_to_rft")
    logger.debug("Head of rft table prior to merge:\n %s", rft_table.head())

    try:
        md_table = pd.read_csv(md_file_path)
    except FileNotFoundError as fnfe:
        raise FileNotFoundError(
            f"There is no md file called {md_file_path}"
        ) from fnfe

    xtgeo_index_names = ["I_INDEX", "J_INDEX", "K_INDEX"]
    rft_index_names = ["CONIPOS", "CONJPOS", "CONKPOS"]
    # for grid indeces xtgeo starts from 0, res2df from 1
    md_table[xtgeo_index_names] += 1
    md_table[xtgeo_index_names] = md_table[xtgeo_index_names].astype(int)
    xtgeo_to_rft_names = dict(zip(xtgeo_index_names, rft_index_names))
    logger.debug(
        "Datatypes, md_table: %s, rft_table: %s",
        md_table[xtgeo_index_names].dtypes,
        rft_table[rft_index_names].dtypes,
    )
    logger.debug(
        "Shapes before merge rft: %s, md: %s", rft_table.shape, md_table.shape
    )
    md_table.rename(xtgeo_to_rft_names, axis=1, inplace=True)
    logger.debug("Header of md table after rename %s", md_table.head())
    rft_table = pd.merge(rft_table, md_table, on=rft_index_names, how="left")
    logger.debug("Shape after merge %s", rft_table.shape)
    logger.debug("Shape with no nans %s", rft_table.dropna().shape)
    logger.debug("Head of merged table to return:\n %s", rft_table.head())

    return rft_table
