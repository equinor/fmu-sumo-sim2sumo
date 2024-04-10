"""Module for uploading tabular data from reservoir simulators to sumo
   Does three things:
   1. Extracts data from simulator to arrow files
   2. Adds the required metadata while exporting to disc
   3. Uploads to Sumo
"""

import argparse
import logging
import re
import sys
from pathlib import Path, PosixPath
from typing import Union

import pandas as pd
import pyarrow as pa
import res2df

from ._special_treatments import SUBMOD_DICT, SUBMODULES, convert_options, tidy

from .common import yaml_load, fix_suffix, export_object

logging.getLogger(__name__).setLevel(logging.DEBUG)

SUBMOD_CONTENT = {
    "summary": "timeseries",
    "satfunc": "relperm",
    "vfp": "lift_curves",
}
SUBMOD_CONTENT.update(
    {name: name for name in ["rft", "pvt", "transmissibilities"]}
)


def get_results(
    datafile_path: str, submod: str, print_help=False, **kwargs
) -> Union[pa.Table, pd.DataFrame]:
    """Fetch dataframe from simulator results

    Args:
        datafile_path (str): the path to the simulator datafile
        submod (str): the name of the submodule to extract with
        kwargs (dict): other options

    Returns:
        pd.DataFrame: the extracted data
    """
    logger = logging.getLogger(__file__ + ".get_dataframe")
    extract_df = SUBMOD_DICT[submod]["extract"]
    arrow = kwargs.get("arrow", True)
    datafile_path = fix_suffix(datafile_path)
    output = None
    trace = None
    if print_help:
        print(SUBMOD_DICT[submod]["doc"])
    else:
        logger.debug("Checking these passed options %s", kwargs)
        right_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key in SUBMOD_DICT[submod]["options"]
        }
        logger.debug("Exporting with arguments %s", right_kwargs)
        try:
            logger.info(
                "Extracting data from %s with %s",
                datafile_path,
                extract_df.__name__,
            )
            output = extract_df(
                res2df.ResdataFiles(datafile_path),
                **convert_options(right_kwargs),
            )
            if submod == "rft":
                output = tidy(output)
            if arrow:
                try:
                    output = SUBMOD_DICT[submod]["arrow_convertor"](output)
                except pa.lib.ArrowInvalid:
                    logger.warning(
                        "Arrow invalid, cannot convert to arrow, keeping pandas format"
                    )
                except TypeError:
                    logger.warning("Type error, cannot convert to arrow")
        except RuntimeError:
            print(give_help(None))
        except TypeError:
            trace = sys.exc_info()[1]
        except FileNotFoundError:
            trace = sys.exc_info()[1]
        except ValueError:
            trace = sys.exc_info()[1]
        if trace is not None:
            logger.warning(
                "Trace: %s, \nNo results produced ",
                trace,
            )

    return output


def export_results(
    datafile_path: str,
    submod: str,
    config_file="fmuconfig/output/global_variables.yml",
    **kwargs,
) -> str:
    """Export csv file with specified datatype

    Args:
        datafile_path (str): the path to the simulator datafile
        submod (str): the name of the submodule to extract with

    Returns:
        str: path of export
    """
    logger = logging.getLogger(__file__ + ".export_results")
    logger.debug("Export will be using these options: %s", kwargs)
    frame = get_results(datafile_path, submod, **kwargs)

    exp_path = export_object(
        datafile_path,
        submod,
        config_file,
        frame,
        SUBMOD_CONTENT.get(submod, "property"),
    )
    return exp_path


def read_config(config, datafile=None, datatype=None):
    """Read config settings

    Args:
        config (dict): the settings for export of simulator results
        kwargs (dict): overiding settings

    Returns:
        tuple: datafiles as list, submodules to use as list, and options as kwargs
    """
    # datafile can be read as list, or string which can be either folder or filepath
    logger = logging.getLogger(__file__ + ".read_config")
    logger.debug("Input config keys are %s", config.keys())

    simconfig = config.get("sim2sumo", {})
    if isinstance(simconfig, bool):
        simconfig = {}
    datafiles = find_datafiles(datafile, simconfig)

    submods = find_datatypes(datatype, simconfig)

    options = simconfig.get("options", {"arrow": True})

    logger.info(
        "Running with: datafile(s): \n%s \n Types: \n %s \noptions:\n %s",
        datafiles,
        submods,
        options,
    )
    return datafiles, submods, options


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


def is_datafile(results: PosixPath) -> bool:
    """Filter results based on suffix

    Args:
        results (PosixPath): path to file

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

    logger = logging.getLogger(__file__ + ".find_datafiles")
    datafiles = []
    seedpoint = simconfig.get("datafile", seedpoint)
    if seedpoint is None:
        datafiles = list(filter(is_datafile, Path().cwd().glob("*/model/*.*")))

    elif isinstance(seedpoint, (str, PosixPath)):
        logger.debug("Using this string %s to find datafile(s)", seedpoint)
        seedpoint_posix = Path(seedpoint)
        if seedpoint_posix.is_file():
            logger.debug("%s is file path, will just use this one", seedpoint)
            datafiles.append(seedpoint)
    else:
        logger.debug("%s is list", seedpoint)
        datafiles.extend(seedpoint)
    logger.debug("Datafile(s) to use %s", datafiles)
    return datafiles


def export_with_config(config_path, datafile=None, datatype=None):
    """Export several datatypes with yaml config file

    Args:
        config_path (str): path to existing yaml file
        extras (dict): extra arguments

    """
    logger = logging.getLogger(__file__ + ".export_w_config")
    logger.debug("Using extras %s", [datafile, datatype])
    suffixes = set()
    export_folder = None
    export_path = None
    try:
        count = 0

        datafiles, submods, options = read_config(
            yaml_load(config_path), datafile, datatype
        )
        for datafile in datafiles:
            for submod in submods:
                logger.info("Exporting %s", submod)
                export_path = export_results(
                    datafile,
                    submod,
                    config_file=config_path,
                    **options,
                )
                count += 1
                export_path = Path(export_path)
                suffixes.add(export_path.suffix)
        try:
            export_folder = str(export_path.parent)
            logger.info("Exported %i files to %s", count, export_folder)
        except AttributeError:
            logger.warning("No results exported ")
    except FileNotFoundError:
        logger.warning("No config file at: %s", config_path)
    return export_folder, suffixes


def parse_args():
    """Parse arguments for command line tool

    Returns:
        argparse.NameSpace: the arguments parsed
    """
    logger = logging.getLogger(__file__ + ".parse_args")
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Parsing input to control export of simulator data",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.default = "execute"

    exec_parser = subparsers.add_parser("execute")
    help_parser = subparsers.add_parser("help")

    exec_parser.add_argument(
        "--config_path",
        type=str,
        help="config that controls the export",
        default="fmuconfig/output/global_variables.yml",
    )
    exec_parser.add_argument(
        "--env",
        type=str,
        help="Which sumo environment to upload to",
        default="prod",
    )
    exec_parser.add_argument(
        "--datatype",
        type=str,
        default=None,
        help="Override datatype setting, intented for testing only",
    )
    exec_parser.add_argument(
        "--datafile",
        type=str,
        default=None,
        help="Override datafile setting, intented for testing only",
    )
    help_parser.add_argument(
        "help_on",
        type=str,
        help=(
            "Use this to get documentation of one of the datatypes to upload\n"
            + f"valid options are \n{', '.join(SUBMODULES)}"
        ),
    )
    args = parser.parse_args()
    if len(vars(args)) == 1 and args.command == "execute":
        # Help out the default option, otherwise
        # NameSpace will only contain execute
        args = exec_parser.parse_args()
    logger.debug("Returning %s", args)
    return args


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


def upload_with_config(config_path, datafile, datatype, env):
    """Upload simulator results to sumo

    Args:
        config_path (str): Path to config file
        env (str, optional): The sumo environment. Defaults to "prod".
        extras (dict): extra arguments
    """
    logger = logging.getLogger(__file__ + ".upload_with_config")
    logger.debug("Executing with:")
    logger.debug("config: %s: ", config_path)
    logger.debug("Sumo env: %s: ", env)

    upload_folder, suffixes = export_with_config(
        config_path, datafile, datatype
    )
    upload(upload_folder, suffixes, env, config_path=config_path)


def main():
    """Main function to be called"""
    logger = logging.getLogger(__file__ + ".main")
    args = parse_args()
    logger.debug("Running with arguments %s", args)
    try:
        print(give_help(args.help_on))
    except AttributeError:
        logger.info("Will be extracting results")
        upload_with_config(
            args.config_path, args.datafile, args.datatype, args.env
        )


if __name__ == "__main__":
    main()
