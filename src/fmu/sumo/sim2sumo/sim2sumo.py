"""Export with metadata"""
import sys
import re
from typing import Union
from pathlib import Path
import logging
import argparse
import pandas as pd
import res2df
import pyarrow as pa
import yaml
from fmu.dataio import ExportData
from fmu.sumo.uploader.scripts.sumo_upload import sumo_upload_main
from ._special_treatments import SUBMODULES, SUBMOD_DICT, convert_options, tidy

logging.getLogger(__name__).setLevel(logging.DEBUG)


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
    submod_contents = {
        "summary": "timeseries",
        "satfunc": "relperm",
        "vfp": "lift_curves",
    }
    submod_contents.update(
        {name: name for name in ["rft", "pvt", "transmissibilities"]}
    )
    if frame is not None:
        logger.debug("Reading global variables from %s", config_file)
        cfg = yaml_load(config_file)
        exp = ExportData(
            config=cfg,
            name=give_name(datafile_path),
            tagname=submod,
            content=submod_contents.get(submod, "property"),
        )
        exp_path = exp.export(frame)
    else:
        exp_path = "Nothing produced"
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

    defaults = {
        "datafile": "eclipse/model/",
        "datatypes": ["summary", "rft", "satfunc"],
        "options": {"arrow": True},
    }
    try:
        simconfig = config["sim2sumo"]
    except KeyError:
        logger.warning(
            "No specification in config, will use defaults %s", defaults
        )
        simconfig = defaults
    if isinstance(simconfig, bool):
        simconfig = defaults
    if datafile is None:
        datafile = simconfig.get("datafile", "eclipse/model/")

    if isinstance(datafile, str):
        logger.debug("Using this string %s to find datafile(s)", datafile)
        datafile_posix = Path(datafile)
        if datafile_posix.is_dir():
            logger.debug("Directory, globbing for datafiles")
            datafiles = list(datafile_posix.glob("*.DATA"))

        else:
            logger.debug("File path, will just use this one")
            datafiles = [datafile]
    else:
        logger.debug("String is list")
        datafiles = datafile
    logger.debug("Datafile(s) to use %s", datafiles)
    if datatype is None:
        try:
            submods = simconfig["datatypes"]
            if submods == "all":
                submods = SUBMODULES
        except KeyError:
            submods = defaults["datatypes"]
    else:
        submods = [datatype]

    try:
        options = simconfig["options"]
        logger.info("Will use these options %s", options)
    except KeyError:
        logger.info("No special options selected")
        options = {}
    options["arrow"] = options.get("arrow", True)
    logger.info(
        "Running with: datafile(s): \n%s \n Types: \n %s \noptions:\n %s",
        datafiles,
        submods,
        options,
    )
    return datafiles, submods, options


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


def upload(
    upload_folder,
    suffixes,
    env="prod",
    threads=5,
    start_del="real",
    config_path="fmuconfig/output/global_variables.yml",
):
    """Upload to sumo

    Args:
        upload_folder (str): folder to upload from
        suffixes (set, list, tuple): suffixes to include in upload
        env (str, optional): sumo environment to upload to. Defaults to "prod".
        threads (int, optional): Threads to use in upload. Defaults to 5.
    """
    logger = logging.getLogger(__file__ + ".upload")
    try:
        case_path = Path(re.sub(rf"\/{start_del}.*", "", upload_folder))
        logger.info("Case to upload from %s", case_path)
        case_meta_path = case_path / "share/metadata/fmu_case.yml"
        logger.info("Case meta object %s", case_meta_path)
        for suffix in suffixes:
            logger.info(suffix)
            upload_search = f"{upload_folder}/*{suffix}"
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
    try:
        print(give_help(args.help_on))
    except AttributeError:
        upload_with_config(
            args.config_path, args.datafile, args.datatype, args.env
        )


if __name__ == "__main__":
    main()
