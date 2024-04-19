"""Control all upload done by sim2sumo"""

import argparse
import logging
from pathlib import Path, PosixPath

from .grid3d import upload_simulation_runs
from .tables import upload_tables
from .common import yaml_load
from ._special_treatments import give_help, SUBMODULES


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

    parser.add_argument(
        "--config_path",
        type=str,
        help="config that controls the export",
        default="fmuconfig/output/global_variables.yml",
    )
    parser.add_argument(
        "--env",
        type=str,
        help="Which sumo environment to upload to",
        default="prod",
    )
    parser.add_argument(
        "--datatype",
        type=str,
        default=None,
        help="Override datatype setting, intented for testing only",
    )
    parser.add_argument(
        "--datafile",
        type=str,
        default=None,
        help="Override datafile setting, intented for testing only",
    )
    parser.add_argument(
        "--help_on",
        type=str,
        help=(
            "Use this to get documentation of one of the datatypes to upload\n"
            + f"valid options are \n{', '.join(SUBMODULES)}"
        ),
        default="No help",
    )
    parser.add_argument("--d", help="Activate debug mode", action="store_true")
    args = parser.parse_args()
    if args.d:
        logging.basicConfig(
            level="DEBUG", format="%(name)s - %(levelname)s - %(message)s"
        )
    logger.debug("Returning args %s", vars(args))
    return args


def read_config(config, datafile=None, datatype=None):
    """Read config settings

    Args:
        config (dict): the settings for export of simulator results
        kwargs (dict): overiding settings

    Returns:
        dict: datafiles as list, submodules to use as list, and options as kwargs
    """
    # datafile can be read as list, or string which can be either folder or filepath
    logger = logging.getLogger(__file__ + ".read_config")
    logger.debug("Using extras %s", [datafile, datatype])
    logger.debug("Input config keys are %s", config.keys())

    simconfig = config.get("sim2sumo", {})
    if isinstance(simconfig, bool):
        simconfig = {}
    datafiles = find_datafiles(datafile, simconfig)

    submods = find_datatypes(datatype, simconfig)

    options = simconfig.get("options", {"arrow": True})

    grid3d = simconfig.get("grid3d", True)
    logger.info(
        "Running with: datafile(s): \n%s \n Types: \n %s \noptions:\n %s",
        datafiles,
        submods,
        options,
    )
    outdict = {
        "datafiles": datafiles,
        "submods": submods,
        "options": options,
        "grid3d": grid3d,
    }
    return outdict


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


def main():
    """Main function to be called"""
    logger = logging.getLogger(__file__ + ".main")
    args = parse_args()
    logger.debug("Running with arguments %s", args)
    if args.help_on != "No help":
        print(give_help(args.help_on))
    else:
        logger.info("Will be extracting results")
        config = yaml_load(args.config_path)
        config["file_path"] = args.config_path
        logger.debug("Added file_path, and config keys are %s", config.keys())
        sim2sumoconfig = read_config(config, args.datafile, args.datatype)
        if "grid3d" in sim2sumoconfig:
            logger.debug("Extracting 3dgrid(s) with properties")
            upload_simulation_runs(
                sim2sumoconfig["datafiles"], config, args.env
            )

        logger.debug("Extracting tables")
        upload_tables(sim2sumoconfig, config, args.env)


if __name__ == "__main__":
    main()
