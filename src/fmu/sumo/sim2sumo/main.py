"""Control all upload done by sim2sumo"""

import argparse
import logging
from os import environ

from .grid3d import upload_simulation_runs
from .tables import upload_tables
from .common import yaml_load, Dispatcher, create_config_dict


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
    parser.add_argument("--d", help="Activate debug mode", action="store_true")
    args = parser.parse_args()
    if args.d:
        logging.basicConfig(
            level="DEBUG", format="%(name)s - %(levelname)s - %(message)s"
        )
    logger.debug("Returning args %s", vars(args))
    return args


# fmu-dataio needs these when creating metadata
REQUIRED_ENV_VARS = ["_ERT_EXPERIMENT_ID", "_ERT_RUNPATH"]


def main():
    """Main function to be called"""
    logger = logging.getLogger(__file__ + ".main")

    missing = 0
    for envVar in REQUIRED_ENV_VARS:
        if environ.get(envVar) is None:
            print(f"Required environment variable {envVar} is not set.")
            missing += 1

    if missing > 0:
        print(
            "Required ERT environment variables not found. This can happen if sim2sumo was called outside the ERT context. Stopping."
        )
        exit()

    args = parse_args()
    logger.debug("Running with arguments %s", args)

    logger.info("Will be extracting results")
    config = yaml_load(args.config_path)
    config["file_path"] = args.config_path
    logger.debug("Added file_path, and config keys are %s", config.keys())
    try:
        sim2sumoconfig = create_config_dict(config)
    except Exception as e:
        logger.error("Failed to create config dict: %s", e)
        return
    # Init of dispatcher needs one datafile to locate case uuid
    one_datafile = list(sim2sumoconfig.keys())[0]
    try:
        dispatcher = Dispatcher(
            one_datafile, args.env, config_path=args.config_path
        )
    except Exception as e:
        logger.error("Failed to create dispatcher: %s", e)
        return

    logger.debug("Extracting tables")
    upload_tables(sim2sumoconfig, config, dispatcher)

    logger.debug("Extracting 3dgrid(s) with properties")
    upload_simulation_runs(sim2sumoconfig, config, dispatcher)

    dispatcher.finish()


if __name__ == "__main__":
    main()
