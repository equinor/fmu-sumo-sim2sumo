"""Control all upload done by sim2sumo"""

import argparse
import logging
import sys
from os import environ

from .common import Dispatcher, create_config_dict, yaml_load
from .grid3d import upload_simulation_runs
from .tables import upload_tables


def parse_args():
    """Parse arguments for command line tool

    Returns:
        argparse.NameSpace: the arguments parsed
    """
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
        help="The '--env' argument is ignored. Sumo environment must be set as an environment variable SUMO_ENV",
        default="prod",
    )
    parser.add_argument("--d", help="Activate debug mode", action="store_true")
    args = parser.parse_args()
    if args.d:
        logging.basicConfig(
            level="DEBUG", format="%(name)s - %(levelname)s - %(message)s"
        )
    return args


# e.g. _ERT_RUNPATH = ../realization-0/iter-0
# e.g. _ERT_EXPERIMENT_ID = <uuid>
# e.g. __ERT_SIMULATION_MODE = ensemble_experiment
# fmu-dataio needs these when creating metadata
REQUIRED_ENV_VARS = [
    "_ERT_EXPERIMENT_ID",
    "_ERT_RUNPATH",
    "_ERT_SIMULATION_MODE",
]


def main():
    """Main function to be called"""
    logger = logging.getLogger(__file__ + ".main")

    missing = []
    for env_var in REQUIRED_ENV_VARS:
        if env_var not in environ:
            missing.append(env_var)

    if missing:
        print(
            "Required ERT environment variables not found:"
            f"{', '.join(missing)}.\n"
            "This can happen if sim2sumo was called outside the ERT context.\n"
            "Stopping."
        )
        sys.exit()

    args = parse_args()

    fmu_config = yaml_load(args.config_path)
    fmu_config["file_path"] = args.config_path
    try:
        config = create_config_dict(fmu_config)
        if not config.get("sim2sumoconfig"):
            raise Exception("Found no files to upload")
    except Exception as e:
        logger.error("Failed to create config dict: %s", e)
        return
    # Init of dispatcher needs one datafile to locate case uuid
    one_datafile = list(config.get("sim2sumoconfig").keys())[0]
    env = environ.get("SUMO_ENV", "prod")
    try:
        dispatcher = Dispatcher(
            one_datafile, env, config_path=args.config_path
        )
    except Exception as e:
        logger.error("Failed to create dispatcher: %s", e)
        return

    # Extract tables
    upload_tables(config, dispatcher)

    # Extract 3dgrid(s) with properties
    upload_simulation_runs(config, dispatcher)

    dispatcher.finish()


if __name__ == "__main__":
    main()
