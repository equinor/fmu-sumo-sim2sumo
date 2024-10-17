"""Control all upload done by sim2sumo"""

import argparse
import logging

from .grid3d import upload_simulation_runs
from .tables import upload_tables
from .common import yaml_load, Dispatcher, create_config_dict


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
        help="Which sumo environment to upload to",
        default="prod",
    )
    parser.add_argument("--d", help="Activate debug mode", action="store_true")
    args = parser.parse_args()
    if args.d:
        logging.basicConfig(
            level="DEBUG", format="%(name)s - %(levelname)s - %(message)s"
        )
    return args


def main():
    """Main function to be called"""
    logger = logging.getLogger(__file__ + ".main")
    args = parse_args()

    config = yaml_load(args.config_path)
    config["file_path"] = args.config_path
    sim2sumoconfig = create_config_dict(config)
    # Init of dispatcher needs one datafile to locate case uuid
    one_datafile = list(sim2sumoconfig.keys())[0]
    try:
        dispatcher = Dispatcher(
            one_datafile, args.env, config_path=args.config_path
        )
    except Exception as e:
        logger.error("Failed to create dispatcher: %s", e)
        return

    upload_tables(sim2sumoconfig, config, dispatcher)

    upload_simulation_runs(sim2sumoconfig, config, dispatcher)

    dispatcher.finish()


if __name__ == "__main__":
    main()
