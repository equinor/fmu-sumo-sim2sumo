"""Control all upload done by sim2sumo"""

import argparse
import logging

from .grid3d import upload_simulation_runs
from .tables import upload_tables
from .common import yaml_load, Dispatcher, create_config_dict
from ._special_treatments import give_help, SUBMODULES


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
        default=False,
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
    args = parse_args()
    if args.help_on:
        print(give_help(args.help_on))
    else:
        config = yaml_load(args.config_path)
        config["file_path"] = args.config_path
        sim2sumoconfig = create_config_dict(
            config, args.datafile, args.datatype
        )
        # Init of dispatcher needs one datafile to locate case uuid
        one_datafile = list(sim2sumoconfig.keys())[0]
        dispatcher = Dispatcher(one_datafile, args.env)

        upload_tables(sim2sumoconfig, config, dispatcher)

        upload_simulation_runs(sim2sumoconfig, config, dispatcher)

        dispatcher.finish()


if __name__ == "__main__":
    main()
