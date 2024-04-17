"""Upload tabular data from reservoir simulators to sumo
   Does three things:
   1. Extracts data from simulator to arrow files
   2. Adds the required metadata while exporting to disc
   3. Uploads to Sumo
"""

import logging
import sys
from pathlib import Path
from typing import Union

import pandas as pd
import pyarrow as pa
import res2df

from ._special_treatments import SUBMOD_DICT, convert_options, tidy
from .common import export_object, fix_suffix, upload

logging.getLogger(__name__).setLevel(logging.DEBUG)

SUBMOD_CONTENT = {
    "summary": "timeseries",
    "satfunc": "relperm",
    "vfp": "lift_curves",
}
SUBMOD_CONTENT.update(
    {name: name for name in ["rft", "pvt", "transmissibilities"]}
)


def get_table(
    datafile_path: str, submod: str, print_help=False, **kwargs
) -> Union[pa.Table, pd.DataFrame]:
    """Fetch arrow.table/pd.dataframe from simulator results

    Args:
        datafile_path (str): the path to the simulator datafile
        submod (str): the name of the submodule to extract with
        kwargs (dict): other options

    Returns:
        pd.DataFrame: the extracted data
    """
    logger = logging.getLogger(__file__ + ".get_table")
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


def export_table(
    datafile_path: str,
    submod: str,
    config,
    **kwargs,
) -> str:
    """Export csv file with specified datatype

    Args:
        datafile_path (str): the path to the simulator datafile
        submod (str): the name of the submodule to extract with

    Returns:
        str: path of export
    """
    logger = logging.getLogger(__file__ + ".export_table")
    logger.debug("Export will be using these options: %s", kwargs)
    frame = get_table(datafile_path, submod, **kwargs)

    exp_path = export_object(
        datafile_path,
        submod,
        config,
        frame,
        SUBMOD_CONTENT.get(submod, "property"),
    )
    return exp_path


def export_tables(sim2sumoconfig, config):
    """Export from all datafiles and all submods selected

    Args:
        datafiles (iterable): the datafiles to extract from
        submods (iterable): the subtypes to extract
        options (dict): the options to use
        config (dict): the metadata

    Returns:
        tuple: path to where things have been exported, suffixes of exported files
    """
    logger = logging.getLogger(__file__ + ".export_tables")
    suffixes = set()
    export_folder = None
    export_path = None

    count = 0

    for datafile in sim2sumoconfig["datafiles"]:
        for submod in sim2sumoconfig["submods"]:
            logger.info("Exporting %s", submod)
            export_path = export_table(
                datafile,
                submod,
                config,
                **sim2sumoconfig["options"],
            )
            count += 1
            export_path = Path(export_path)
            suffixes.add(export_path.suffix)
    try:
        export_folder = str(export_path.parent)
        logger.info("Exported %i files to %s", count, export_folder)
    except AttributeError:
        logger.warning("No results exported ")

    return export_folder, suffixes


def upload_tables(sim2sumoconfig, config, env):
    """Upload simulator results to sumo

    Args:
        config_path (str): Path to config file
        env (str, optional): The sumo environment. Defaults to "prod".
        extras (dict): extra arguments
    """
    logger = logging.getLogger(__file__ + ".upload_with_config")
    logger.debug("Executing with:")
    logger.debug("config keys: %s: ", config.keys())
    logger.debug("Settings for export_tables %s", sim2sumoconfig)
    logger.debug("Sumo env: %s: ", env)
    config_path = config["file_path"]
    upload_folder, suffixes = export_tables(sim2sumoconfig, config)
    upload(
        upload_folder,
        suffixes,
        "*",
        env,
        config_path=config_path,
    )
