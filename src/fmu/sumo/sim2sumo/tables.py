"""Upload tabular data from reservoir simulators to sumo
   Does three things:
   1. Extracts data from simulator to arrow files
   2. Adds the required metadata while exporting to disc
   3. Uploads to Sumo
"""

import logging
import sys
from typing import Union

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import res2df

from ._special_treatments import (
    SUBMOD_DICT,
    convert_options,
    tidy,
    convert_to_arrow,
)
from .common import (
    fix_suffix,
    generate_meta,
    get_case_uuid,
    convert_to_bytestring,
    convert_2_sumo_file,
)


SUBMOD_CONTENT = {
    "summary": "timeseries",
    "satfunc": "relperm",
    "vfp": "lift_curves",
}
SUBMOD_CONTENT.update(
    {name: name for name in ["rft", "pvt", "transmissibilities"]}
)


def table_to_bytes(table: pa.Table):
    """Return table as bytestring

    Args:
        table (pa.Table): the table to be converted

    Returns:
        bytes: table as bytestring
    """
    logger = logging.getLogger(__name__ + ".table_to_bytes")
    sink = pa.BufferOutputStream()
    logger.debug("Writing %s to sink", table)
    pq.write_table(table, sink)
    byte_string = sink.getvalue().to_pybytes()
    logger.debug("Returning bytestring with size %s", len(byte_string))
    return byte_string


def table_2_bytestring(table):
    """Convert pa.table to bytestring

    Args:
        table (pa.table): the table to convert

    Returns:
        bytest: the bytes string
    """
    bytestring = convert_to_bytestring(table_to_bytes, table)
    return bytestring


def generate_table_meta(datafile, obj, tagname, config):
    """Generate metadata for xtgeo object

    Args:
        datafile (str): path to datafile
        obj (xtgeo object): the object to generate metadata on
        prefix (str): prefix to include
        config (dict): the fmu config file
        content (str): content for data

    Returns:
        dict: the metadata for obj
    """
    logger = logging.getLogger(__name__ + ".generate_table_meta")

    metadata = generate_meta(
        config, datafile, tagname, obj, SUBMOD_CONTENT.get(tagname, "property")
    )
    logger.debug("Generated meta are %s", metadata)

    return metadata


def convert_table_2_sumo_file(datafile, obj, tagname, config):
    """Convert table to Sumo File ready for shipping to sumo

    Args:
      datafile (str|PosixPath): path to datafile connected to extracted object
        obj (pa.Table): The object to prepare for upload
        tagname (str): what submodule the table is extracted from
        config (dict): dictionary with master metadata needed for Sumo
    Returns:
         SumoFile: Object containing table object as bytestring + metadata as dictionary
    """
    logger = logging.getLogger(__name__ + ".convert_table_2_sumo_file")
    logger.debug("Datafile %s", datafile)
    logger.debug("Obj of type: %s", type(obj))
    logger.debug("tagname: %s", tagname)
    logger.debug("Config: %s", config)

    meta_args = (datafile, obj, tagname, config)
    logger.debug(
        "sending in %s",
        dict(
            zip(("datafile", "obj", "tagname", "config", "content"), meta_args)
        ),
    )
    sumo_file = convert_2_sumo_file(
        obj, table_2_bytestring, generate_table_meta, meta_args
    )
    return sumo_file


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
    print_help = False
    if print_help:
        print("------------------")
        print(SUBMOD_DICT[submod]["doc"])
        print("------------------")
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
                "Extracting data from %s with func %s",
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
                    convert_func = SUBMOD_DICT[submod]["arrow_convertor"]
                    logger.debug(
                        "Using function %s to convert to arrow",
                        convert_func.__name__,
                    )
                    output = convert_func(output)
                except pa.lib.ArrowInvalid:
                    logger.warning(
                        "Arrow invalid, cannot convert to arrow, keeping pandas format, (trace %s)",
                        sys.exc_info()[1],
                    )
                    logger.debug(
                        "Falling back to converting with %s",
                        convert_to_arrow.__name__,
                    )
                    output = convert_to_arrow(output)

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
    logger.debug("Returning %s", output)
    return output


def upload_tables(sim2sumoconfig, config, dispatcher):
    """Upload tables to sumo

    Args:
        sim2sumoconfig (dict): the sim2sumo configuration
        config (dict): the fmu config file with metadata
        env (str): what environment to upload to
    """
    logger = logging.getLogger(__file__ + ".upload_tables")
    parentid = get_case_uuid(sim2sumoconfig["datafiles"][0])
    logger.info("Sumo case uuid: %s", parentid)
    for datafile in sim2sumoconfig["datafiles"]:

        upload_tables_from_simulation_run(
            datafile,
            sim2sumoconfig["submods"],
            sim2sumoconfig["options"],
            config,
            dispatcher,
        )


def upload_tables_from_simulation_run(
    datafile, submods, options, config, dispatcher
):
    """Upload tables from one simulator run to Sumo

    Args:
        datafile (str): the datafile defining the simulation run
        submods (list): the datatypes to extract
        options (dict): the options to pass inn
        config (dict): the fmu config with metadata
        dispatcher (sim2sumo.common.Dispatcher)
    """
    logger = logging.getLogger(__name__ + ".upload_tables_from_simulation_run")
    logger.info("Extracting tables from %s", datafile)
    count = 0
    for submod in submods:
        table = get_table(datafile, submod, options)
        logger.debug("Sending %s onto file creation", table)
        sumo_file = convert_table_2_sumo_file(datafile, table, submod, config)
        dispatcher.add(sumo_file)

    logger.info("%s properties", count)
