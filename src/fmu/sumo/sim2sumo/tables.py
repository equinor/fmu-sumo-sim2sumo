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
    complete_rft,
    convert_to_arrow,
    vfp_to_arrow_dict,
    find_md_log,
)
from .common import (
    generate_meta,
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

    if "vfp" in tagname.lower():
        content = "lift_curves"
    else:
        content = SUBMOD_CONTENT.get(tagname, "property")

    metadata = generate_meta(config, datafile, tagname, obj, content)
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
    logger.debug(
        "Input arguments %s",
    )
    extract_df = SUBMOD_DICT[submod]["extract"]
    arrow = kwargs.get("arrow", True)
    try:
        del kwargs[
            "arrow"
        ]  # This argument should not be passed to extract function
    except KeyError:
        logger.debug("No arrow key to delete")
    output = None
    trace = None
    print_help = False
    if print_help:
        print("------------------")
        print(SUBMOD_DICT[submod]["doc"])
        print("------------------")
        # TODO: see if there is a cleaner way with rft, see functions
        # find_md_log, and complete_rft, but needs really to be fixed in res2df
    else:
        md_log_file = find_md_log(submod, kwargs)
        logger.debug("Checking these passed options %s", kwargs)
        try:
            logger.info(
                "Extracting data from %s with func %s for %s",
                datafile_path,
                extract_df.__name__,
                submod,
            )
            output = extract_df(
                res2df.ResdataFiles(datafile_path),
                **kwargs,
            )
            if submod == "rft":

                output = complete_rft(output, md_log_file)
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
    logger.debug("Will upload with settings %s", sim2sumoconfig)
    for datafile_path, submod_and_options in sim2sumoconfig.items():
        logger.debug("datafile: %s", datafile_path)
        upload_tables_from_simulation_run(
            datafile_path,
            submod_and_options,
            config,
            dispatcher,
        )


def upload_vfp_tables_from_simulation_run(
    datafile, options, config, dispatcher
):
    """Upload vfp tables from one simulator run to Sumo

    Args:
        datafile (str): the datafile defining the simulation run
        options (dict): the options for vfp
        config (dict): the fmu config with metadata
        dispatcher (sim2sumo.common.Dispatcher): job dispatcher
    """
    logger = logging.getLogger(
        __name__ + ".upload_vfp_tables_from_simulation_run"
    )
    vfp_dict = vfp_to_arrow_dict(datafile, options)
    for keyword, tables in vfp_dict.items():
        for table in tables:
            table_number = str(
                table.schema.metadata[b"TABLE_NUMBER"].decode("utf-8")
            )
            logger.debug(table)
            tagname = f"{keyword}_{table_number}"
            logger.debug("Generated tagname: %s", tagname)
            sumo_file = convert_table_2_sumo_file(
                datafile, table, tagname.lower(), config
            )
            dispatcher.add(sumo_file)


def upload_tables_from_simulation_run(
    datafile, submod_and_options, config, dispatcher
):
    """Upload tables from one simulator run to Sumo

    Args:
        datafile (str): the datafile defining the simulation run
        config (dict): the fmu config with metadata
        dispatcher (sim2sumo.common.Dispatcher)
    """
    logger = logging.getLogger(__name__ + ".upload_tables_from_simulation_run")
    logger.info("Extracting tables from %s", datafile)
    count = 0
    for submod, options in submod_and_options.items():
        if submod == "grid3d":
            logger.debug("No tables for grid3d, skipping")
            continue

        if submod == "vfp":
            upload_vfp_tables_from_simulation_run(
                datafile, options, config, dispatcher
            )
        else:
            table = get_table(datafile, submod, options)
            logger.debug("Sending %s onto file creation", table)
            sumo_file = convert_table_2_sumo_file(
                datafile, table, submod, config
            )
            if sumo_file is None:
                logger.warning(
                    "Table with datatype %s extracted from %s returned nothing",
                    submod,
                    datafile,
                )
                continue
            dispatcher.add(sumo_file)

    logger.info("%s properties", count)
