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
    tidy,
    convert_to_arrow,
    vfp_to_arrow_dict,
)
from .common import (
    generate_meta,
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
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    byte_string = sink.getvalue().to_pybytes()
    return byte_string


def table_2_bytestring(table):
    """Convert pa.table to bytestring

    Args:
        table (pa.table): the table to convert

    Returns:
        bytest: the bytes string
    """
    bytestring = table_to_bytes(table)
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
    if "vfp" in tagname.lower():
        content = "lift_curves"
    else:
        content = SUBMOD_CONTENT.get(tagname, "property")

    metadata = generate_meta(config, datafile, tagname, obj, content)

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
    meta_args = (datafile, obj, tagname, config)

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
    else:
        try:
            output = extract_df(
                res2df.ResdataFiles(datafile_path),
                **kwargs,
            )
            if submod == "rft":
                output = tidy(output)
            if arrow:
                try:
                    convert_func = SUBMOD_DICT[submod]["arrow_convertor"]
                    output = convert_func(output)
                except pa.lib.ArrowInvalid:
                    logger.warning(
                        "Arrow invalid, cannot convert to arrow, keeping pandas format, (trace %s)",
                        sys.exc_info()[1],
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
    return output


def upload_tables(sim2sumoconfig, config, dispatcher):
    """Upload tables to sumo

    Args:
        sim2sumoconfig (dict): the sim2sumo configuration
        config (dict): the fmu config file with metadata
        env (str): what environment to upload to
    """
    for datafile_path, submod_and_options in sim2sumoconfig.items():
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
    keyword, tables = vfp_to_arrow_dict(datafile, options)
    for table in tables:
        table_number = str(
            table.schema.metadata[b"TABLE_NUMBER"].decode("utf-8")
        )
        tagname = f"{keyword}_{table_number}"
        sumo_file = convert_table_2_sumo_file(datafile, table, tagname, config)
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
    for submod, options in submod_and_options.items():
        if submod == "grid3d":
            continue

        if submod == "vfp":
            upload_vfp_tables_from_simulation_run(
                datafile, options, config, dispatcher
            )
        else:
            table = get_table(datafile, submod, options)
            sumo_file = convert_table_2_sumo_file(
                datafile, table, submod, config
            )
            if sumo_file is None:
                logger.warning(
                    f"Table with datatype {submod} extracted from {datafile} returned nothing"
                )
                continue
            dispatcher.add(sumo_file)
