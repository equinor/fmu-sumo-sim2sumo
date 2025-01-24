"""Upload tabular data from reservoir simulators to sumo
Does three things:
1. Extracts data from simulator to arrow files
2. Adds the required metadata while exporting to disc
3. Uploads to Sumo
"""

import base64
import hashlib
import logging
import sys
from copy import deepcopy
from itertools import islice
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import res2df

from fmu.dataio import ExportData
from fmu.sumo.uploader._fileonjob import FileOnJob

from ._special_treatments import (
    SUBMOD_DICT,
    convert_to_arrow,
    tidy,
    vfp_to_arrow_dict,
)
from .common import (
    find_datefield,
    give_name,
)

SUBMOD_CONTENT = {
    "summary": "simulationtimeseries",
    "satfunc": "relperm",
    "vfp": "lift_curves",
    "rft": "rft",
    "pvt": "pvt",
    "transmissibilities": "transmissibilities",
}

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    try:
        from more_itertools import batched
    except ImportError:

        def batched(iterable, chunk_size):
            iterator = iter(iterable)
            while chunk := tuple(islice(iterator, chunk_size)):
                yield chunk


def table_2_bytestring(table):
    """Convert pa.table to bytestring

    Args:
        table (pa.table): the table to convert

    Returns:
        bytes: table as bytestring
    """
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    bytestring = sink.getvalue().to_pybytes()
    return bytestring


# Almost equal to grid3d.py::generate_grid3d_meta, but note difference in name and tagname
def generate_table_meta(datafile, obj, tagname, config):
    """Generate metadata for xtgeo object

    Args:
        datafile (str): path to datafile
        obj (xtgeo object): the object to generate metadata on
        tagname: tagname
        config (dict): the fmu config file

    Returns:
        dict: the metadata for obj
    """
    if "vfp" in tagname.lower():
        content = "lift_curves"
    else:
        content = SUBMOD_CONTENT.get(tagname, "property")

    name = give_name(datafile)

    exp_args = {
        "config": config,
        "name": name,
        "tagname": tagname,
        "content": content,
    }

    datefield = find_datefield(tagname)
    if datefield is not None:
        exp_args["timedata"] = [[datefield]]

    exd = ExportData(**exp_args)
    metadata = exd.generate_metadata(obj)

    return metadata


def convert_table_2_sumo_file(datafile, obj, tagname, config):
    """Convert table to Sumo File ready for shipping to sumo
    If the table is a summary table and has a defined table_index
        we also return the table in chunks of 500 columns with
        _sumo.hidden set to True

    Args:
      datafile (str|PosixPath): path to datafile connected to extracted object
      obj (pa.Table): The object to prepare for upload
      tagname (str): what submodule the table is extracted from
      config (dict): dictionary with master metadata needed for Sumo
    Returns:
      files (list): List of SumoFile objects with table object
        as bytestring and metadata as dictionary
    """
    if obj is None:
        return obj

    metadata = generate_table_meta(datafile, obj, tagname, config)

    files = []
    chunk_size = 500
    columns = metadata["data"]["spec"]["columns"]
    table_index = metadata["data"]["table_index"]
    tagname = metadata["data"]["tagname"]

    if table_index and tagname == "summary":
        cols = [c for c in columns if c not in table_index]
        chunks = batched(cols, chunk_size - len(table_index))
        for idx, chunk in enumerate(chunks):
            chunk_columns = table_index + list(chunk)
            table = obj.select(chunk_columns)
            chunk_meta = deepcopy(metadata)
            bytestring = table_2_bytestring(table)

            md5_hex = hashlib.md5(bytestring).hexdigest()
            md5_b64 = base64.b64encode(bytes.fromhex(md5_hex)).decode()
            chunk_meta["data"]["spec"]["columns"] = table.column_names
            chunk_meta["data"]["spec"]["num_columns"] = table.num_columns
            chunk_meta["data"]["spec"]["num_rows"] = table.num_rows
            chunk_meta["data"]["spec"]["size"] = (
                table.num_columns * table.num_rows
            )
            chunk_meta["file"]["size_bytes"] = len(bytestring)
            chunk_meta["file"]["checksum_md5"] = md5_hex
            relative_path = metadata["file"]["relative_path"]
            chunk_meta["file"]["relative_path"] = relative_path.replace(
                f"--{tagname}", f"--{tagname}:{idx:03d}"
            )

            sumo_file = FileOnJob(bytestring, chunk_meta)
            sumo_file.path = chunk_meta["file"]["relative_path"]
            sumo_file.metadata_path = ""
            sumo_file.size = len(sumo_file.byte_string)

            file_meta = sumo_file.metadata
            if "_sumo" not in file_meta:
                file_meta["_sumo"] = {}
            _sumo = file_meta["_sumo"]
            _sumo["blob_size"] = len(bytestring)
            _sumo["blob_md5"] = md5_b64
            _sumo["hidden"] = True
            _sumo["fragment"] = idx

            files.append(sumo_file)

    bytestring = table_2_bytestring(obj)
    sumo_file = FileOnJob(bytestring, metadata)
    sumo_file.path = metadata["file"]["relative_path"]
    sumo_file.metadata_path = ""
    sumo_file.size = len(sumo_file.byte_string)
    files.append(sumo_file)

    return files


def get_table(
    datafile_path: str, submod: str, **kwargs
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
    from contextlib import suppress

    with suppress(KeyError):
        del kwargs["arrow"]
    output = None
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
            output = tidy(output)
        if arrow:
            try:
                convert_func = SUBMOD_DICT[submod]["arrow_convertor"]
                output = convert_func(output)
            except pa.lib.ArrowInvalid:
                logger.warning(
                    "Arrow invalid, cannot convert to arrow, "
                    "keeping pandas format, "
                    "(trace %s). \nFalling back to converting with %s",
                    sys.exc_info()[1],
                    convert_to_arrow.__name__,
                )
                output = convert_to_arrow(output)
            except TypeError:
                logger.warning("Type error, cannot convert to arrow")

    except (TypeError, FileNotFoundError, ValueError):
        logger.warning(
            "Trace: %s, \nNo results produced ",
            sys.exc_info()[1],
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
        datafile_path = datafile_path.resolve()
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
    vfp_dict = vfp_to_arrow_dict(datafile, options)
    for keyword, tables in vfp_dict.items():
        for table in tables:
            table_number = str(
                table.schema.metadata[b"TABLE_NUMBER"].decode("utf-8")
            )
            tagname = f"{keyword}_{table_number}"
            sumo_files = convert_table_2_sumo_file(
                datafile, table, tagname.lower(), config
            )
            for file in sumo_files:
                dispatcher.add(file)


def upload_tables_from_simulation_run(
    datafile, submod_and_options, config, dispatcher
):
    """Upload tables from one simulator run to Sumo

    Args:
        datafile (str): the datafile defining the simulation run
        submod_and_options (dict): key=submodule, value=options for submodule
        config (dict): the fmu config with metadata
        dispatcher (sim2sumo.common.Dispatcher)
    """
    logger = logging.getLogger(__name__ + ".upload_tables_from_simulation_run")
    for submod, options in submod_and_options.items():
        if submod == "grid3d":
            # No tables for grid3d
            continue

        if submod == "vfp":
            upload_vfp_tables_from_simulation_run(
                datafile, options, config, dispatcher
            )
        else:
            table = get_table(datafile, submod, **options)
            if table is None:
                logger.warning(
                    "Table with datatype %s from %s returned nothing",
                    submod,
                    datafile,
                )
                continue
            sumo_files = convert_table_2_sumo_file(
                datafile, table, submod, config
            )
            for file in sumo_files:
                dispatcher.add(file)
