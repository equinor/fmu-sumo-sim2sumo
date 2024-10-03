#!/usr/bin/env python
"""Upload grid3d data from reservoir simulators to Sumo
   Does three things:
   1. Extracts data from simulator to roff files
   2. Adds the required metadata while exporting to disc
   3. Uploads to Sumo
"""
import logging
from pathlib import Path
import re
from datetime import datetime

from io import BytesIO
import numpy as np
from resdata.grid import Grid
from resdata.resfile import ResdataRestartFile
from xtgeo import GridProperty, grid_from_file
from xtgeo.grid3d import _gridprop_import_eclrun as eclrun
from xtgeo.io._file import FileWrapper

from fmu.dataio import ExportData

from .common import (
    find_datefield,
    convert_2_sumo_file,
    give_name,
)


def xtgeo_2_bytestring(obj):
    """Convert xtgeo object to bytestring

    Args:
        obj (xtgeo object): the object to convert

    Returns:
        bytestring: bytes
    """
    logger = logging.getLogger(__name__ + ".xtgeo_2_bytestring")
    if obj is None:
        return obj
    logger.debug("Converting %s", obj.name)
    sink = BytesIO()
    obj.to_file(sink)
    sink.seek(0)
    bytestring = sink.getbuffer().tobytes()
    logger.debug("Returning bytestring with size %s", len(bytestring))

    return bytestring


# Almost equal to tables.py::generate_table_meta, but note difference in name and tagname
def generate_grid3d_meta(datafile, obj, prefix, config, content):
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
    if obj is None:
        return obj

    if prefix == "grid":
        name = prefix
    else:
        name = f"{prefix}-{obj.name}"
    tagname = give_name(datafile)

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
    relative_parent = str(Path(datafile).parents[2]).replace(
        str(Path(datafile).parents[4]), ""
    )
    metadata["file"] = {
        "relative_path": f"{relative_parent}/{tagname}--{name}".lower()
    }

    return metadata


def convert_xtgeo_2_sumo_file(datafile, obj, prefix, config):
    """Convert xtgeo object to SumoFile ready for shipping to Sumo

    Args:
        datafile (str|PosixPath): path to datafile connected to extracted object
        obj (Xtgeo object): The object to prepare for upload
        prefix (str): prefix to distinguish between init and restart
        config (dict): dictionary with master metadata needed for Sumo

    Returns:
        SumoFile: Object containing xtgeo object as bytestring + metadata as dictionary
    """
    logger = logging.getLogger(__name__ + ".convert_xtgeo_2_sumo_file")
    logger.debug("Datafile %s", datafile)
    logger.debug("Obj of type: %s", type(obj))
    logger.debug("prefix: %s", prefix)
    logger.debug("Config: %s", config)
    if obj is None:
        return obj
    if isinstance(obj, Grid):
        content = "depth"
    else:
        content = "property"

    meta_args = (datafile, obj, prefix, config, content)
    logger.debug(
        "sending in %s",
        dict(
            zip(("datafile", "obj", "prefix", "config", "content"), meta_args)
        ),
    )
    sumo_file = convert_2_sumo_file(
        obj, xtgeo_2_bytestring, generate_grid3d_meta, meta_args
    )
    return sumo_file


def get_xtgeo_egrid(datafile):
    """Export egrid file to sumo

    Args:
        datafile (str): path to datafile
    """
    logger = logging.getLogger(__name__ + ".get_xtgeo_egrid")
    logger.debug("Fetching %s", datafile)
    egrid_path = str(datafile).replace(".DATA", ".EGRID")
    egrid = grid_from_file(egrid_path)

    logger.info("Fetched %s", egrid.name)
    return egrid


def readname(filename):
    """Read keyword from grdecl file

    Args:
        filename (str): name of file to read

    Returns:
        str: keyword name
    """
    logger = logging.getLogger(__name__ + ".readname")
    name = ""
    linenr = 0
    with open(filename, "r", encoding="utf-8") as file_handle:
        for line in file_handle:
            linenr += 1
            logger.debug("%s %s", linenr, line)
            if "ECHO" in line:
                continue
            match = re.match(r"^([a-zA-Z].*)", line)
            # match = re.match(r"$([a-zA-Z][0-9A-Za-z]+)\s+", line)
            if match:
                name = match.group(0)
                break
            if linenr > 20:
                break
    logger.debug("Property %s", name)

    return name


def make_dates_from_timelist(time_list):
    """Convert time list format from resdata.RestartFile to strings

    Args:
        time_list (ResDataRestartFile.timelist): the input list of dates

    Returns:
        list: dates in string format
    """
    dates = []
    for date in time_list:
        date_str = datetime.strftime(date[1], "%Y-%m-%d")
        dates.append(date_str)
    return dates


def upload_init(init_path, xtgeoegrid, config, dispatcher):
    """Upload properties from init file

    Args:
        init_path (str): path to init file
        xtgeoegrid (xtgeo.Grid): The grid to upack the properties to

    Returns:
        int: number of objects to export
    """
    logger = logging.getLogger(__name__ + ".upload_init")
    logger.debug("File to load init from %s", init_path)
    unwanted = ["ENDNUM", "DX", "DY", "DZ", "TOPS"]
    init_props = list(
        eclrun.find_gridprop_from_init_file(init_path, "all", xtgeoegrid)
    )
    count = 0
    logger.debug("%s properties found in init", len(init_props))
    for init_prop in init_props:
        if init_prop["name"] in unwanted:
            logger.warning("%s will not be exported", init_prop["name"])
            continue
        xtgeo_prop = make_xtgeo_prop(xtgeoegrid, init_prop)
        if xtgeo_prop is None:
            logger.warning("%s will not be uploaded", init_prop["name"])
            continue
        sumo_file = convert_xtgeo_2_sumo_file(
            init_path, xtgeo_prop, "INIT", config
        )
        if sumo_file is None:
            logger.warning(
                "Property with name %s extracted from %s returned nothing",
                init_prop["name"],
                init_path,
            )
            continue
        dispatcher.add(sumo_file)
        count += 1
    logger.info("%s properties sendt on", count)
    return count


def upload_restart(
    restart_path,
    xtgeoegrid,
    time_steps,
    config,
    dispatcher,
    prop_names=("SWAT", "SGAS", "SOIL", "PRESSURE", "SFIPOIL", "SFIPGAS"),
):
    """Export properties from restart file

    Args:
        restart_path (str): path to restart file
        xtgeoegrid (xtge.Grid): the grid to unpack the properties to
        time_steps (list): the timesteps to use
        prop_names (iterable, optional): the properties to export. Defaults to ("SWAT", "SGAS", "SOIL", "PRESSURE").

    Returns:
        int: number of objects to export
    """
    logger = logging.getLogger(__name__ + ".upload_restart")
    logger.debug("File to load restart from %s", restart_path)
    count = 0
    for prop_name in prop_names:
        for time_step in time_steps:

            try:
                restart_prop = eclrun.import_gridprop_from_restart(
                    FileWrapper(restart_path), prop_name, xtgeoegrid, time_step
                )
            except ValueError:
                logger.warning("Cannot find %s", prop_name)
                continue

            xtgeo_prop = make_xtgeo_prop(xtgeoegrid, restart_prop)
            if xtgeo_prop is not None:
                # TODO: refactor this if statement together with identical
                # code in export_init
                # These are identical, and should be treated as such
                logger.debug("Exporting %s", xtgeo_prop.name)
                sumo_file = convert_xtgeo_2_sumo_file(
                    restart_path, xtgeo_prop, "UNRST", config
                )
                if sumo_file is None:
                    logger.warning(
                        "Property with name %s extracted from %s returned nothing",
                        prop_name,
                        restart_path,
                    )
                    continue
                dispatcher.add(sumo_file)
                count += 1
    logger.info("%s properties sendt on", count)

    return count


def upload_simulation_runs(datafiles, config, dispatcher):
    """Upload 3d grid and parameters for set of simulation runs

    Args:
        datafiles (list): the datafiles defining the rums
        config (dict): the fmu config file with metadata
        dispatcher (sim2sumo.common.Dispatcher)
    """
    logger = logging.getLogger(__name__ + ".upload_simulation_runs")
    for datafile in datafiles:
        if not datafiles[datafile]["grid3d"]:
            logger.info("Export of grid3d deactivated for %s", datafile)
            continue
        upload_simulation_run(datafile, config, dispatcher)


def upload_simulation_run(datafile, config, dispatcher):
    """Export 3d grid properties from simulation run

    Args:
        datafile (str): path to datafile
    """
    logger = logging.getLogger(__name__ + ".upload_simulation_run")
    datafile_path = Path(datafile)
    init_path = str(datafile_path.with_suffix(".INIT"))
    restart_path = str(datafile_path.with_suffix(".UNRST"))
    grid_path = str(datafile_path.with_suffix(".EGRID"))
    egrid = Grid(grid_path)
    xtgeoegrid = grid_from_file(grid_path)
    # grid_exp_path = export_object(
    #     datafile, "grid", config, xtgeoegrid, "depth"
    # )
    sumo_file = convert_xtgeo_2_sumo_file(
        restart_path, xtgeoegrid, "grid", config
    )
    dispatcher.add(sumo_file)
    time_steps = get_timesteps(restart_path, egrid)

    count = upload_init(init_path, xtgeoegrid, config, dispatcher)
    count += upload_restart(
        restart_path, xtgeoegrid, time_steps, config, dispatcher
    )
    logger.info("Exported %s properties", count)


def get_timesteps(restart_path, egrid):
    """Get all available timesteps in restart file

    Args:
        restart_path (str): path to restart file
        egrid (resdata.EGRID): grid connected to restart file

    Returns:
        list: list of dates
    """
    restart = ResdataRestartFile(egrid, restart_path)
    time_steps = make_dates_from_timelist(restart.time_list())
    return time_steps


def make_xtgeo_prop(
    xtgeoegrid, prop_dict, describe=False, return_single=False
):
    """Build an xtgeo property from xtgeo record

    Args:
        xtgeoegrid (xtgeo.Grid): the grid to connect property to
        prop_dict (dict): xtgeo record
        describe (bool, optional): Print some statistics for property. Defaults to False.

    Returns:
        xtgeo.GridProperty: the extracted results
    """
    logger = logging.getLogger(__name__ + ".make_xtgeo_prop")
    prop_name = prop_dict["name"]
    values = prop_dict["values"]
    single_value = np.unique(values).size == 1
    if single_value:
        logger.info("%s has only one value", prop_name)
    if single_value and not return_single:
        xtgeo_prop = None
        logger.debug("Will not return single value property")
    else:
        xtgeo_prop = GridProperty(xtgeoegrid, name=prop_name)
        xtgeo_prop.values = values
        if describe:
            xtgeo_prop.describe()

    return xtgeo_prop
