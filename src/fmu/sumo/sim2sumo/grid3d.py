#!/usr/bin/env python
"""Upload grid3d data from reservoir simulators to Sumo
   Does three things:
   1. Extracts data from simulator to roff files
   2. Adds the required metadata while exporting to disc
   3. Uploads to Sumo
"""
import logging
import re
from datetime import datetime
from pathlib import Path

from io import BytesIO
import numpy as np
from resdata.grid import Grid
from resdata.resfile import ResdataRestartFile
from xtgeo import GridProperty, grid_from_file, gridproperty_from_file
from xtgeo.grid3d import _gridprop_import_eclrun as eclrun
from xtgeo.io._file import FileWrapper

from .common import (
    export_object,
    generate_meta,
    get_case_uuid,
    convert_to_bytestring,
    convert_2_sumo_file,
    nodisk_upload,
    fix_suffix,
    upload,
)


def xtgeo_2_bytes(obj):
    """Convert xtgeo object to bytesring

    Args:
        obj (xtgeo.Obj): the object to convert

    Returns:
        bytes: bytestring
    """
    logger = logging.getLogger(__name__ + ".xtgeo_2_bytes")
    if obj is None:
        return obj
    logger.debug("Converting %s", obj.name)
    sink = BytesIO()
    obj.to_file(sink)
    sink.seek(0)
    bytestring = sink.getbuffer().tobytes()
    logger.debug("Returning bytestring with size %s", len(bytestring))
    return bytestring


def xtgeo_2_bytestring(obj):
    """Convert xtgeo object to bytestring

    Args:
        obj (xtgeo object): the object to convert

    Returns:
        bytestring: bytes
    """
    if obj is None:
        return obj
    bytestring = convert_to_bytestring(xtgeo_2_bytes, obj)

    return bytestring


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
    logger = logging.getLogger(__name__ + ".generate_grid3d_meta")
    if obj is None:
        return obj

    if prefix == "grid":
        tagname = prefix
    else:
        tagname = f"{prefix}-{obj.name}"
    metadata = generate_meta(config, datafile, tagname, obj, content)
    logger.debug("Generated meta are %s", metadata)

    return metadata


def convert_xtgeo_2_sumo_file(datafile, obj, prefix, config):
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


def export_grdecl_grid(grid_path, exporter):
    """Export the grdecl grid

    Args:
        grid_path (str): path to grid

    Returns:
        xtgeo.grid: grid read from file
    """
    logger = logging.getLogger(__name__ + ".export_grdecl_grid")
    grid = grid_from_file(grid_path)
    logger.debug(grid.name)
    # logger.info(
    #     "Exported to %s", exporter.export(grid, name=grid.name, tagname="grdecl_grid")
    # )
    return grid


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


def export_grdecl_props(include_path, grid, exporter):
    """Export grid properties

    Args:
        include_path (Pathlib.Path): path where all grdecls are stored
        grid (xtgeo.Grid): grid to connect to properties
    """
    logger = logging.getLogger(__name__ + ".export_grdecl_props")
    includes = include_path
    grdecls = list(includes.glob("**/*.grdecl"))
    for grdecl in grdecls:
        logger.debug(grdecl)
        name = readname(grdecl)
        if name == "":
            logger.warning("Found no name, file is probably empty")
            continue
        try:
            prop = gridproperty_from_file(grdecl, name=name, grid=grid)
            logger.info(
                "Exported to %s",
                exporter.export(
                    prop, name=name, tagname=grid.name + "_grdecl_grid"
                ),
            )
        except ValueError:
            logger.warning("Something wrong with reading of file")
    # logger.debug(grdecls)


def export_from_simulation_runs(datafiles, config, env="prod"):
    """Export 3d grid properties from simulation runs

    Args:
        datafiles (list): path to datafiles
        config (dict): config with metadata
        env (str): environment to upload to
    """
    logger = logging.getLogger(__name__ + ".export_from_simulation_runs")
    logger.debug("These datafiles are used to extract results %s", datafiles)
    for datafile in datafiles:
        export_from_simulation_run(datafile, config, env)


def export_from_simulation_run(datafile, config, env="prod"):
    """Export 3d grid properties from simulation run

    Args:
        datafile (str): path to datafile
    """
    logger = logging.getLogger(__name__ + ".export_from_simulation_run")
    init_path = fix_suffix(datafile, ".INIT")
    restart_path = fix_suffix(datafile, ".UNRST")
    grid_path = fix_suffix(datafile, ".EGRID")
    egrid = Grid(grid_path)
    xtgeoegrid = grid_from_file(grid_path)
    grid_exp_path = export_object(
        datafile, "grid", config, xtgeoegrid, "depth"
    )
    upload(Path(grid_exp_path).parent, [".roff"], "*grid", env)
    time_steps = get_timesteps(restart_path, egrid)

    count = export_init(init_path, xtgeoegrid, config, env)
    count += export_restart(
        restart_path, xtgeoegrid, time_steps, config, env=env
    )
    logger.info("Exported %s properties", count)


def export_restart(
    restart_path,
    xtgeoegrid,
    time_steps,
    config,
    prop_names=("SWAT", "SGAS", "SOIL", "PRESSURE"),
    env="prod",
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
    logger = logging.getLogger(__name__ + ".export_init")
    logger.debug("File to load init from %s", restart_path)
    count = 0
    for base_name in prop_names:
        for time_step in time_steps:
            restart_prop = eclrun.import_gridprop_from_restart(
                FileWrapper(restart_path), base_name, xtgeoegrid, time_step
            )
            xtgeo_prop = make_xtgeo_prop(xtgeoegrid, restart_prop)

            if xtgeo_prop is not None:
                # TODO: refactor this if statement together with identical
                # code in export_init
                # These are identical, and should be treated as such
                logger.debug("Exporting %s", xtgeo_prop.name)
                export_path = export_object(
                    restart_path,
                    "UNRST-" + xtgeo_prop.name,
                    config,
                    xtgeo_prop,
                    "property",
                )
                count += 1

    logger.info("%s properties", count)
    export_folder = Path(export_path).parent
    config_file = config["file_path"]
    upload(
        export_folder, [".roff"], "*unrst", env=env, config_path=config_file
    )
    return count


def export_init(init_path, xtgeoegrid, config, env="prod"):
    """Export properties from init file

    Args:
        init_path (str): path to init file
        xtgeoegrid (xtgeo.Grid): The grid to upack the properties to

    Returns:
        int: number of objects to export
    """
    logger = logging.getLogger(__name__ + ".export_init")
    logger.debug("File to load init from %s", init_path)
    unwanted = ["ENDNUM", "DX", "DY", "DZ", "TOPS"]
    init_props = list(
        eclrun.find_gridprop_from_init_file(init_path, "all", xtgeoegrid)
    )
    count = 0
    export_path = None
    logger.debug("%s properties found in init", len(init_props))
    for init_prop in init_props:
        if init_prop["name"] in unwanted:
            logger.warning("%s will not be exported", init_prop["name"])
            continue
        xtgeo_prop = make_xtgeo_prop(xtgeoegrid, init_prop)
        if xtgeo_prop is not None:
            logger.debug("Exporting %s", xtgeo_prop.name)
            export_path = export_object(
                init_path,
                "INIT-" + xtgeo_prop.name,
                config,
                xtgeo_prop,
                "property",
            )
            count += 1

    logger.info("%s properties", count)
    export_folder = Path(export_path).parent
    config_file = config["file_path"]
    upload(export_folder, [".roff"], "*init", env=env, config_path=config_file)
    return count


def upload_init(init_path, xtgeoegrid, config, parentid, env="prod"):
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
    tosumo = []
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
        if sumo_file is not None:
            tosumo.append(sumo_file)
            count += 1
        if len(tosumo) > 50:
            nodisk_upload(tosumo, parentid, env)
            tosumo = []

    if len(tosumo) > 0:
        nodisk_upload(tosumo, parentid, env)
    return count


def upload_restart(
    restart_path,
    xtgeoegrid,
    time_steps,
    config,
    parentid,
    prop_names=("SWAT", "SGAS", "SOIL", "PRESSURE", "SFIPOIL", "SFIPGAS"),
    env="prod",
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
    tosumo = []
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
                tosumo.append(sumo_file)
                count += 1
            if len(tosumo) > 50:
                nodisk_upload(tosumo, parentid, env)
                tosumo = []
        if len(tosumo) > 0:
            nodisk_upload(tosumo, parentid, env)
    logger.info("%s properties", count)

    return count


def upload_simulation_runs(datafiles, config, env="prod"):
    """Upload 3d grid and parameters for set of simulation runs

    Args:
        datafiles (list): the datafiles defining the rums
        config (dict): the fmu config file with metadata
        env (str, optional): which Sumo environment that contains the case. Defaults to "prod".
    """
    for datafile in datafiles:
        upload_simulation_run(datafile, config, env)


def upload_simulation_run(datafile, config, env="prod"):
    """Export 3d grid properties from simulation run

    Args:
        datafile (str): path to datafile
    """
    logger = logging.getLogger(__name__ + ".upload_simulation_run")
    init_path = fix_suffix(datafile, ".INIT")
    restart_path = fix_suffix(datafile, ".UNRST")
    grid_path = fix_suffix(datafile, ".EGRID")
    egrid = Grid(grid_path)
    xtgeoegrid = grid_from_file(grid_path)
    # grid_exp_path = export_object(
    #     datafile, "grid", config, xtgeoegrid, "depth"
    # )
    parentid = get_case_uuid(datafile)
    sumo_file = convert_xtgeo_2_sumo_file(
        restart_path, xtgeoegrid, "grid", config
    )
    nodisk_upload([sumo_file], parentid, env)
    time_steps = get_timesteps(restart_path, egrid)

    count = upload_init(init_path, xtgeoegrid, config, parentid, env)
    count += upload_restart(
        restart_path, xtgeoegrid, time_steps, config, parentid, env=env
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
        logging.debug("Will not return single value property")
    else:
        xtgeo_prop = GridProperty(xtgeoegrid, name=prop_name)
        xtgeo_prop.values = values
        if describe:
            xtgeo_prop.describe()

    return xtgeo_prop
