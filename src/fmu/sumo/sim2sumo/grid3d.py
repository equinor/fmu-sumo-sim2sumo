#!/usr/bin/env python
"""Upload grid3d data from reservoir simulators to Sumo
Does three things:
1. Extracts data from simulator to roff files
2. Adds the required metadata while exporting to disc
3. Uploads to Sumo
"""

import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path

import numpy as np
from resdata.grid import Grid
from resdata.resfile import ResdataRestartFile
from xtgeo import GridProperty, grid_from_file
from xtgeo.grid3d import _gridprop_import_eclrun as eclrun
from xtgeo.io._file import FileWrapper

from fmu.dataio import ExportData
from fmu.sumo.uploader._fileonjob import FileOnJob

from .common import find_datefield, give_name


def xtgeo_2_bytestring(obj):
    """Convert xtgeo object to bytestring

    Args:
        obj (xtgeo object): the object to convert

    Returns:
        bytestring: bytes
    """
    if obj is None:
        return obj
    sink = BytesIO()
    obj.to_file(sink)
    sink.seek(0)
    bytestring = sink.getbuffer().tobytes()

    return bytestring


# Almost equal to tables.py::generate_table_meta, but note difference in name and tagname
def generate_grid3d_meta(datafile, obj, prefix, config):
    """Generate metadata for xtgeo object

    Args:
        datafile (str): path to datafile
        obj (xtgeo object): the object to generate metadata on
        prefix (str): prefix to include
        config (dict): the fmu config file

    Returns:
        dict: the metadata for obj
    """
    if isinstance(obj, Grid):
        content = "depth"
    else:
        content = {"property": {"is_discrete": False}}

    name = prefix if prefix == "grid" else f"{prefix}-{obj.name}"
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
    assert isinstance(
        metadata, dict
    ), f"meta should be dict, but is {type(metadata)}"

    return metadata


def convert_xtgeo_2_sumo_file(datafile, obj, prefix, config):
    """Convert xtgeo object to SumoFile ready for shipping to Sumo

    Args:
        datafile (str|PosixPath):
            path to datafile connected to extracted object
        obj (Xtgeo object): The object to prepare for upload
        prefix (str): prefix to distinguish between init and restart
        config (dict): dictionary with master metadata needed for Sumo

    Returns:
        SumoFile: Object containing xtgeo object as bytestring
                    and metadata as dictionary
    """
    if obj is None:
        return obj

    bytestring = xtgeo_2_bytestring(obj)
    metadata = generate_grid3d_meta(datafile, obj, prefix, config)

    sumo_file = FileOnJob(bytestring, metadata)
    sumo_file.path = metadata["file"]["relative_path"]
    sumo_file.metadata_path = ""
    sumo_file.size = len(sumo_file.byte_string)

    return sumo_file


def upload_init(init_path, xtgeoegrid, config, dispatcher):
    """Upload properties from init file

    Args:
        init_path (str): path to init file
        xtgeoegrid (xtgeo.Grid): The grid to upack the properties to

    Returns:
        int: number of objects to export
    """
    logger = logging.getLogger(__name__ + ".upload_init")
    unwanted = ["ENDNUM", "DX", "DY", "DZ", "TOPS"]
    init_props = list(
        eclrun.find_gridprop_from_init_file(init_path, "all", xtgeoegrid)
    )
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


def upload_restart(restart_path, xtgeoegrid, time_steps, config, dispatcher):
    """Export properties from restart file

    Args:
        restart_path (str): path to restart file
        xtgeoegrid (xtge.Grid): the grid to unpack the properties to
        time_steps (list): the timesteps to use

    Returns:
        int: number of objects to export
    """
    logger = logging.getLogger(__name__ + ".upload_restart")
    prop_names = ("SWAT", "SGAS", "SOIL", "PRESSURE", "SFIPOIL", "SFIPGAS")
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
                sumo_file = convert_xtgeo_2_sumo_file(
                    restart_path, xtgeo_prop, "UNRST", config
                )
                if sumo_file is None:
                    logger.warning(
                        "Property %s extracted from %s returned nothing",
                        prop_name,
                        restart_path,
                    )
                    continue
                dispatcher.add(sumo_file)


def upload_simulation_runs(datafiles, config, dispatcher):
    """Upload 3d grid and parameters for set of simulation runs

    Args:
        datafiles (list): the datafiles defining the runs
        config (dict): the fmu config file with metadata
        dispatcher (sim2sumo.common.Dispatcher)
    """
    for datafile in datafiles:
        if not datafiles[datafile]["grid3d"]:
            continue
        upload_simulation_run(datafile, config, dispatcher)


def upload_simulation_run(datafile, config, dispatcher):
    """Export 3d grid properties from simulation run

    Args:
        datafile (str): path to datafile
    """
    datafile_path = Path(datafile).resolve()
    init_path = str(datafile_path.with_suffix(".INIT"))
    restart_path = str(datafile_path.with_suffix(".UNRST"))
    grid_path = str(datafile_path.with_suffix(".EGRID"))
    egrid = Grid(grid_path)
    xtgeoegrid = grid_from_file(grid_path)
    sumo_file = convert_xtgeo_2_sumo_file(
        restart_path, xtgeoegrid, "grid", config
    )
    dispatcher.add(sumo_file)
    time_steps = get_timesteps(restart_path, egrid)

    upload_init(init_path, xtgeoegrid, config, dispatcher)
    upload_restart(restart_path, xtgeoegrid, time_steps, config, dispatcher)


def get_timesteps(restart_path, egrid):
    """Get all available timesteps in restart file

    Args:
        restart_path (str): path to restart file
        egrid (resdata.EGRID): grid connected to restart file

    Returns:
        list: list of dates
    """
    restart = ResdataRestartFile(egrid, restart_path)

    dates = []
    for date in restart.time_list():
        date_str = datetime.strftime(date[1], "%Y-%m-%d")
        dates.append(date_str)
    return dates


def make_xtgeo_prop(xtgeoegrid, prop_dict):
    """Build an xtgeo property from xtgeo record

    Args:
        xtgeoegrid (xtgeo.Grid): the grid to connect property to
        prop_dict (dict): xtgeo record

    Returns:
        xtgeo.GridProperty: the extracted results
    """
    prop_name = prop_dict["name"]
    values = prop_dict["values"]
    # TODO: Why do we skip single value properties?
    single_value = np.unique(values).size == 1
    if single_value:
        # prop_name has only one value. Will not return single value property."
        return None

    xtgeo_prop = GridProperty(xtgeoegrid, name=prop_name)
    xtgeo_prop.values = values
    return xtgeo_prop
