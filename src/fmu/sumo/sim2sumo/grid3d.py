#!/usr/bin/env python
"""Upload grid3d data from reservoir simulators to Sumo
Does three things:
1. Extracts data from simulator to roff files
2. Adds the required metadata while exporting to disc
3. Uploads to Sumo
"""

import logging  # noqa: I001
import os
import re
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

from .common import find_datefield, give_name, yaml_load
from ._units import get_all_properties_units, get_datafile_unit_system


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


# Almost equal to tables.py::generate_table_meta,
# difference in name and tagname
def generate_grid3d_meta(datafile, obj, config):
    """Generate metadata for xtgeo object

    Args:
        datafile (str): path to datafile
        obj (xtgeo object): the object to generate metadata on
        config (dict): the fmu config file

    Returns:
        dict: the metadata for obj
    """

    tagname = give_name(datafile)
    exp_args = {
        "config": config,
        "name": give_name(datafile),
        "tagname": tagname,
        "content": "depth",
    }

    # Future: refactor to be "diskless"
    #   i.e. use exd.generate_metadata() instead of exd.export()
    #       metadata = exd.generate_metadata(obj)
    #   Currently have to write to disk because GridProperty expects
    #   the grid to exist on disk when linking geometry
    exd = ExportData(**exp_args)
    outfile = exd.export(obj)

    outfile_path = Path(outfile)
    metadata_path = outfile_path.parent / f".{outfile_path.name}.yml"

    metadata = yaml_load(metadata_path)

    assert isinstance(metadata, dict), (
        f"meta should be dict, but is {type(metadata)}"
    )

    return metadata


def generate_gridproperty_meta(datafile, obj, property_units, config, geogrid):
    """Generate metadata for xtgeo object

    Args:
        datafile (str): path to datafile
        obj (xtgeo object): the object to generate metadata on
        property_units (dict): property - unit map
        config (dict): the fmu config file
        geogrid (str): path to the grid to link as geometry

    Returns:
        dict: the metadata for obj
    """

    tagname = give_name(datafile)
    exp_args = {
        "config": config,
        "tagname": tagname,
        "content": {"property": {"is_discrete": False}},
        "geometry": geogrid,
    }

    datefield = find_datefield(obj.name)
    if datefield is not None:
        exp_args["timedata"] = [[datefield]]

    # Time metadata is extracted from the original object name,
    # which has the format "PROPERTY-DATE".
    # The name has to be santised after find_datafield().
    exp_args["name"] = sanitise_gridprop_name(obj.name)
    exp_args["unit"] = property_units.get(exp_args["name"], None)

    exd = ExportData(**exp_args)

    metadata = exd.generate_metadata(obj)
    assert isinstance(metadata, dict), (
        f"meta should be dict, but is {type(metadata)}"
    )

    return metadata


def sanitise_gridprop_name(name: str) -> str:
    """
    Removes date suffix "_YYYYMMDD" from the names of dynamic ("restart") grid
    properties. If the name of a static ("init") property is passed, the name
    is returned unchanged.

    >>> sanitise_gridprop_name("PERMX")
    "PERMX"

    >>> sanitise_gridprop_name("SWAT_20191001")
    "SWAT"

    Args:
        name (str): name of an xtgeo grid property object

    Returns:
        str: name minus the date (if there was one)
    """

    clean_name = re.sub(r"_\d{8}", "", name)

    return clean_name


def convert_xtgeo_to_sumo_file(obj, metadata):
    """Convert xtgeo object to SumoFile

    Args:
        obj (Xtgeo object): The object to prepare for upload
        metadata (dict): dictionary with metadata

    Returns:
        SumoFile: Object containing xtgeo object as bytestring
                    and metadata as dictionary
    """
    if obj is None:
        return obj

    bytestring = xtgeo_2_bytestring(obj)

    sumo_file = FileOnJob(bytestring, metadata)
    sumo_file.path = metadata["file"]["relative_path"]
    sumo_file.metadata_path = ""
    sumo_file.size = len(sumo_file.byte_string)

    return sumo_file


def upload_init(
    init_path, xtgeoegrid, property_units, config, dispatcher, geometry_path
):
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
        prop_metadata = generate_gridproperty_meta(
            init_path, xtgeo_prop, property_units, config, geometry_path
        )
        sumo_file = convert_xtgeo_to_sumo_file(xtgeo_prop, prop_metadata)
        if sumo_file is None:
            logger.warning(
                "Property with name %s extracted from %s returned nothing",
                init_prop["name"],
                init_path,
            )
            continue
        dispatcher.add(sumo_file)


def upload_restart(
    restart_path,
    xtgeoegrid,
    property_units,
    time_steps,
    config,
    s2s_config,
    dispatcher,
    geometry_path,
):
    """Export properties from restart file

    Args:
        restart_path (str): path to restart file
        xtgeoegrid (xtge.Grid): the grid to unpack the properties to
        time_steps (list): the timesteps to use

    Returns:
        int: number of objects to export
    """
    logger = logging.getLogger(__name__ + ".upload_restart")

    # Get all restart properties names from the restart file. Have to load one timestep
    # first to get a list of properties. Getting properties from last timestep
    # as some restart properties are missing when taking the first timestep.
    props_all = list(
    eclrun.find_gridprops_from_restart_file(restart_path, "all", "last",  xtgeoegrid)
    )
    prop_names_all = [prop["name"] for prop in props_all]
    # SOIL property is not a "real" property. It is calculated from 1 - SWAT - SGAS.
    # xtgeo will calculate SOIL if it is requested.
    prop_names_all.append("SOIL")

    if "ALL" in s2s_config["grid"]["rstprops"]:
        prop_names = prop_names_all
    else:
        prop_names = s2s_config["grid"]["rstprops"]

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
                prop_metadata = generate_gridproperty_meta(
                    restart_path,
                    xtgeo_prop,
                    property_units,
                    config,
                    geometry_path,
                )

                sumo_file = convert_xtgeo_to_sumo_file(
                    xtgeo_prop, prop_metadata
                )
                if sumo_file is None:
                    logger.warning(
                        "Property %s extracted from %s returned nothing",
                        prop_name,
                        restart_path,
                    )
                    continue
                dispatcher.add(sumo_file)



def upload_simulation_runs(s2s_config, config, dispatcher):
    """Upload 3d grid and parameters for set of simulation runs

    Args:
        s2s_config (list[dict]): the datafiles defining the runs
        config (dict): the fmu config file with metadata
        dispatcher (sim2sumo.common.Dispatcher)
    """
    for datafile in s2s_config:
        if "grid" not in s2s_config[datafile]:
            continue
        upload_simulation_run(datafile, s2s_config[datafile], config, dispatcher)


def upload_simulation_run(datafile, s2s_config, config, dispatcher):
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
    grid_metadata = generate_grid3d_meta(restart_path, xtgeoegrid, config)

    exported_grid_path = Path(grid_metadata["file"]["absolute_path"])

    sumo_file = convert_xtgeo_to_sumo_file(xtgeoegrid, grid_metadata)
    dispatcher.add(sumo_file)
    time_steps = get_timesteps(restart_path, egrid)

    unit_system = get_datafile_unit_system(datafile)
    property_units = get_all_properties_units(unit_system)

    upload_init(
        init_path,
        xtgeoegrid,
        property_units,
        config,
        dispatcher,
        exported_grid_path,
    )
    upload_restart(
        restart_path,
        xtgeoegrid,
        property_units,
        time_steps,
        config,
        s2s_config,
        dispatcher,
        exported_grid_path,
    )

    if os.path.exists(exported_grid_path):
        os.remove(exported_grid_path)
    metadata_path = (
        exported_grid_path.parent / f".{exported_grid_path.name}.yml"
    )
    if os.path.exists(metadata_path):
        os.remove(metadata_path)


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
