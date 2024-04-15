#!/usr/bin/env python
"""Export grid data from eclipse with metadata"""
import logging
import re
from pathlib import Path
import argparse
from datetime import datetime
from fmu.dataio import ExportData
from fmu.config.utilities import yaml_load
from resdata.resfile import ResdataRestartFile
from resdata.grid import Grid
from xtgeo import grid_from_file, gridproperty_from_file
from xtgeo.grid3d import _gridprop_import_eclrun as eclrun
from xtgeo.io._file import FileWrapper
from xtgeo import GridProperty
from .common import export_object, upload


def parse_args():
    """Parse arguments for script

    Returns:
        argparse.NameSpace: The arguments parsed
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=("Export grid data from "),
    )
    parser.add_argument("datafile", help="Path to eclipse datafile", type=str)
    parser.add_argument(
        "config_path", help="Path to fmu config path", type=str
    )
    parser.add_argument("grdecl_grid", help="path to grdecl grid", type=str)
    args = parser.parse_args()
    return args


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
    name = ""
    linenr = 0
    with open(filename, "r", encoding="utf-8") as file_handle:
        for line in file_handle:
            linenr += 1
            logger.debug(f"{linenr}: {line}")
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


def export_from_simulation_run(datafile, config_file):
    """Export 3d grid properties from simulation run

    Args:
        datafile (str): path to datafile
    """
    logger = logging.getLogger(__name__ + ".export_from_simulation_run")
    init_path = datafile.replace(".DATA", ".INIT")
    restart_path = datafile.replace(".DATA", ".UNRST")
    grid_path = datafile.replace(".DATA", ".EGRID")
    egrid = Grid(grid_path)
    xtgeoegrid = grid_from_file(grid_path)

    time_steps = get_timesteps(restart_path, egrid)

    count = export_init(init_path, xtgeoegrid, config_file)
    count += export_restart(restart_path, xtgeoegrid, time_steps, config_file)
    logger.info("Exported %s properties", count)


def export_restart(
    restart_path,
    xtgeoegrid,
    time_steps,
    config_file,
    prop_names=("SWAT", "SGAS", "SOIL", "PRESSURE"),
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
            logger.debug("Exporting %s", xtgeo_prop.name)
            export_path = export_object(
                restart_path,
                "UNRST-" + xtgeo_prop.name,
                config_file,
                xtgeo_prop,
                "property",
            )
            count += 1
    logger.info("%s properties", count)
    export_folder = Path(export_path).parent
    upload(export_folder, [".roff"], env="dev", config_path=config_file)
    return count


def export_init(init_path, xtgeoegrid, config_file):
    """Export properties from init file

    Args:
        init_path (str): path to init file
        xtgeoegrid (xtgeo.Grid): The grid to upack the properties to

    Returns:
        int: number of objects to export
    """
    logger = logging.getLogger(__name__ + ".export_init")
    logger.debug("File to load init from %s", init_path)
    logger.debug("Config file to marry with data %s", config_file)
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
        logger.debug("Exporting %s", xtgeo_prop.name)
        export_path = export_object(
            init_path,
            "INIT-" + xtgeo_prop.name,
            config_file,
            xtgeo_prop,
            "property",
        )
        count += 1
    logger.info("%s properties", count)
    export_folder = Path(export_path).parent
    upload(export_folder, [".roff"], env="dev", config_path=config_file)
    return count


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


def make_xtgeo_prop(xtgeoegrid, prop_dict, describe=True):
    """Build an xtgeo property from xtgeo record

    Args:
        xtgeoegrid (xtgeo.Grid): the grid to connect property to
        prop_dict (dict): xtgeo record
        describe (bool, optional): Print some statistics for property. Defaults to True.

    Returns:
        xtgeo.GridProperty: the extracted results
    """
    prop_name = prop_dict["name"]
    values = prop_dict["values"]
    xtgeo_prop = GridProperty(xtgeoegrid, name=prop_name)
    xtgeo_prop.values = values
    if describe:
        xtgeo_prop.describe()
    return xtgeo_prop


def init_exporter(config_path):
    """Initialize ExportData class

    Args:
        config_path (str): path to fmu config file
    """
    exp = ExportData(config=yaml_load(config_path))
    return exp


def main():
    """Run script"""
    args = parse_args()
    exporter = init_exporter(args.config_path)
    inc_path = Path(args.datafile).parent.parent
    egrid = export_egrid(args.datafile, exporter)
    export_from_simulation_run(args.datafile)
    # grid = export_grdecl_grid(args.grdecl_grid, exporter)
    # export_grdecl_props(inc_path, grid, exporter)


if __name__ == "__main__":
    main()
