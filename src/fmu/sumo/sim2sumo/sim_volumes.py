from datetime import date
from typing import Tuple

import pandas as pd
import res2df


def _filter_to_first_date(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, date]:
    """
    Filter fipreports dataframe to initial date.

    Args:
        df (pd.DataFrame): fipreports dataframe

    Returns:
        Tuple[pd.DataFrame, date]: fipreports dataframe filtered to initial
        date, the initial date
    """

    # Keep only first date for initial in-place volumes
    first_date = df["DATE"].min()
    dff = df[
        (df["DATATYPE"] == "ORIGINALLY IN PLACE") & (df["DATE"] == first_date)
    ].copy()

    # If sub-daily FIP reports are requested, there will be duplicate
    # entries for the same date. Remove duplicates.
    dff = dff.drop_duplicates(keep="first")

    return dff, first_date


def _split_zones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splitting into oil at reservoir conditions, and gas at reservoir conditions.

    Comments on volume columns in input fipreports dataframe:
    --- Liquid at surface ---
    STOIIP_OIL = liquid in reservoir, liquid at surface --> from oil zone
    ASSOCIATEDOIL_GAS = gas in reservoir, liquid at surface --> from gas zone
    STOIIP_TOTAL = liquid at surface (STOIIP_OIL + ASSOCIATEDOIL_GAS). Not used

    --- Gas at surface ---
    GIIP_GAS = gas in reservoir, gas at surface --> from gas zone
    ASSOCIATEDGAS_OIL = liquid in reservoir, gas at surface --> from oil zone
    GIIP_TOTAL = gas at surface (GIIP_OIL + ASSOCIATEDGAS_OIL). Not used

    Args:
        df (pd.DataFrame): clean fipreports dataframe

    Returns:
        pd.DataFrame: dataframe in standard resultss format
    """

    oil_rc = df[["DATE", "FIPNUM"]].copy()
    oil_rc["FLUID"] = "oil"
    oil_rc["STOIIP"] = df["STOIIP_OIL"]
    oil_rc["ASSOCIATEDGAS"] = df["ASSOCIATEDGAS_OIL"]

    gas_rc = df[["DATE", "FIPNUM"]].copy()
    gas_rc["FLUID"] = "gas"
    gas_rc["ASSOCIATEDOIL"] = df["ASSOCIATEDOIL_GAS"]
    gas_rc["GIIP"] = df["GIIP_GAS"]

    df_rc = pd.concat([oil_rc, gas_rc], ignore_index=True).fillna(0)
    df_rc = df_rc.sort_values(["DATE", "FIPNUM", "FLUID"]).reset_index(
        drop=True
    )

    return df_rc


def _get_fipreport_sr_format(df: pd.DataFrame) -> pd.DataFrame:
    # Remove unnecessary columns
    dff = df.copy()
    dff = dff.drop(columns=["DATATYPE", "FIPNAME", "TO_REGION", "WIIP_TOTAL"])
    dff = dff.rename(columns={"REGION": "FIPNUM"})

    df_zones = _split_zones(dff)

    return df_zones


def _split_zones_rst(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split restart data into oil & gas zones

    Args:
        df (pd.DataFrame): restart data

    Returns:
        pd.DataFrame: restart data split into oil & gas zones
    """

    cols = ["FIPNUM", "FLUID", "VOLUME", "NET", "PORV", "HCPV"]
    index_cols = ["FIPNUM", "FLUID"]

    above_goc_mask = df["SGAS"] > 0
    above_owc_mask = df["SOIL"] > 0
    # Gas zone defined as any cell containing SGAS > 0
    # All cells above GOC will be assigned to gas zone
    gas_zone = df[above_goc_mask].copy()
    # Oil zone defined as any cell containing SOIL > 0 and SGAS < 0
    # All cells above OWC and below GOC will be assigned to oil zone
    oil_zone = df[(above_owc_mask) & ~(above_goc_mask)].copy()

    gas_zone["FLUID"] = "gas"
    oil_zone["FLUID"] = "oil"

    gas_zone_sum = (
        gas_zone[cols]
        .groupby(index_cols)
        .sum()
        .rename(columns={"VOLUME": "BULK"})
    )
    oil_zone_sum = (
        oil_zone[cols]
        .groupby(index_cols)
        .sum()
        .rename(columns={"VOLUME": "BULK"})
    )
    all_zones_sum = pd.concat((oil_zone_sum, gas_zone_sum))

    return all_zones_sum


def _restart_date_exists(datafile_path: str, date: date) -> bool:
    """
    Check if restart date exists for the fipreports date

    Args:
        datafile_path (str): path to .DATA file
        date (date): fipreports date

    Returns:
        bool
    """

    availables_dates = res2df.grid.get_available_rst_dates(
        res2df.ResdataFiles(datafile_path)
    )

    return date in availables_dates


def _get_rst_data(datafile_path: str, date: date) -> pd.DataFrame:
    # BULK, NET, PORV, HCPV
    cols = ["FIPNUM", "VOLUME", "PORV", "NTG", "SGAS", "SOIL"]

    df = res2df.grid.df(res2df.ResdataFiles(datafile_path), rstdates=date)

    # Using reindex so any missing columns get NaN rather than failing
    df = df.reindex(columns=cols)

    # Calculate columns that don't exist in the simulator/res2df output
    df["NET"] = df["VOLUME"] * df["NTG"]
    df["HCPV"] = df["PORV"] * (df["SOIL"] + df["SGAS"])

    return df


def extract_simulator_inplace(datafile_path: str) -> pd.DataFrame:
    """
    Load fipreports dataframe using res2df and convert to in-place
    standard restults format. Add volume data from grid if available.

    Args:
        datafile_path (str): _description_

    Returns:
        pd.DataFrame: _description_
    """

    fipreport = res2df.fipreports.df(
        res2df.ResdataFiles(datafile_path),
        fipname="FIPNUM",
    )
    fipreport, fipreport_date = _filter_to_first_date(fipreport)
    fipreport_sr = _get_fipreport_sr_format(fipreport)

    # Load restart data and add BULK, NET, PORV and HCPV
    if _restart_date_exists(datafile_path, fipreport_date):
        df_grid = _get_rst_data(datafile_path, fipreport_date)
        df_grid_zones = _split_zones_rst(df_grid)

        fipreport_sr = fipreport_sr.merge(
            df_grid_zones.reset_index(), on=["FIPNUM", "FLUID"]
        )

    return fipreport_sr
