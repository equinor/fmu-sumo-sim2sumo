import pandas as pd
import res2df


def _get_fipreport(datafile_path: str):

    df = res2df.fipreports.df(
        res2df.ResdataFiles(datafile_path),
        fipname="FIPNUM",
    )

    return df


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean fipreports dataframe

    Args:
        df (pd.DataFrame): fipreports dataframe

    Returns:
        pd.DataFrame: clean fipreports dataframe
    """

    dff = df[df["DATATYPE"] != "ORIGINALLY IN PLACE"]
    dff = dff.drop(columns=["DATATYPE", "FIPNAME", "TO_REGION", "WIIP_TOTAL"])
    dff = dff.rename(columns={"REGION": "FIPNUM"})

    return dff


def _split_zones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splitting into oil & gas zones

    Comments on volume columns in input fipreports dataframe:
    STOIIP_OIL = oil zone. Liquid in reservoir, liquid at surface
    ASSOCIATEDOIL_GAS = gas zone. Gas in reservoir, liquid at surface
    STOIIP_TOTAL = oil + gas zone. Liquid at surface
    GIIP_GAS = gas zone. Gas in reservoir, gas at surface
    ASSOCIATEDGAS_OIL = oil zone. Liquid in reservoir, gas at surface
    GIIP_TOTAL = oil + gas zone. Gas at surface

    Args:
        df (pd.DataFrame): clean fipreports dataframe

    Returns:
        pd.DataFrame: dataframe in standard restults format
    """

    oil_zone = df[["DATE", "FIPNUM"]].copy()
    oil_zone["ZONE"] = "oil"
    oil_zone["STOIIP"] = df["STOIIP_OIL"]
    oil_zone["ASSOCIATEDGAS"] = df["ASSOCIATEDGAS_OIL"]

    gas_zone = df[["DATE", "FIPNUM"]].copy()
    gas_zone["ZONE"] = "gas"
    gas_zone["ASSOCIATEDOIL"] = df["ASSOCIATEDOIL_GAS"]
    gas_zone["GIIP"] = df["GIIP_GAS"]

    df_zones = pd.concat([oil_zone, gas_zone], ignore_index=True).fillna(0)
    df_zones = df_zones.sort_values(["DATE", "FIPNUM", "ZONE"]).reset_index(
        drop=True
    )

    return df_zones


def extract_simulator_inplace(datafile_path: str) -> pd.DataFrame:
    """
    Load fipreports dataframe using res2df and convert to in-place
    standard restults format.

    Args:
        datafile_path (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df = _get_fipreport(datafile_path)
    df = _clean_dataframe(df)
    df_zones = _split_zones(df)

    return df_zones
