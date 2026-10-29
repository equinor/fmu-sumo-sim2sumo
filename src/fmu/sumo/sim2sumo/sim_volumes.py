import pandas as pd
import res2df

SR_COLS: list[str] = [
    "DATE",
    "FLUID",
    "FIPNUM",
    "HCPV",
    "STOIIP",
    "GIIP",
    "ASSOCIATEDGAS",
    "ASSOCIATEDOIL",
]


def extract_simulator_inplace(datafile_path):
    """
    Requires RPTSOL FIP = 2 (or 3)
    """

    fipreports = res2df.fipreports.df(
        datafile_path.with_suffix(".PRT"), fipname="FIPNUM", resv=True
    )

    # Filter to a single fipreport for the first date
    fipreport = _filter_to_first_date(fipreports)
    fipreport_sr = _get_fipreport_sr_format(fipreport)

    return fipreport_sr


def _filter_to_first_date(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Filter fipreports dataframe to initial date.

    Args:
        df (pd.DataFrame): fipreports dataframe

    Returns:
        pd.DataFrame: fipreports dataframe filtered to initial date
    """

    # Keep only first date for initial in-place volumes
    dff = df[
        (df["DATATYPE"] == "ORIGINALLY IN PLACE")
        & (df["DATE"] == df["DATE"].min())
    ].copy()

    # If sub-daily FIP reports are requested, there will be duplicate
    # entries for the same date. Remove duplicates.
    dff = dff.drop_duplicates(keep="first")

    return dff


def _get_fipreport_sr_format(df: pd.DataFrame) -> pd.DataFrame:
    # Remove unnecessary columns
    dff = df.copy()
    dff = dff.drop(columns=["DATATYPE", "FIPNAME", "TO_REGION", "WIIP_TOTAL"])
    dff = dff.rename(columns={"REGION": "FIPNUM"})

    df_zones = _split_zones(dff)

    return df_zones


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

    --- Reservoir volumes ---
    HCPV_GAS = gas reservoir pore volumes
    HCPV_OIL = oil reservoir pore volumes
    WATPV_TOTAL = water reservoir pore volumes. Not used as there is not enough
    information to split between the water, oil and gas zones
    PORV_TOTAL = oil + gas + water reservoir pore volumes. Not used for the same
    reason as WATPV_TOTAL
    HCPV_TOTAL = oil + gas reservoir pore volumes. Not used


    Args:
        df (pd.DataFrame): clean fipreports dataframe

    Returns:
        pd.DataFrame: dataframe in standard resultss format
    """

    oil_rc = df[["DATE", "FIPNUM"]].copy()
    oil_rc["FLUID"] = "oil"
    oil_rc["STOIIP"] = df["STOIIP_OIL"]
    oil_rc["ASSOCIATEDGAS"] = df["ASSOCIATEDGAS_OIL"]
    oil_rc["HCPV"] = df["HCPV_OIL"]

    gas_rc = df[["DATE", "FIPNUM"]].copy()
    gas_rc["FLUID"] = "gas"
    gas_rc["ASSOCIATEDOIL"] = df["ASSOCIATEDOIL_GAS"]
    gas_rc["GIIP"] = df["GIIP_GAS"]
    gas_rc["HCPV"] = df["HCPV_GAS"]

    df_rc = pd.concat([oil_rc, gas_rc], ignore_index=True)

    df_rc = df_rc[SR_COLS]  # for correct column order
    # Drop empty columns e.g. if HCPV is missing because
    # RPTSOL FIP = 2 (or 3) was not requested.
    df_rc = df_rc.dropna(axis=1, how="all")
    # Fill rows with intermittent nan, e.g. gas colums for FLUID == 'oil'
    df_rc = df_rc.fillna(0)
    df_rc = df_rc.sort_values(["DATE", "FIPNUM", "FLUID"]).reset_index(
        drop=True
    )

    return df_rc
