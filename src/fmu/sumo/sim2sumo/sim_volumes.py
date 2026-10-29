import pandas as pd
import res2df


def extract_simulator_inplace(datafile_path):

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
        tuple[pd.DataFrame, date]: fipreports dataframe filtered to initial
        date, the initial date
    """

    # Keep only first date for initial in-place volumes
    first_date = df["DATE"].min()

    if isinstance(first_date, pd.Timestamp):
        first_date = first_date.date()

    dff = df[
        (df["DATATYPE"] == "ORIGINALLY IN PLACE") & (df["DATE"] == first_date)
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

    return dff
