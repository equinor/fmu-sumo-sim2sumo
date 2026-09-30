import pandas as pd
import res2df


def _get_fipreport(datafile_path: str):

    df = res2df.fipreports.df(
        res2df.ResdataFiles(datafile_path),
        fipname="FIPNUM",
    )

    return df


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    dff = df[df["DATATYPE"] != "ORIGINALLY IN PLACE"]
    dff = dff.drop(columns=["DATATYPE", "FIPNAME", "TO_REGION", "WIIP_TOTAL"])
    dff = dff.rename(columns={"REGION": "FIPNUM"})

    return dff


def _split_zones(df: pd.DataFrame) -> pd.DataFrame:
    # Splitting into oil & gas zones

    # STOIIP_OIL = oil zone. Liquid in reservoir, liquid at surface
    # ASSOCIATEDOIL_GAS = gas zone. Gas in reservoir, liquid at surface
    # STOIIP_TOTAL = oil + gas zone. Liquid at surface
    # GIIP_GAS = gas zone. Gas in reservoir, gas at surface
    # ASSOCIATEDGAS_OIL = oil zone. Liquid in reservoir, gas at surface
    # GIIP_TOTAL = oil + gas zone. Gas at surface

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


def extract_simulator_inplace(datafile_path: str):
    df = _get_fipreport(datafile_path)
    df = _clean_dataframe(df)
    df_zones = _split_zones(df)

    return df_zones


# def extract_simulator_inplace(datafile_path: str):
#     """
#     _summary_

#     Args:
#         datafile_path (str): _description_

#     Returns:
#         _type_: _description_
#     """
#     df = res2df.summary.df(
#         res2df.ResdataFiles(datafile_path),
#         column_keys=[
#             "ROIP:*",
#             "ROIPG:*",
#             "ROIPL:*",
#             "RGIP:*",
#             "RGIPG:*",
#             "RGIPL:*",
#         ],
#         end_date="first",
#     ).reset_index()  # get DATE as column

#     df_melt = _melt_df(df)
#     df_pivot = _pivot_df(df_melt)

#     # TODO check required vectors present in df_pivot.
#     # Log a warning and continue rather than raising an error?
#     # At least ROIP.

#     # Minimum vectors required to get information about in-place volumes
#     assert "ROIP" in df_pivot.columns
#     assert "RGIP" in df_pivot.columns

#     df_zones = _convert_to_sr_format(df_pivot)

#     print(df_melt)
#     print(df_pivot)
#     print(df_zones)

#     return df_zones


# def _melt_df(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Rearrange dataframe from wide to long, with the following columns:
#     "DATE", "value", "vector", "FIPNUM".

#     Args:
#         df (pd.DataFrame): _description_

#     Returns:
#         pd.DataFrame: _description_
#     """
#     df_melt = df.melt(
#         id_vars="DATE", var_name="vector_name", value_name="value"
#     )

#     df_melt[["vector", "FIPNUM"]] = df_melt["vector_name"].str.split(
#         ":", n=1, expand=True
#     )
#     df_melt["FIPNUM"] = df_melt["FIPNUM"].astype(int)
#     df_melt = df_melt.drop(columns=("vector_name"))

#     return df_melt


# def _pivot_df(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Rearrange dataframe.

#     Example columns: "DATE", "FIPNUM", "RGIP", "ROIP".

#     Args:
#         df (pd.DataFrame): _description_

#     Returns:
#         pd.DataFrame: _description_
#     """
#     df_pivot = df.pivot_table(
#         index=["DATE", "FIPNUM"], columns="vector", values="value"
#     ).reset_index()

#     # TODO ensure date is date and not object
#     df_pivot = df_pivot[df_pivot["DATE"] == df_pivot["DATE"].min()]
#     df_pivot = df_pivot.sort_values(by=("FIPNUM"))

#     return df_pivot


# def _build_oil_zone_df(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     _summary_

#     Args:
#         df (pd.DataFrame): _description_

#     Returns:
#         pd.DataFrame: _description_
#     """

#     oil_zone_rows = df["ROIPL"].gt(0)

#     oil_zone = df.loc[oil_zone_rows].copy()
#     oil_zone["FLUID"] = "oil"
#     oil_zone["GIIP"] = 0  # pd.NA
#     oil_zone["ASSOCIATEDOIL"] = 0  # pd.NA
#     oil_zone["STOIIP"] = oil_zone[
#         "ROIPL"
#     ]  # liquid at surface (liquid in reservoir)

#     if "RGIPL" in oil_zone.columns:
#         oil_zone["ASSOCIATEDGAS"] = oil_zone[
#             "RGIPL"
#         ]  # liquid in reservoir, gas at surface

#     return oil_zone


# def _build_gas_zone_df(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     _summary_

#     Args:
#         df (pd.DataFrame): _description_

#     Returns:
#         pd.DataFrame: _description_
#     """

#     gas_zone_rows = df["RGIPG"].gt(0)

#     gas_zone = df.loc[gas_zone_rows].copy()
#     gas_zone["FLUID"] = "gas"
#     gas_zone["STOIIP"] = 0  # pd.NA
#     gas_zone["ASSOCIATEDGAS"] = 0  # pd.NA
#     gas_zone["GIIP"] = gas_zone["RGIPG"]  # gas at surface

#     if "ROIPG" in gas_zone.columns:
#         gas_zone["ASSOCIATEDOIL"] = gas_zone[
#             "ROIPG"
#         ]  # gas in reservoir, liquid at surface

#     return gas_zone


# def _build_hc_zone_df(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     _summary_

#     Args:
#         df (pd.DataFrame): _description_

#     Returns:
#         pd.DataFrame: _description_
#     """
#     # ROIP and RGIP report volumes at surface conditions. With only ROIP, it is
#     # not possible to say whether the oil at surface conditions was oil in the
#     # reservoir, or gas in the reservoir. The same applies for RGIP and gas.
#     # Therefore it is not possible to say whether the surface phase comes from
#     # the oil zone or the gas zone. In this case, a combined hydrocarbon zone
#     # table is created.
#     df_zones = df.copy()
#     df_zones["STOIIP"] = df["ROIP"]
#     df_zones["GIIP"] = df["RGIP"]
#     df_zones["FLUID"] = "oil + gas"
#     df_zones = df_zones.sort_values(["FLUID", "FIPNUM"])
#     df_zones = df_zones.reset_index(drop=True)
#     df_zones = df_zones[["FLUID", "FIPNUM", "STOIIP", "GIIP"]]

#     return df_zones


# def _convert_to_sr_format(df: pd.DataFrame) -> pd.DataFrame:
#     # STOIIP (from ROIPL, only for oil zone). Oil at surface conditions
#     # GIIP (from RGIPL, only for gas zone). Gas at surface conditions
#     # ASSOCIATEDGAS (from RGIPL, only for oil zone). Oil in reservoir, gas at surface
#     # ASSOCIATEDOIL (from ROIPG, only for gas zone). Gas in reservoir, oil at surface
#     # STOIIP_TOTAL = STOIIP + ASSOCIATEDOIL. Total oil at surface
#     # GIIP_TOTAL = GIIP + ASSOCIATEDGAS. Total gas at surface

#     # Split table into oil and gas zones

#     # TODO what happens if a field only has an oil or gas zone?
#     # Can we use just ROIP or RGIP?

#     # TODO add logging about the level of detail in the volume table
#     if ("ROIPL" in df.columns) & ("RGIPG" in df.columns):
#         oil_zone = _build_oil_zone_df(df)
#         gas_zone = _build_gas_zone_df(df)

#         df_zones = pd.concat(
#             [oil_zone, gas_zone],
#             axis=0,
#             ignore_index=True,
#         ).sort_values(["FLUID", "FIPNUM"])

#         df_zones = df_zones.reset_index(drop=True)
#         df_zones = df_zones[
#             [
#                 "FLUID",
#                 "FIPNUM",
#                 "STOIIP",
#                 "GIIP",
#                 "ASSOCIATEDGAS",
#                 "ASSOCIATEDOIL",
#             ]
#         ]

#     # TODO add logging about the level of detail in the volume table
#     elif ("ROIP" in df.columns) & ("RGIP" in df.columns):
#         df_zones = _build_hc_zone_df(df)

#     # TODO this should not be encountered when running through extract_simulator_inplace,
#     # as a check is made for ROIP and RGIP in df_pivot
#     else:
#         raise ValueError(
#             "Required vectors not present. At least ROIP and RGIP must be present \
#             in the summary data to calculate in-place volumes."
#         )

#     return df_zones
