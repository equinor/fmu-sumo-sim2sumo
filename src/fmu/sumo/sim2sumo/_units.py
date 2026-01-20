import re

ALLOWED_UNIT_SYSTEMS = ["METRIC", "FIELD", "LAB", "PVT-M"]


def read_file_generator(file_path: str):
    with open(file_path, "r") as file:
        for line in file:
            yield line.strip()


def get_datafile_unit_system(datafile_path: str) -> str:
    """
    Parse datafile and find unit system.

    Args:
        datafile_path (str): path to datafile

    Returns:
        str: unit system name
    """
    unit_system = ""

    # Match any of the allowed unit systems
    # Match beginning of the line to end of word
    # Don't match strings after comments (syntax: "--") to 1) avoid matching
    # commented-out keywords & 2) returning the unit system and a following comment
    pattern = rf"^({'|'.join(ALLOWED_UNIT_SYSTEMS)})\b(?=\s*--|$)"

    for line in read_file_generator(datafile_path):
        if re.search(pattern, line):
            unit_system = re.search(pattern, line)[0]
            break

    if not unit_system:
        print(
            f"Unit system definition not found in datafile: {datafile_path}. Defaulting to METRIC."
        )
        return "METRIC"

    return unit_system


# Maps of quantities to units
# Some quantities share the same unit across unit systems and can be represented
# as variables
LENGTH = {"METRIC": "m", "FIELD": "ft", "LAB": "cm", "PVT-M": "m"}
TIME = {
    "METRIC": "day",
    "FIELD": "day",
    "LAB": "hr",
    "PVT-M": "day",
}
PERM = "mD"
PRESSURE = {
    "METRIC": "bar",
    "FIELD": "psi",
    "LAB": "atm",
    "PVT-M": "atm",
}
LIQUID_SURFACE_VOL = {
    "METRIC": "Sm3",
    "FIELD": "stb",
    "LAB": "scc",
    "PVT-M": "Sm3",
}
GAS_SURFACE_VOL = {
    "METRIC": "Sm3",
    "FIELD": "Mscf",
    "LAB": "scc",
    "PVT-M": "Sm3",
}
RESERVOIR_VOL = {"METRIC": "rm3", "FIELD": "rb", "LAB": "rcc", "PVT-M": "rm3"}
VOLUME = {"METRIC": "m3", "FIELD": "ft3", "LAB": "cc", "PVT-M": "m3"}
LIQUID_FVF = {
    key: f"{RESERVOIR_VOL[key]}/{LIQUID_SURFACE_VOL[key]}"
    for key in LIQUID_SURFACE_VOL
}
GAS_FVF = {
    key: f"{RESERVOIR_VOL[key]}/{GAS_SURFACE_VOL[key]}"
    for key in GAS_SURFACE_VOL
}
# Transmissibility
VISCOSITY = "cP"
RESERVOIR_VOL_RATE = {
    key: f"{RESERVOIR_VOL[key]}/{TIME[key]}" for key in RESERVOIR_VOL
}
TRANSMISSIBILITY = {
    key: f"{VISCOSITY}.{RESERVOIR_VOL_RATE[key]}/{PRESSURE[key]}"
    for key in RESERVOIR_VOL
}

PORO = {
    key: f"{RESERVOIR_VOL[key]}/{RESERVOIR_VOL[key]}" for key in RESERVOIR_VOL
}
RELPERM = f"{PERM}/{PERM}"
SATURATION = {
    key: f"{RESERVOIR_VOL[key]}/{RESERVOIR_VOL[key]}" for key in RESERVOIR_VOL
}
NTG = {key: f"{LENGTH[key]}/{LENGTH[key]}" for key in LENGTH}


def get_all_properties_units(unit_system: str) -> dict:
    """
    Get a map of grid properties:units for the given unit system.

    Args:
        unit_system (str): unit system name

    Returns:
        dict: map of grid properties:units for the given unit system
    """
    if unit_system not in ALLOWED_UNIT_SYSTEMS:
        raise ValueError(
            f"Unrecognised unit_system '{unit_system}'. Must be one of {ALLOWED_UNIT_SYSTEMS}"
        )

    property_units = {
        "SATURATION": SATURATION[unit_system],
        "DEPTH": LENGTH[unit_system],
        "BOTTOM": LENGTH[unit_system],
        "TOPS": LENGTH[unit_system],
        "DX": LENGTH[unit_system],
        "DY": LENGTH[unit_system],
        "DZ": LENGTH[unit_system],
        "FAULTDIST": LENGTH[unit_system],
        "TRANX": TRANSMISSIBILITY[unit_system],
        "TRANY": TRANSMISSIBILITY[unit_system],
        "TRANZ": TRANSMISSIBILITY[unit_system],
        "PERMX": PERM,
        "PERMY": PERM,
        "PERMZ": PERM,
        "PORO": PORO[unit_system],
        "NTG": NTG[unit_system],
        "PORV": RESERVOIR_VOL[unit_system],
        "SWAT": SATURATION[unit_system],
        "SWATINIT": SATURATION[unit_system],
        "SWCR": SATURATION[unit_system],
        "SWL": SATURATION[unit_system],
        "SWU": SATURATION[unit_system],
        "SWLPC": SATURATION[unit_system],
        "SGLPC": SATURATION[unit_system],
        "SGAS": SATURATION[unit_system],
        "SGL": SATURATION[unit_system],
        "SGU": SATURATION[unit_system],
        "SGWCR": SATURATION[unit_system],
        "SGCR": SATURATION[unit_system],
        "SOIL": SATURATION[unit_system],
        "SOGCR": SATURATION[unit_system],
        "SOWCR": SATURATION[unit_system],
        "KRG": RELPERM,
        "KRO": RELPERM,
        "KRW": RELPERM,
        "KRGR": RELPERM,
        "KROR": RELPERM,
        "KROGR": RELPERM,
        "KRORW": RELPERM,
        "KRWR": RELPERM,
        "PRESSURE": PRESSURE[unit_system],
        "PCG": PRESSURE[unit_system],
        "PCW": PRESSURE[unit_system],
        "SFIPOIL": LIQUID_SURFACE_VOL[unit_system],
        "SFIPGAS": GAS_SURFACE_VOL[unit_system],
    }

    return property_units
