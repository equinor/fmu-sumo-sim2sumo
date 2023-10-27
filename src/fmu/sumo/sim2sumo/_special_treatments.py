"""Special treatment of some options used in ecl2df"""
from ecl2df.common import convert_lyrlist_to_zonemap, parse_lyrfile


def final_touches(options):
    """Convert dictionary options further

    Args:
        options (dict): the input options

    Returns:
        dict: options after special treatment
    """
    if "zonemap" in options:
        options["zonemap"] = convert_lyrlist_to_zonemap(
            parse_lyrfile(options["zonemap"])
        )
    return options
