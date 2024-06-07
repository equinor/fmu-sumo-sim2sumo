"""Module that can write tailor made scripts for certain tasks"""

import logging
import re
from pathlib import Path

TOOLS_PATH = Path(__file__).parent / "_tools/"


def fetch_top_docstring(text_string: str) -> str:

    logger = logging.getLogger(__file__ + ".fetch_top_string")
    primary_docstring = re.compile(r'^"""(.+)"""', re.MULTILINE)
    results = None
    try:
        results = primary_docstring.search(text_string).group(1)
    except AttributeError:
        logger.warning("No docstring found")
    return results


def list_tools():
    """List scripts in tools"""
    logger = logging.getLogger(__file__ + ".list_tools")
    glob_res = list(TOOLS_PATH.glob("*.txt"))
    logger.debug(glob_res)
    tools = {
        re.sub(r"\..*", "", res.name): fetch_top_docstring(
            res.read_text(encoding="utf-8")
        )
        for res in glob_res
    }
    logger.debug("Available tools are %s", tools)
    return tools


def fetch_tool(tool_name: str, location: str):
    """Write local copy of tool script

    Args:
        tool_name (str): the name of the script
        location (str): the output location
    """
    script = TOOLS_PATH / f"{tool_name}.txt"
    out_path = Path(location) / f"{tool_name}.py"
    out_path.write_text(script.read_text())
    print(f"Written {str(out_path)}")
