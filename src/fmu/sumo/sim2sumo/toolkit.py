"""Module that can write tailor made scripts for certain tasks"""

import logging
import re
from pathlib import Path

TOOLS_PATH = Path(__file__).parent / "_tools/"


def fetch_top_docstring(text_string: str) -> str:

    logger = logging.getLogger(__file__ + ".fetch_top_string")
    logger.debug("This is the string to extract from %s", text_string)
    primary_docstring = re.compile(r'"""(.*?)"""')
    new_line = re.compile(r"(\n|\r)")
    try:
        return primary_docstring.search(new_line.sub("", text_string)).group(1)
    except AttributeError:

        return None


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
    tools_str = ""
    for tool_name, description in tools.items():

        tools_str += f"{tool_name:}\n" + description
    return tools_str


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
