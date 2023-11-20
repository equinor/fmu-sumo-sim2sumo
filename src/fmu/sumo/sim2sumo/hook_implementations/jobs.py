import importlib
import os
from pkg_resources import resource_filename

from ert.shared.plugins.plugin_manager import hook_implementation
from ert.shared.plugins.plugin_response import plugin_response


def _get_jobs_from_directory(directory):
    """Do a filesystem lookup in a directory to check
    for available ERT forward models"""
    resource_directory = resource_filename("fmu", directory)

    all_files = [
        os.path.join(resource_directory, f)
        for f in os.listdir(resource_directory)
        if os.path.isfile(os.path.join(resource_directory, f))
    ]
    return {os.path.basename(path): path for path in all_files}


# pylint: disable=no-value-for-parameter
@hook_implementation
@plugin_response(
    plugin_name="fmu_sumo_sim2sumo"
)  # pylint: disable=no-value-for-parameter
def installable_jobs():
    return _get_jobs_from_directory("sumo/sim2sumo/config_jobs")


def _get_module_variable_if_exists(module_name, variable_name, default=""):
    try:
        script_module = importlib.import_module(module_name)
    except ImportError:
        return default

    return getattr(script_module, variable_name, default)


@hook_implementation
@plugin_response(
    plugin_name="fmu_sumo_sim2sumo"
)  # pylint: disable=no-value-for-parameter
def job_documentation(job_name):
    sumo_fmu_jobs = set(installable_jobs().data.keys())
    if job_name not in sumo_fmu_jobs:
        return None

    module_name = "jobs.scripts.{}".format(job_name.lower())

    description = _get_module_variable_if_exists(
        module_name=module_name, variable_name="description"
    )
    examples = _get_module_variable_if_exists(
        module_name=module_name, variable_name="examples"
    )
    category = _get_module_variable_if_exists(
        module_name=module_name, variable_name="category", default="other"
    )

    return {
        "description": description,
        "examples": examples,
        "category": category,
    }
