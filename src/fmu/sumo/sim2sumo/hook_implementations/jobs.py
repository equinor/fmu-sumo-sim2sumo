"""Install in ert, including documentation to ert-docs"""

import os
import sys
from pathlib import Path

from ert.shared.plugins.plugin_manager import hook_implementation
from ert.shared.plugins.plugin_response import plugin_response

DESCRIPTION = """
Makes result simulator (Eclipse, OPM, IX) available in sumo.
This is done by SIM2SUMO in a three step process:
1. Data is extracted in arrow format using res2df
2. Corresponding metadata are generated via fmu-dataio
3. Data and metadata are then uploaded to Sumo using fmu-sumo-upload
SIM2SUMO defaults to extract summary, rft and well completion data, but
you can configure it to extract any datatype that res2df can produce.

SIM2SUMO finds results from the eclipse/model/ folder in your realization.
It looks for files with suffix .DATA, and then finds the eclipse run results.
This means that if you have more than one eclipse run in that folder, you will
get the specified result from all those runs.
The results are stored in Sumo with name <name of datafile> without the
habituary -<IENS> at the end.
Tagname will be name of datatype extracted.

E.G: summary files extracted from an eclipse run named DROGON-1, will be
stored in Sumo with name: DROGON, tagname: summary, and fmu.realization.id: 1

 Pre-requisites: SIM2SUMO is dependent on a configuration yaml file.
                (defaulted to ../../fmuconfig/output/global_variables.yml).
                This file needs to contain masterdata, and if you want
                to have some custom configurations, this is also done here .
                See examples.

"""
EXAMPLES = """
1. Extracting the defaults, with fmu_config in the standard location:

   In an Ert config file
   FORWARD_MODEL ECLIPSE100(...)
   --Note: SIM2SUMO must come after ECLIPSE100
   FORWARD_MODEL SIM2SUMO(<S2S_CONFIG_PATH>=path/to/config/file)

2. Extracting the defaults, but with fmu_config in a non standard location:

   In an Ert config file
   FORWARD_MODEL ECLIPSE100(...)
   --Note: SIM2SUMO must come after ECLIPSE100
   FORWARD_MODEL SIM2SUMO(<S2S_CONFIG_PATH>=path/to/config/file)

3. When you want to configure the fmu_config file to control what SIM2SUMO
   produces add section sim2sumo to your fmu_config file.
   (for inclusion of the forward model in your ert setup see examples 1. or 2.)
"""
PLUGIN_NAME = "SIM2SUMO"


def _get_jobs_from_directory(directory):
    """Do a filesystem lookup in a directory to check
    for available ERT forward models"""
    resource_directory = (
        Path(sys.modules["fmu.sumo.sim2sumo"].__file__).parent / directory
    )
    all_files = [
        os.path.join(resource_directory, f)
        for f in os.listdir(resource_directory)
        if os.path.isfile(os.path.join(resource_directory, f))
    ]
    return {os.path.basename(path): path for path in all_files}


# pylint: disable=no-value-for-parameter
@hook_implementation
@plugin_response(
    plugin_name=PLUGIN_NAME
)  # pylint: disable=no-value-for-parameter
def installable_jobs():
    """Return installable jobs

    Returns:
        dictionary: the jobs to install
    """
    return _get_jobs_from_directory("config_jobs")


@hook_implementation
@plugin_response(
    plugin_name=PLUGIN_NAME
)  # pylint: disable=no-value-for-parameter
def job_documentation(job_name):
    sumo_fmu_jobs = set(installable_jobs().data.keys())
    if job_name not in sumo_fmu_jobs:
        return None

    return {
        "description": DESCRIPTION,
        "examples": EXAMPLES,
        "category": "export",
    }
