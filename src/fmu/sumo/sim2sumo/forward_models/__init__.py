import subprocess

from ert import (  # type: ignore
    ForwardModelStepDocumentation,
    ForwardModelStepJSON,
    ForwardModelStepPlugin,
    ForwardModelStepValidationError,
    plugin,
)
from ert.plugins.plugin_manager import hook_implementation


class Sim2Sumo(ForwardModelStepPlugin):
    def __init__(self):
        super().__init__(
            name="SIM2SUMO",
            command=[
                "sim2sumo",
                "--config_path",
                "<S2S_CONF_PATH>",
                "--env",
                "<SUMO_ENV>",
            ],
            default_mapping={
                "<S2S_CONF_PATH>": "fmuconfig/output/global_variables.yml",
                "<SUMO_ENV>": "prod",
            },
            stderr_file="sim2sumo.stderr",
            stdout_file="sim2sumo.stdout",
        )

    def validate_pre_realization_run(
        self, fm_step_json: ForwardModelStepJSON
    ) -> ForwardModelStepJSON:
        return fm_step_json

    def validate_pre_experiment(
        self, fm_step_json: ForwardModelStepJSON
    ) -> None:
        env = fm_step_json["argList"][3]
        command = f"sumo_login -e {env} -m silent"
        return_code = subprocess.call(command, shell=True)

        err_msg = (
            "Your config uses Sumo"
            ", please authenticate using:"
            f"sumo_login{f' -e {env}' if env != 'prod' else ''}"
        )

        if return_code != 0:
            raise ForwardModelStepValidationError(err_msg)

    @staticmethod
    def documentation() -> ForwardModelStepDocumentation | None:
        return ForwardModelStepDocumentation(
            description="""
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

""",
            examples="""
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
""",
            category="export",
            source_package="sim2sumo",
        )


@hook_implementation
@plugin(name="fmu_sumo_sim2sumo")
def installable_forward_model_steps():
    return [Sim2Sumo]
