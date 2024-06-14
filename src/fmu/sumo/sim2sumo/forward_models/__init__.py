import subprocess
from ert import (
    ForwardModelStepJSON,
    ForwardModelStepPlugin,
    ForwardModelStepValidationError,
)


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
