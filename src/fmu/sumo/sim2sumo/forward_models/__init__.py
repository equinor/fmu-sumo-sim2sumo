import subprocess
from ert.config.forward_model_step import (
    ForwardModelStep,
    ForwardModelStepJSON,
)


class Sim2Sumo(ForwardModelStep):
    def __init__(self):
        super().__init__(
            name="SIM2SUMO",
            executable="sim2sumo",
            arglist=[
                '"--config_path"',
                "<S2S_CONF_PATH>",
                '"--env"',
                "<SUMO_MODE>",
            ],
            min_arg=5,
            max_arg=5,
            arg_types=[
                "STRING",
                "STRING",
                "STRING",
                "STRING",
                "STRING",
            ],
        )

    def validate_pre_realization_run(
        self, fm_step_json: ForwardModelStepJSON
    ) -> ForwardModelStepJSON:
        return fm_step_json

    def validate_pre_experiment(self) -> None:
        try:
            env = self.private_args["<SUMO_ENV>"]
        except KeyError:
            env = "prod"

        command = f"sumo_login -e {env} -m silent"
        return_code = subprocess.call(command, shell=True)

        assert (
            return_code == 0
        ), "Your config uses Sumo, run sumo_login to authenticate."
