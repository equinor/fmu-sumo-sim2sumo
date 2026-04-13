"""Configuration container and builder for sim2sumo."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

from fmu.dataio._global_config import load_global_config
from fmu.datamodels.fmu_results.global_configuration import GlobalConfiguration
from fmu.sumo.sim2sumo._special_treatments import (
    DEFAULT_RST_PROPS,
    DEFAULT_SUBMODULES,
    SUBMODULES,
)
from fmu.sumo.sim2sumo.common import (
    find_datafiles,
    validate_sim2sumo_config,
    yaml_load,
)


@dataclass
class Sim2SumoConfig:
    """Configuration container for sim2sumo.

    Attributes:
        global_config: Validated FMU global configuration from fmu-dataio.
        sim2sumo: Per-datafile mapping of submodules to their options.
    """

    global_config: GlobalConfiguration
    sim2sumo: dict[Path, dict[str, dict[str, Any]]] = field(
        default_factory=dict
    )

    @classmethod
    def from_global_variables(
        cls, config_path: Path | str | None = None
    ) -> Self:
        """Build a Sim2SumoConfig from a global_variables.yml path.

        The FMU global configuration is loaded via
        ``fmu.dataio._global_config.load_global_config`` which first checks for
        a ``.fmu/`` directory and then falls back to the YAML file at
        *config_path*.  The ``sim2sumo`` section is read from the same YAML
        file (it is not part of the GlobalConfiguration model).

        Args:
            config_path: Path to ``global_variables.yml``.  When *None*,
                ``load_global_config`` will search in the standard locations.

        Returns:
            A fully resolved :class:`Sim2SumoConfig`.
        """
        resolved_path = Path(config_path) if config_path is not None else None
        global_config = load_global_config(resolved_path)

        # The sim2sumo section lives in the same YAML but is not part of
        # GlobalConfiguration. We need to load the raw YAML to get it.
        raw: dict[str, Any] = {}
        if resolved_path is not None:
            raw = yaml_load(resolved_path)
        else:
            # Try the standard location used by load_global_config
            for candidate in [
                Path("fmuconfig/output/global_variables.yml"),
                Path("../../fmuconfig/output/global_variables.yml"),
            ]:
                if candidate.is_file():
                    raw = yaml_load(candidate)
                    break

        simconfig = raw.get("sim2sumo", {})
        sim2sumo_config = _build_sim2sumo_config(simconfig)

        return cls(
            global_config=global_config,
            sim2sumo=sim2sumo_config,
        )


def _build_sim2sumo_config(
    simconfig: dict[str, Any],
) -> dict[Path, dict[str, dict[str, Any]]]:
    """Resolve datafiles and build the per-file submodule config mappings.

    Args:
        simconfig: The ``sim2sumo`` section from the raw YAML config.

    Returns:
        Mapping of datafile paths to their submodule config dicts.
    """
    validate_sim2sumo_config(simconfig)

    datafile = simconfig.get("datafile")
    datatype = simconfig.get("datatypes")

    if datatype is None:
        default_submods = DEFAULT_SUBMODULES
    elif "all" in datatype:
        default_submods = SUBMODULES
    elif isinstance(datatype, list):
        default_submods = datatype
    else:
        default_submods = [datatype]

    submods = default_submods

    paths: list[Path] = []
    if datafile:
        for file in datafile:
            if isinstance(file, dict):
                (((filepath, file_submods)),) = file.items()
                submods = file_submods or default_submods
            else:
                filepath = file

            path = Path(filepath)
            if path.is_file():
                paths.append(path)
            else:
                paths.extend(find_datafiles(path))
    else:
        paths.extend(find_datafiles(None))

    sim2sumoconfig: dict[Path, dict[str, dict[str, Any]]] = {}
    for datafile_path in paths:
        sim2sumoconfig[datafile_path] = {}
        for submod in submods:
            sim2sumoconfig[datafile_path][submod] = {"arrow": True}

            # Restart properties config
            if submod == "grid":
                # Get rstprops config if it is provided
                rstprops = simconfig.get("rstprops")
                if rstprops:
                    sim2sumoconfig[datafile_path][submod]["rstprops"] = [
                        x.upper() for x in rstprops
                    ]
                else:
                    sim2sumoconfig[datafile_path][submod]["rstprops"] = (
                        DEFAULT_RST_PROPS
                    )

    return sim2sumoconfig
