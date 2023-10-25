# fmu-sumo-sim2sumo
Makes reservoir simulator (Eclipse, [OPM](https://opm-project.org/)) results available through Sumo.

Uploads files from reservoir simulators to azure assisted by Sumo. This is done in a three step process.

1. Data is extracted in arrow format using [ecl2df](https://github.com/equinor/ecl2df).
2. Corresponding metadata is generated via [fmu-dataio](https://github.com/equinor/fmu-dataio).
3. Data and metadata is then uploaded to Sumo using [fmu-sumo-uploader](https://github.com/equinor/fmu-sumo-uploader).

The entire process is triggered by an ERT forward model which is available as SIM2SUMO.
```
FORWARD_MODEL ECLIPSE100(...)
--Note: SIM2SUMO must come after ECLIPSE100
FORWARD_MODEL SIM2SUMO
```

## Pre-requisites
SIM2SUMO expects the fmu-config file to be located at `fmuconfig/output/global_variables.yml`.

If the config file is located elsewhere:
```
FORWARD_MODEL ECLIPSE100(...)
--Note: SIM2SUMO must come after ECLIPSE100
FORWARD_MODEL SIM2SUMO(<S2S_CONFIG_PATH>=path/to/config/file)
```
