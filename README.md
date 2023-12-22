# fmu-sumo-sim2sumo
Makes reservoir simulator (Eclipse, [OPM](https://opm-project.org/)) results available through Sumo.

Uploads files from reservoir simulators to azure assisted by Sumo. This is done in a three step process.

1. Data is extracted in arrow format using [res2df](https://github.com/equinor/res2df).
2. Corresponding metadata is generated via [fmu-dataio](https://github.com/equinor/fmu-dataio).
3. Data and metadata is then uploaded to Sumo using [fmu-sumo-uploader](https://github.com/equinor/fmu-sumo-uploader).

The entire process is triggered by an ERT forward model which is available as SIM2SUMO.
```
FORWARD_MODEL ECLIPSE100(...)
--Note: SIM2SUMO must come after ECLIPSE100
FORWARD_MODEL SIM2SUMO
```

## Contributing
Want to contribute? Read our [contributing](./CONTRIBUTING.md) guidelines

## Pre-requisites
SIM2SUMO expects the fmu-config file to be located at `fmuconfig/output/global_variables.yml`.

If the config file is located elsewhere:
```
FORWARD_MODEL ECLIPSE100(...)
--Note: SIM2SUMO must come after ECLIPSE100
FORWARD_MODEL SIM2SUMO(<S2S_CONFIG_PATH>=path/to/config/file)
```


## Testing on top of Komodo
Sim2sumo and [sumo uploader](https://github.com/equinor/fmu-sumo-uploader) are both installed under `fmu/sumo/`.
This means that the uploader must also be installed to test a new version of sim2sumo on top of Komodo.

Example: Installing sim2sumo from `mybranch` on top of Komodo bleeding
```
< Create a new komodo env from komodo bleeding >
< Activate the new env >

pip install git+https://github.com/equinor/fmu-sumo-sim2sumo.git@mybranch
pip install git+https://github.com/equinor/fmu-sumo-uploader.git
```

The [Explorer](https://github.com/equinor/fmu-sumo) is also installed under `fmu/sumo`. Meaning that if the testing scenario includes the Explorer then it should also be installed on top of Komodo.
```
pip install git+https://github.com/equinor/fmu-sumo.git
```
