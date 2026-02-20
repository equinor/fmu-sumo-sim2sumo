For developers
#################

**sim2sumo** couples together three packages often used in the FMU sphere.

* **fmu-dataio** (`docs <https://equinor.github.io/fmu-dataio/>`__) the FMU
  package for exporting data, with rich metadata, out of FMU workflows
* **res2df** (`docs <https://equinor.github.io/res2df/>`__), package used for
  extracting results from reservoir simulator runs. Not strictly tied down to FMU,
  but often used in this domain
*  **fmu.sumo.uploader** (`repo <https://github.com/equinor/fmu-sumo>`__),
   package that uploads files exported to disc with metadata to Sumo

The package makes available export of all datatypes that you can export with
``res2df``, and uploads these results to Sumo. It is part of ``komodo`` and can
be accessed from the command line with ``sim2sumo``, or from ``ert`` with the
`SIM2SUMO ERT forward model
<https://fmu-docs.equinor.com/docs/ert/reference/configuration/forward_model.html#SIM2SUMO>`__.
In its simplest form it needs no extra configuration settings, but comes
pre-configured to extract certain datatypes. See :ref:`the supported datatypes
section <supported_datatypes>`.

Using sim2sumo in scripts
*********************************

Exporting data from eclipse with metadata
===========================================
| This code exports summary data results from an eclipse simulation run.
| Will export to the "prod" environment of Sumo.

.. code-block::

   from fmu.sumo.utilities.sim2sumo as s2s


   DATAFILE = "eclipse/model/2_REEK-0.DATA"
   CONFIG_PATH = "fmuconfig/output/global_variables.yml"
   SUBMODULE = "summary"
   s2s.upload_with_config(CONFIG_PATH, DATAFILE, SUBMODULE, "prod")

The intention of sim2sumo is to be run as an ert FORWARD_MODEL.

The following environment variables need to be set if sim2sumo is not run as part of an ert FORWARD_MODEL:

   * _ERT_EXPERIMENT_ID
   * _ERT_RUNPATH
   * _ERT_SIMULATION_MODE
