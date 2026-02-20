Functionality and configuration 
##################

.. _supported_datatypes:
Data which sim2sumo supports
******************************

sim2sumo can extract and upload the following data to Sumo. For more information on these datatypes,
see the `res2df docs <https://equinor.github.io/res2df/introduction.html>`__.

.. _default-data-types:

Default data types
===================
.. note::
   These data are uploaded by default, with no additional configuration required:

   * summary
   * rft
   * satfunc (relperm curves)
   * gruptree (production network)
   * wellcompletiondata 
   .. (# TODO: layer table requirement?)


.. warning::

   The following datatypes need additional configuration:

   * grid (INIT "initial" and restart "dynamic" 3D grid properties)
   * pvt (PVT tables)
   * tran (transmissibilities)
   * vfp (lift curves; one table for each lift curve)

   See `Configuration of sim2sumo`_.

To upload other data to Sumo, see the `SUMO_UPLOAD forward model
<https://fmu-docs.equinor.com/docs/ert/reference/configuration/forward_model.html#SIM2SUMO>`__. 

Configuration of sim2sumo
******************************

sim2sumo can be configured in your FMU model's `global_variables.yml` file. This file is generated using `fmu-config`. 
If you are not familiar with updating global variables, see the `fmu-config docs <https://equinor.github.io/fmu-config/>`__.

The simplest way to configure sim2sumo is to add a `sim2sumo` key to the `global_variables_master.yml` file.

.. code-block:: yaml
   [...]

   (rest of global variables master file)

   #===================================================================================
   # sim2sumo config settings
   #===================================================================================

   sim2sumo:
      datafile:
         - ...
      datatypes:
         - ...
      rstprops:
         - ...

The SIM2SUMO forward model looks for sim2sumo config in `../../fmuconfig/output/global_variables.yml` by default. 
If your config file isn't located here, you need to tell SIM2SUMO where to find it:

.. code-block::

   FORWARD_MODEL SIM2SUMO(<S2S_CONFIG_PATH>=path/to/config/file.yml)


Data types
=============================
To specify which data to upload, provide the datatypes as a list

.. code-block:: yaml

   sim2sumo:
      datatypes:
      - summary
      - rft
      - gruptree
      - ...

To include all datatypes use a list with a single item "all". This will also include the *grid* datatype:

.. code-block:: yaml

   sim2sumo:
      datatypes:
         - all

3D grid properties
=============================
To upload both "static" (INIT) and dynamic "restart" (UNRST) 3D grid properties, additional configuration is required.

Examples can be found :doc:`here <grid>`.

Paths to files
=============================

By default, SIM2SUMO looks for all DATA files in the runpath ``<user>/<case>/realization-*/<ensemble>``. 
It is possible to configure where results will be extracted from in two ways:

1. Specify file paths, or file stubs (without an extension):

.. code-block:: yaml

   sim2sumo:
      datafile:
         - eclipse/model/DROGON
         - eclipse/model/DROGON-0.DATA


2. Specify folder paths:

.. code-block:: yaml

   sim2sumo:
      datafile:
         - eclipse/model/


Running several DATA files in one runpath, or running a prediction case from a restart?
------------------------------

.. warning::

    Data consumers expect to see one of each datatype for each realisation, e.g.
    one summary file for each realisation. If you run several simulations in the
    same runpath (``<user>/<case>/realization-*/<ensemble>``) or if you symlink
    to a restart file when running a prediction ensemble, **you should specify the
    path of the datafiles to upload**. If you don't do this, you may find your
    results are not displayed or are displayed incorrectly in applications
    reading data from Sumo.

    Example: a prediction run is found in
    ``realization-*/pred/eclipse/model`` and a symlinked restart file is located
    in ``realization-*/pred/restart/model``. The symlinked restart file can be
    omitted by specifying just the folder path of the prediction run:

    .. code-block:: yaml

        sim2sumo:
            datafile:
                - eclipse/model/

            