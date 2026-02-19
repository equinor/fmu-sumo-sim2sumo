.. fmu-sumo-sim2sumo documentation master file, created by
   sphinx-quickstart on Wed Jan 31 10:50:22 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Getting started
#################
``fmu.sumo.sim2sumo`` is a python package for uploading reservoir simulation model data to Sumo.

Quick start
============================

.. NOTE these ERT docs are Equinor internal only

A prerequisite for using sim2sumo is that your FMU model is already connected to
Sumo. See `this guide in the Sumo docs
<https://fmu-docs.equinor.com/docs/sumo/guides>`__.

The simplest way to run sim2sumo is to add the `SIM2SUMO ERT forward model
<https://fmu-docs.equinor.com/docs/ert/reference/configuration/forward_model.html#SIM2SUMO>`__ to your ERT config file.
For example:

.. code-block::

   FORWARD_MODEL FLOW(...)        -- Run reservoir simulator (e.g. FLOW, ECLIPSE100, ECLIPSE300)
   FORWARD_MODEL SIM2SUMO         -- Run SIM2SUMO after your simulation run


.. _supported_datatypes:
Data which sim2sumo supports
============================

sim2sumo can extract and upload the following data to Sumo. For more information on these datatypes,
see the `res2df docs <https://equinor.github.io/res2df/introduction.html>`__.

.. _default-data-types:

Default data types
-------------------
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
*******************

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
----------------
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
----------------
To upload both "static" (INIT) and dynamic "restart" (UNRST) 3D grid properties, additional configuration is required.

Examples can be found :doc:`here <grid>`.

Paths to files
----------------

By default, SIM2SUMO looks for all DATA files in the runpath `<user>/<case>/realization-*/<ensemble>`. 
It is possible to configure where results will be extracted from in two ways:

.. TODO add restart example

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

.. toctree::
   :maxdepth: 2
   :caption: Contents

   self
   grid
   developers
   api