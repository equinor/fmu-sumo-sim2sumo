fmu.sumo.sim2sumo
#################

Short introduction
*******************
.. note::

  **sim2sumo** couples together three packages often used in the FMU sphere.


  * **fmu-dataio** (`docs <https://equinor.github.io/fmu-dataio/>`__) the FMU package for exporting data, with rich metadata, out of FMU workflows.
  * **res2df** (`docs <https://equinor.github.io/res2df/>`__), package used for extracting results from flow simulator runs, not strictly tied down to FMU, but often used in this domain
  *  **fmu.sumo.uploader** (`repo <https://github.com/equinor/fmu-sumo>`__), package that uploads files exported to disc with metadata to sumo


The package makes available export of all datatypes that you can export with ``res2df``, and uploads these results to Sumo. It is part of ``komodo`` and can be accessed from the command line with ``sim2sumo``, or from
``ert`` with the forward model **SIM2SUMO**. In its simplest form it needs no extra configuration settings, but comes pre-configured to extract datatypes:


* summary
* rft
* satfunc
* gruptree
* wellcompletiondata

| To run sim2sumo with ERT it needs to be inserted in your *ERT config file* after the reservoir simulator run.
| See :ref:`examples`.


.. _preconditions:

Preconditions
***************

* Sim2sumo needs to run within an FMU workflow which is enabled for using Sumo.

.. important:: **First time uploading to Sumo?**

   There are some requirements that need to be in place for sim2sumo to run, these are the same preparations that are required for any upload to Sumo.
   Refer to the `Getting Started <https://doc-sumo-doc-dev.radix.equinor.com/guides/>`_ section in the Sumo documentation.


.. _config file:

The config file
*****************

In it's simplest form `sim2sumo` does not need any configuration. 

However, custom configuration of `sim2sumo` can be done through `global_variables.yml`.

.. important::

   If you need to include custom configuration for `sim2sumo`, you are encouraged to add the "sim2sumo" section to a *_sim2sumo.yml* file and link this into the file *global_master.yml* config file before
   you generate the *global_variables.yml* and *global_variables.yml.tmpl* files.

   Example:
    .. code-block:: yaml

      [...]

      (rest of global_variables master file)

      #===================================================================================
      # sim2sumo config settings
      #===================================================================================

      sim2sumo: !include _sim2sumo.yml


.. _examples:

Usage and examples
********************

Simplest case
-----------------

.. code-block::


    FORWARD_MODEL ECLIPSE(...)
    -- Note: sim2sumo needs to be run after the reservoir simulation
    FORWARD_MODEL SIM2SUMO



Config file with non-standard name/location
---------------------------------------------
If you for some reason don't have the fmu config file in the standard location or with the standard name, use this in you *ert config file*

.. code-block::


    FORWARD_MODEL ECLIPSE(...)
    -- Note: sim2sumo needs to be run after the reservoir simulation
    FORWARD_MODEL SIM2SUMO(<S2S_CONFIG_PATH>=path/to/config/file)

.. _config settings:

Config settings
********************


Example of config settings for sim2sumo
============================================


.. literalinclude:: ../tests/data/reek/realization-0/iter-0/fmuconfig/output/global_variables_w_eclpath_and_extras.yml
   :language: yaml

|

.. _custom config:

Custom configuration
=====================

The "sim2sumo" section in the config file gives you full flexibility for extracting anything that ``res2df`` can extract.
You can also change where you extract results from, and use some of the extra customization options that ``res2df`` has available.
The three relevant sections are:

datafile
--------------------
This section is for configuring where you extract results from, meaning where to look for simulation results.
This section should be a list where each item is either a file path, file stub or a folder path.

1. File paths, or file stubs (without an extension):

   .. code-block::

      datafile:
         - eclipse/model/DROGON
         - eclipse/model/DROGON-0.DATA


2. Folder paths:

   .. code-block::

      datafile:
         - eclipse/model/


You can also specify what datatypes should be extracted for each file, by adding a list of datatypes to each file path:

   .. code-block::

      datafile:
         - eclipse/model/DROGON:
            - summary
            - rft
            - gruptree
         - eclipse/model/DROGON-0.DATA:
            - summary
            - rft
            - gruptree


datatypes
----------------
This section is for configuration of what data to extract. It should be specified as a list

   .. code-block::

      datatypes:
        - summary
        - rft
        - gruptree
        - ..

To include all datatypes use a list with a single item "all":

   .. code-block::

      datatypes:
         - all

The available datatypes are:
   * summary
   * rft
   * satfunc
   * gruptree
   * wellcompletiondata
   * grid

For more information on these datatypes available, see the documentation for ``res2df``

.. options:
.. -------------
..    | This section is for adding extra optional configuration for extracting the different datatypes.
..    | This section needs to be in a list format.

grid datatype
~~~~~~~~~~~~~~~~
When including the "grid" datatype, sim2sumo will try to upload 3D grid data and properties for all datafiles specified in the ``datafile`` section.
For the init file the following properties will be *ignored*: ENDNUM, DX, DY, DZ, TOPS.
For the restart the following properties will be *exported by default*: SWAT, SGAS, SOIL, PRESSURE.

Restart properties export configuration
^^^^^^^^^^^^^^^^
The ``rstprops`` argument can be used to specify which restart properties to export. If ``rstprops`` isn't defined, the default restart properties will be exported (SWAT, SGAS, SOIL, PRESSURE).

To include all restart properties, use a list with a single item "all":

   .. code-block::

      sim2sumo:
         datatypes:
            - grid
         rstprops:
            - all

To include specific restart properties, use a list with single items. The following is equivalent to exporting the default restart properties, plus the "RS" property

   .. code-block::

      sim2sumo:
         datatypes:
            - grid
         rstprops:
            - SWAT
            - SGAS
            - SOIL
            - PRESSURE
            - RS

Overriding default datatypes
----------------
``datatypes`` applies to all datafiles specified in the ``datafile`` section.
It is possible to override this configuration for individual files.
The example shows how to only extract summary data from the first file, and all default ``datatypes`` from the second file.

   .. code-block::

      datafile:
         - eclipse/model/DROGON-0.DATA:
            - summary
         - eclipse/model/DROGON-1.DATA
      datatypes:
         - summary
         - rft
         - gruptree


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

See also :ref:`preconditions`.

Using sim2sumo from the command line
***************************************

Using sim2sumo from the command line is discouraged, and there is no guarantee that it will work.

The intension of sim2sumo is to be run as an ert FORWARD_MODEL.

The following environment variables need to be set if sim2sumo is not run as part of an ert FORWARD_MODEL:

   * _ERT_EXPERIMENT_ID
   * _ERT_RUNPATH
   * _ERT_SIMULATION_MODE

Execution of sim2sumo from command line
========================================

Extracting the default datatypes with sim2sumo
-------------------------------------------------

.. code-block::
   :caption: Accessing the help information

   sum2sumo execute --config_path fmuconfig/output/global_variables.yml --env dev

See also :ref:`preconditions`.


Getting help on sim2sumo from the command line
=================================================

You can get help on sim2sumo from the command line. Here are some examples:


.. code-block::
   :caption: Accessing the general help information

   sim2sumo -h

Accessing help from ``res2df`` via sim2sumo
==============================================
| There is extensive information on extraction of individual datatypes with ``res2df``.
| This information can be accessed from sim2sumo.

.. code-block::
   :caption: Getting help on summary data from ``res2df`` with sim2sumo from the command line

   sim2sumo help summary



VFP export
***************************************
**Note** that Sim2sumo exports VFP tables as separate files. (Similar to using ``--arrow`` with ``res2df``)

VFP data can be exported either for all datafiles by adding "vfp" to the datatypes list:

   .. code-block::

      datatypes:
        - ..
        - vfp

Or for individual files by adding "vfp" to the list of datatypes for that file:

.. code-block::

      datafile:
         - eclipse/model/DROGON-0.DATA:
            - ..
            - vfp

