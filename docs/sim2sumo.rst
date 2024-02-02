fmu.sumo.sim2sumo
#################

Short introduction
*******************
.. note::

  **sim2sumo** couples together three packages often used in the FMU sphere.


  * **fmu-dataio** (`docs <https://equinor.github.io/fmu-dataio/>`__) the FMU package for exporting data, with rich metadata, out of FMU workflows.
  * **res2df** (`docs <https://equinor.github.io/res2df/>`__), a package used for extracting results from flow simulator runs, not strictly tied down to FMU, but often used in this domain
  *  **fmu.sumo.uploader** (`repo <https://github.com/equinor/fmu-sumo>`__), a package that uploads files exported to disc with metadata to sumo


The package makes available export of all datatypes that you can export with ``res2df``, and uploads these results to Sumo. It is part of ``komodo`` and can be accessed from the command line with ``sim2sumo``, or from
``ert`` with the forward model **SIM2SUMO**. In it't simplest form it needs no extra configuration setting, but comes pre-configured to extract datatypes:


* summary
* rft
* wellcompletions


To run sim2sumo with ert it needs to be inserted in your *ert config file* after the simulator run, which is typically **eclipse** as of 2024.


.. note::

   Sim2sumo as fmu-sumo-uploader uses the fmu-config file (aka global variables) to pick up required metadata. For more on this topic see :ref:`Config settings <target config settings>`

.. _target preconditions:

Preconditions
***************
There are some requirements that need to be in place for sim2sumo to run

* Sim2sumo needs to be run in from a location where it can find a case metadata object stored on disk.
* The case metadata object must have been uploaded to Sumo, and in the same environment that you are intending to upload to

Api Reference
***************

- `API reference <apiref/fmu.sumo.sim2sumo.html>`_


Usage and examples
********************
Simplest case: As a FORWARD_MODEL in ERT
=========================================

.. code-block::


    FORWARD_MODEL ECLIPSE(...)
    -- Note: SIM2SUMO needs to be run after the reservoir simulation
    FORWARD_MODEL SIM2SUMO

| Example above extracts results to ``share/results/tables``, and uploads those results to Sumo.
| You don't need to have more than one instance of this job, it automatically extracts the default results.
| If you want to include more datatypes add that to the configuration file, see :ref:`Config settings <target config settings>`,
| See also :ref:`preconditions <target preconditions>`.

.. _target config settings:

The config file
*****************

In it's simplest form sim2sumo seems to not need any configuration. This is not strictly true. It is based on finding a config file.
For the simplest implementation this file needs to be stored at ``../fmuconfig/output/global_variables.yml``, relative to where you run sim2sumo.
Sim2sumo requires a configuration file to at the very least include the neccessary master data for the asset where the results are coming from.
There are two parts in the global variables file that are relevant for sim2sumo:

1. The master data needed for the upload to sumo, that is the three sections model, masterdata, and access.
   These are absolutely neccessary, as with all uploads to sumo. More on this topic can be found in the documentation
   for fmu-sumo-uploader
2. A section named sim2sumo. This section is not strictly needed, but needs to exist for custom configuration of sim2sumo, see :ref:`Custom configuration <target custom config>`

Config settings
********************


Config file with non-standard name/location
==============================================
If you for some reason don't have the fmu config file in the standard location or with the standard name, use this in you *ert-config*

.. code-block::


    FORWARD_MODEL ECLIPSE(...)
    -- Note: SIM2SUMO needs to be run after the reservoir simulation
    FORWARD_MODEL SIM2SUMO(<S2S_CONFIG_PATH>=path/to/config/file)

See also :ref:`preconditions <target preconditions>`.

.. _target custom config:

Custom configuration
=====================

The sim2sumo section in the config file gives you full flexibility for extracting anything that ``res2df`` can extract.
You can also change where you extract results from, and even use all the extra custumization options that ``res2df`` has makes available.
The three relevant sections are:

*datafile*:
--------------------
This section is for configuring where you extract results from, meaning where to look for simulation results. This section can configured in several ways:

1. As a path to a file, or file stub (without an extension):

   .. code-block:: yaml

      datafile ../../eclipse/model/DROGON


2. As a path to a folder:

   .. code-block::

      datafile: ../../eclipse/model/


3. As a list:

   .. code-block::

      datafile:
        - ../../eclipse/model
        - ../../ix/model
        ..


datatypes:
----------------
This section is for configuration of what data to extract. This section can be configured in several ways.

1. As list:

   .. code-block::

      datatypes:
        - summary
        - wcon
        - faults
        - ..

2. as string:

   Here there are two options, you can use both the name of one single datatype
   or the all argument for all datatypes:

   .. code-block::

      datatypes: all


options:
-------------
   | This section is for adding extra optional configuration for extracting the different datatypes.
   | This section needs to be in a list format


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

See also :ref:`preconditions <target preconditions>`.

Using sim2sumo from the command line
***************************************

   sim2sumo can be run from any terminal window where komodo is activated. For execution it is mostly not of any use,
   but can be useful to run in help mode

Getting help on sim2sumo from the command line
=================================================

You can get help on sim2sumo from the command line. Here are some examples:


.. code-block::
   :caption: Accessing the help information

   sim2sumo -h

Getting help on sim2sumo available options for a datatype from the command line
================================================================================

.. code-block::
   :caption: Printing the help info from ``res2df`` with sim2sumo from the command line

   sim2sumo help summary








