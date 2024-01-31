fmu.sumo.sim2sumo
#############

``fmu.sumo.sim2sumo`` is a python package for uploading results to Sumo.
The package facilitates upload of results from reservoir simulators such as **eclipse**, **IX**, and **OPM flow** as arrow files.

Short introduction
--------
.. note::

  **sim2sumo** couples together three packages often used in the FMU sphere.


  * **fmu-dataio** (`docs <https://equinor.github.io/fmu-dataio/>`_) the FMU package for exporting data, with rich metadata, out of FMU workflows.
  * **res2df** (`docs <https://equinor.github.io/res2df/>`_), a package used for extracting results from flow simulator runs, not strictly tied down to FMU, but often used in this domain
  *  **fmu.sumo.uploader** (`repo <https://github.com/equinor/fmu-sumo>`_), a package that uploads files exported to disc with metadata to sumo


The package makes available export of all datatypes that you can export with ``res2df``, and uploads these results to Sumo. It is part of ``komodo`` and can be accessed from the command line with ``sim2sumo``, or from
``ert`` with the forward model **SIM2SUMO**. In it't simplest form it needs no extra configuration setting, but comes pre-configured to extract datatypes:


* summary
* rft
* wellcompletions


To run sim2sumo with ert it needs to be inserted in your *ert config file* after the simulator run, which is typically **eclipse** as of 2024.


.. note::

   Sim2sumo has the same requirements as fmu-sumo-uploader, and therefore needs some sections defined in the file fmu config file (aka global variables file)
   For the simplest implementation this file needs to be stored at ``../fmuconfig/output/global_variables.yml``, for other name/location see
   section :ref:`Config settings <target config settings>`


Api Reference
-------------

- `API reference <apiref/fmu.sumo.sim2sumo.html>`_


Usage and examples
------------------

Config settings
------------------------------

sim2sumo is set up such that you provide a config file with the section sim2sumo defined.
The config file needs to be in yaml format. You can add this to the global_variables for the case,
or make your own file. There are two sections that are relevant when using sim2sumo:
1. The metadata needed for the upload to sumo, that is the three sections model, masterdata, and accesscd ..
2. A section named sim2sumo. There are several ways to define this section sim2sumo.

Simplest case
^^^^^^^^^^^^^^
This is a snippet of the ``global_variables.yml`` containing enough data to upload to sumo with sum2sumo.
 In real cases this file will be much longer. When the entire section for sum2sumo is equal to ''sim2sumo: true''
 sim2sumo will extract from all simulation runs in a folder called eclipse/model/ relative to where you are running from,
 and at the same time export all datatypes available. See the example file below.

.. toggle::

   .. literalinclude:: ../tests/data/reek/realization-0/iter-0/fmuconfig/output/global_variables.yml
      :language: yaml

|
Case where eclipse datafile is explicitly defined
^^^^^^^^^^^^^^
This is a snippet of the ``global_variables.yml`` file which holds the static metadata described in the
`previous section <./preparations.html>`__. In real cases this file will be much longer.

.. toggle::

   .. literalinclude:: ../tests/data/reek/realization-0/iter-0/fmuconfig/output/global_variables_w_eclpath.yml
      :language: yaml

|
Case where eclipse datafile, what types to export, and options to use are explicitly defind
^^^^^^^^^^^^^^
This is a snippet of the ``global_variables.yml`` file which holds the static metadata described in the
`previous section <./preparations.html>`__. In real cases this file will be much longer.

.. toggle::

   .. literalinclude:: ../tests/data/reek/realization-0/iter-0/fmuconfig/output/global_variables_w_eclpath_and_extras.yml
      :language: yaml

|

Exporting data from eclipse with metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This code exports summary data results from simulation
.. code-block::

    from fmu.sumo.utilities.sim2sumo as s2s

    DATAFILE = "eclipse/model/2_REEK-0.DATA"
    CONFIG_PATH = "fmuconfig/output/global_variables.yml"
    SUBMODULE = "summary"
    s2s.export_csv(DATAFILE, SUBMODULE, CONFIG_PATH)

As a FORWARD_MODEL in ERT
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    FORWARD_MODEL SIM2SUMO


Example above uploads all surfaces dumped to ``share/results/maps``. You don't need to have more
than one instance of this job, it will generate and upload the data specified in the corresponding
config file.

.. note::



.. note::





