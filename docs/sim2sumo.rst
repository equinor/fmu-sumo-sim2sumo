Sumo Utilities
#############

The ``fmu.sumo.utilities`` is a python package for integrating other tools into the FMU-SUMO ecosystem.
So far the only utility available is the utility sim2sumo which facilitates upload of results from
reservoir simulators such as eclipse and opm flow as csv or arrow format files.

sim2sumo
--------
.. note::

  **sim2sumo** couples together three packages often used in the FMU sphere.
  * **fmu-dataio** the FMU plugin for exporting data out of FMU workflows with rich metadata.
  * **ecl2df**, a plugin not strictly tied down to FMU, but often used in this domain
  * **fmu.sumo.uploader**, a plugin that uploads files exported to disc with metadata to sumo

- User has necessary accesses

.. note::
Api Reference
-------------

- `API reference <apiref/fmu.sumo.utilities.html>`_


Usage and examples
------------------

Config settings
------------------------------

sim2sumo is set up such that you provide a config file with the section sim2sumo defined.
The config file needs to be in yaml format. You can add this to the global_variables for the case,
or make your own file. The file needs to contain two parts:
1. The metadata needed for the upload to sumo, that is the three sections model, masterdata, and access
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





