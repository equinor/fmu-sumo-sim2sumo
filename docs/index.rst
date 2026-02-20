.. fmu-sumo-sim2sumo documentation master file, created by
   sphinx-quickstart on Wed Jan 31 10:50:22 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Getting started
#################
``fmu.sumo.sim2sumo`` is a python package for uploading reservoir simulation model data to Sumo.

``sim2sumo`` extracts data from the simulation model using `res2df
<https://equinor.github.io/res2df/>`__, attaches rich metadata using
`fmu-dataio <https://fmu-dataio.readthedocs.io/en/latest/>`__, and uploads both to Sumo. 
The result: your simulation results can be located and used by various applications, without 
you having to extensively configure and maintain your own workflows.

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


.. toctree::
   :maxdepth: 2
   :caption: Contents

   self
   config
   grid
   developers
   api