3D grid properties
#################

When including the "grid" datatype, sim2sumo will upload static and dynamic 3D properties.

Static grid properties
=========================
Most static grid properties are uploaded by default if the grid datatype is requested.
The following properties **will not be uploaded**: ENDNUM, DX, DY, DZ, TOPS.

Dynamic grid properties
=========================
For the dynamic properties, the following properties **will be uploaded by default**: SWAT, SGAS, SOIL, PRESSURE.

Data for all timesteps are uploaded. There is currently no configuration for which dates to upload.

Configuration
=========================
It is likely you will want to include grid properties as well as the :ref:`default datatypes <default-data-types>`.
In this case, you can define several datatypes or simply "all":

.. code-block:: yaml

    # Example config to upload summary, RFT and default grid properties
    sim2sumo:
        datatypes:
        - summary
        - rft
        - grid

.. code-block:: yaml

    # Example config to upload all supported datatypes, including default grid properties
    sim2sumo:
        datatypes:
        - all

To include just grid properties:

.. code-block:: yaml

    # Example config to upload just default grid properties
    sim2sumo:
        datatypes:
        - grid

Restart properties export configuration
----------------------
The ``rstprops`` argument can be used to specify which restart properties to
export. If ``rstprops`` isn't defined, the default restart properties will be
exported (SWAT, SGAS, SOIL, PRESSURE).

To include all restart properties, use a list with a single item "all":

.. code-block:: yaml

    sim2sumo:
        datatypes:
        - grid
        rstprops:
        - all

To include specific restart properties, use a list with single items. The following is equivalent to exporting the default restart properties, plus the "RS" property

.. code-block:: yaml

    sim2sumo:
        datatypes:
        - grid
        rstprops:
        - SWAT
        - SGAS
        - SOIL
        - PRESSURE
        - RS