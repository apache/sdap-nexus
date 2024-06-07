.. _collections:

***********************
Collection Config Guide
***********************

Introduction
============

The Collection Config is a configuration file that defines collections to be ingested and maintained in SDAP. Currently,
it supports defining collections of NetCDF data that will be processed into the custom NEXUS protobuf tile format or gridded
Zarr data which can be used by SDAP directly with no need for processing. SDAP Ingester currently supports source data stored
in AWS S3 or on the local filesystem (currently, however, not both at the same time).

This guide will explain how to set up both protobuf and Zarr collections.

.. _collections-basics:

Basic Structure
===============

The Collection Config is a YAML file containing a single list named ``collections``:

.. code-block:: yaml

  collections: []

The items in this list are the collections defined and they have the basic structure:

.. code-block:: yaml

  - id: <single variable collection name>
    path: <root collection location. Local path or S3 URI>
    priority: <queue priority>
    projection: <Grid | Swath>
    dimensionNames:
      latitude: <name of the latitude coordinate in the data>
      longitude: <name of the longitude coordinate in the data>
      time: <name of the time coordinate in the data>
      variable: <variable name>
  - id: <multi variable collection name>
    path: <root collection location. Local path or S3 URI>
    priority: <queue priority>
    projection: <GridMulti | SwathMulti>
    dimensionNames:
      latitude: <name of the latitude coordinate in the data>
      longitude: <name of the longitude coordinate in the data>
      time: <name of the time coordinate in the data>
      variables:
      - <variable name 1>
      - <variable name 2>
      - <variable name 3>

There are slight variations and additions to this structure depending on the type of collection, which will be covered below.

.. _collections-nc:

NetCDF - Protobuf Collections
=============================

For NetCDF data, you'll also need to tell the Ingester how big you want to make the tiles. This is set with the ``slices``
object, which is a dictionary mapping dimension names to slice lengths. Omitted dimensions are assumed to be 1. It is important
to set tile sizes that are not too big as to result in excess unnecessary data transfer, but also not too small as to result in
an explosion in the number of generated tiles, which will lead to excessive metadata storage overhead and possible performance
degradations. For gridded data, we recommend tile sizes between 30 x 30 and 100 x 100, we also strongly recommend swath tiles be 
sized no larger than 15 x 15, as the current method for handling swath data is very memory inefficient scaled rapidly by tile size.

.. note:: The source dataset dimension names are used in slice definitions, not the coordinate names as in the ``dimensionNames`` object. In gridded datasets, these names are often the same, but this is not the case for swath data.

Example:

.. code-block:: yaml

  collections:
  - id: MUR25-JPL-L4-GLOB-v04.2
    path: s3://mur-sst/zarr-v1/
    priority: 1
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: analysed_sst
    slices:
      lat: 100
      lon: 100
      time: 1
  - id: ASCATB-L2-Coastal
    path: s3://example-bucket/swath-path/
    priority: 1
    projection: SwathMulti
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variables:
      - wind_speed
      - wind_dir
    slices:
      NUMROWS: 15
      NUMROWS: 15


.. _collections-zarr:

Zarr Collections
================

To specify a collection as a Zarr collection, simply add ``storeType: zarr`` to the collection object. If the data is local,
this is all you need to do.

.. code-block:: yaml

  id: <collection name>
  path: <root collection location. Local path>
  priority: <queue priority>
  projection: <Grid | GridMulti>
  storeType: zarr
  dimensionNames:
    latitude: <name of the latitude coordinate in the data>
    longitude: <name of the longitude coordinate in the data>
    time: <name of the time coordinate in the data>
    variable: <variable name>

For data in S3, you need to provide information on how to access the data. This is currently done with the ``config.aws`` object.

You will need to provide credentials to access the bucket, or specify if it is public:

Example:

.. code-block:: yaml

  collections:
  - id: MUR_SST
    path: s3://mur-sst/zarr-v1/
    priority: 1
    projection: Grid
    storeType: zarr
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: analysed_sst
    config:
      aws:
        public: true
  - id: private_data
    path: s3://example-bucket/zarr/path/
    priority: 1
    projection: GridMulti
    storeType: zarr
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variables:
      - var1
      - var2
      - var3
    config:
      aws:
        accessKeyID: <secret>
        secretAccessKey: <secret>
        public: false
