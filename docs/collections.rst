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

You will need to provide credentials to access the bucket, or specify if it is public.

There are 5 ways to configure access to data in S3: public access, fixed credentials, profile credentials, host credentials, and
EDL credentials.

1. Public Access: The zarr data is in a bucket that is configured to allow unauthenticated data access from where the SDAP application is deployed and thus do not bother with signing any requests
2. Fixed Credentials: The simplest but least recommended option. Provide the AccessKeyId and SecretAccessKey for an IAM identity with sufficient S3 access permissions
3. Profile Credentials: Use the credentials for a given profile defined in an `AWS credentials file <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-using-profiles>`_ which can exist on the local filesystem or be mounted in via a Kubernetes Secret or ConfigMap. This would be the preferred method for running SDAP on local/on-premises hardware
4. Host Credentials: For running on AWS only: use EC2 instance, EKS node or associated EKS pod identifies. Preferred method for running on EKS
5. EDL Credentials: **ONLY WORKS WHEN RUNNING IN AWS US-WEST-2 REGION** For data in NASA Earthdata DAACs, use temporary credentials furnished by the DAACs' ``/s3Credentials`` endpoints. Only some DAACs are supported. This feature is still in beta.

Note for EDL credentials, a valid `Earthdata Login <https://urs.earthdata.nasa.gov/>`_ (username & password) must be provided.

Example:

.. code-block:: yaml

  collections:
  - id: MUR_SST  # Config for Public Access
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
  - id: private_data  # Config for Public Access
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
        region: us-west-2
        public: false
        creds:
          accessKeyID: <secret>
          secretAccessKey: <secret>
  - id: private_data  # Config for Profile Credentials
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
        region: us-west-2
        public: false
        profile: <profile name>
  - id: private_data  # Config for Host Credentials
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
        region: us-west-2
        public: false
  - id: private_data  # Config for EDL Credentials
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
      earthdata:
        edl_username: <username>  # Can be omitted here in favor of setting the EDL_USERNAME environment variable
        edl_password: <password>  # Can be omitted here in favor of setting the EDL_PASSWORD environment variable
        daac: podaac
  - id: private_data  # Alternate config for EDL Credentials
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
      earthdata:
        edl_username: <username>  # Can be omitted here in favor of setting the EDL_USERNAME environment variable
        edl_password: <password>  # Can be omitted here in favor of setting the EDL_PASSWORD environment variable
        endpoint: </s3Credentials URL>

The ``config.aws`` and ``config.earthdata`` schemas are the same when using the ``datasets/add`` and ``datasets/update`` endpoints.

Example:

.. code-block::

    POST /nexus/datasets/add?name=<dataset name>&path=<s3 URL>&= HTTP/1.1
    Content-Type: application/yaml
    Host: <SDAP Hostname>
    Content-Length: <Length>

    variable: <Var name>
    coords:
      latitude: latitude
      longitude: longitude
      time: time
    aws:
      public: false
      region: us-west-2
      creds:
        accessKeyID: <secret>
        secretAccessKey: <secret>

The current list of supported DAACs and their credentials endpoints for the ``config.earthdata`` configuration are as follows:

 =================== ==============================================================
  DAAC                URL
 =================== ==============================================================
  podaac              https://archive.podaac.earthdata.nasa.gov/s3credentials
  podaac-swot         https://archive.swot.podaac.earthdata.nasa.gov/s3credentials
  gesdisc             https://data.gesdisc.earthdatacloud.nasa.gov/s3credentials
  lpdaac              https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials
  obdaac              https://obdaac-tea.earthdatacloud.nasa.gov/s3credentials
  nsidc               https://data.nsidc.earthdatacloud.nasa.gov/s3credentials
  laads               https://data.laadsdaac.earthdatacloud.nasa.gov/s3credentials
  asfdaac             https://cumulus.asf.alaska.edu/s3credentials
  asfdaac-sentinel1   https://sentinel1.asf.alaska.edu/s3credentials
 =================== ==============================================================


