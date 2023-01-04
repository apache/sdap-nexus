.. _build:

*****************
How to Build the SDAP Docker Images
*****************

In this guide, we will go over how to build the docker images for the various components of SDAP.

Prepare
===========

First, we must ensure we have Docker installed and running. For this guide, we used Docker Desktop version 4.12.0. Download Docker Desktop `here. <https://www.docker.com/products/docker-desktop/>`_

Now we must download and extract the source code for NEXUS and the ingester.

.. code-block:: bash

  export NEXUS_DIR=~/sdap-build/nexus
  export INGESTER_DIR=~/sdap-build/ingester

  mkdir -p ${NEXUS_DIR}
  mkdir -p ${INGESTER_DIR}

We should also set variables for a consistent tag across all images. These should be consistent with the release versions we plan to build.

.. code-block:: bash

  export NEXUS_VERSION=1.0.0
  export INGESTER_VERSION=1.0.0

To build the necessary images, we will need to first download the latest releases for SDAP NEXUS and SDAP Ingester ((URL)). We will not need to download the SDAP Nexusproto release directly.

Now we must extract the releases to their respective directories.

Create a temporary directory to extract to.

.. code-block:: bash

  export TMP_DIR=/tmp/SDAP-extract
  mkdir -p ${TMP_DIR}

.. note::

  In the following code block, make sure the versions in the filenames correspond with the versions that were downloaded.

.. code-block:: bash

  tar xvf apache-sdap-ingester-${INGESTER_VERSION}-incubating-src.tar.gz -C ${TMP_DIR}
  tar xvf apache-sdap-nexus-${NEXUS_VERSION}-incubating-src.tar.gz -C ${TMP_DIR}

  mv ${TMP_DIR}/Apache-SDAP/apache-sdap-ingester-${INGESTER_VERSION}-incubating-src/* ${INGESTER_DIR}
  mv ${TMP_DIR}/Apache-SDAP/apache-sdap-nexus-${NEXUS_VERSION}-incubating-src/* ${NEXUS_DIR}

Build Ingester Components
=========================

There are two components to build: the Collection Manager & Granule Ingester.

For both of these, we must first move to the ingester root directory.

.. code-block:: bash

  cd ${INGESTER_DIR}

Build the Collection Manager
-------

From the ingester root directory, run:

.. code-block:: bash

  docker build . -f collection_manager/docker/Dockerfile -t sdap-local/sdap-collection-manager:${INGESTER_VERSION}

Build the Granule Ingester
-------

From the ingester root directory, run:

.. code-block:: bash

  docker build . -f granule_ingester/docker/Dockerfile -t sdap-local/sdap-granule-ingester:${INGESTER_VERSION}

Build the Solr & Webapp Components
======

For the remaining three components, we must now move to the nexus root directory.

.. code-block:: bash

  cd ${NEXUS_DIR}

Build the Solr Image
-------

First we must move to the Solr Docker directory.

.. code-block:: bash

  cd docker/solr

Now to build the image:

.. code-block:: bash

  docker build . -t sdap-local/sdap-solr-cloud:${NEXUS_VERSION}

Build the Solr Initialization Image
-------

Now to build the image:

.. code-block:: bash

  docker build . -t sdap-local/sdap-solr-cloud-init:${NEXUS_VERSION} -f cloud-init/Dockerfile

Build the Webapp Image
---------

For the final image, we must return to the NEXUS root directory.

.. code-block:: bash

  cd ${NEXUS_DIR}

Now we can build the webapp with:

.. code-block:: bash

  docker build . -f docker/nexus-webapp/Dockerfile -t sdap-local/sdap-nexus-webapp:${NEXUS_VERSION}

Verify Successful Build
====

To verify build success, follow the :ref:`quickstart guide<quickstart>`.


Finished!
=====

Congratulations! You have successfully built the images required for running NEXUS.

If you used your own repository for the image tags, you can push them using ``docker push``.

