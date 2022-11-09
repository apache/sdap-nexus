.. _build:

*****************
How to Build the NEXUS Docker Images
*****************

In this guide, we will go over how to build the docker images for the various components of NEXUS.

Prepare
===========

First, we must ensure we have Docker installed and running. For this guide, we used Docker Desktop version 4.12.0. Download Docker Desktop `here. <https://www.docker.com/products/docker-desktop/>`_

Now we must download and extract the source code for NEXUS and the ingester.

.. code-block:: bash

  export NEXUS_DIR=~/nexus-build/nexus
  export INGESTER_DIR=~/nexus-build/ingester

  mkdir -p ${NEXUS_DIR}
  mkdir -p ${INGESTER_DIR}

We should also set a variable for a consistent tag across all images.

.. code-block:: bash

  export VERSION=1.0-build-demo

--TBD: Dl & extract nexus & ingester tarballs--

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

  docker build . -f collection_manager/docker/Dockerfile -t nexusjpl/collection-manager:${VERSION}

Build the Granule Ingester
-------

From the ingester root directory, run:

.. code-block:: bash

  docker build . -f granule_ingester/docker/Dockerfile -t nexusjpl/granule-ingester:${VERSION}

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

  docker build . -t nexusjpl/solr:${VERSION}

Build the Solr Initialization Image
-------

Now to build the image:

.. code-block:: bash

  docker build . -t nexusjpl/solr-cloud-init:${VERSION} -f cloud-init/Dockerfile

Build the Webapp Image
---------

For the final image, we must return to the NEXUS root directory.

.. code-block:: bash

  cd ${NEXUS_DIR}

Now we can build the webapp with:

.. code-block:: bash

  docker build . -f docker/nexus-webapp/Dockerfile -t nexusjpl/nexus-webapp:${VERSION}

Verify Successful Build
====

To verify build success, follow the :ref:`quickstart guide<quickstart>`.


Finished!
=====

Congratulations! You have successfully built the images required for running NEXUS.

If you used your own repository for the image tags, you can push them using ``docker push``.

