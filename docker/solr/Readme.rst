Solr Images
=====================

All docker builds for the Solr images should happen from this directory. For copy/paste ability, first export the environment variable ``BUILD_VERSION`` to the version number you would like to tag images as.

Common Environment Variables
------------------------------

Any environment variable that can be passed to `solr.in.sh <https://github.com/apache/lucene-solr/blob/95d01c6583b825b6b87591e4f27002c285ea25fb/solr/bin/solr.in.sh>`_ and be passed as an environment variable to the docker container and it will be utilized. A few options are called out here:

========================  ========================  =============
       Variable               Default               Description
========================  ========================  =============
``SOLR_HEAP``              ``localhost``            Increase Java Heap as needed to support your indexing / query needs
``SOLR_HOME``              ``/usr/local/solrhome``  Path to a directory for Solr to store cores and their data.

                                                    This directory is exposed as a ``VOLUME`` that can be mounted.
========================  ========================  =============

If you want to mount the ``SOLR_HOME`` directory to a directory on the host machine, you need to provide the container path to the docker run ``-v`` option. Doing this allows you to retain the index between start/stop of this container.

sdap/solr
---------

This is the base image used by both singlenode and cloud versions of the Solr image.

How To Build
^^^^^^^^^^^^

This image can be built by:

.. code-block:: bash

    docker build -t sdap/solr:${BUILD_VERSION} .

How to Run
^^^^^^^^^^

This image is not intended to be run directly

.. include:: ../docker/solr/singlenode/Readme.rst

.. include:: ../docker/solr/cloud/Readme.rst
