.. _solr_cloud_init:

sdap/solr-cloud-init
--------------------

This image can be used to automatically create the ``nexustiles`` collection in SolrCloud.

How To Build
^^^^^^^^^^^^

This image can be built from the incubator/sdap/solr directory:

.. code-block:: bash

    docker build -t sdap/solr-cloud-init:${BUILD_VERSION} -f cloud-init/Dockerfile .

How to Run
^^^^^^^^^^

This image is designed to run in a container alongside the :ref:`solr_cloud` container. The purpose is to detect if there are at least ``MINIMUM_NODES`` live nodes in the cluster. If there are, then detect if the ``nexustiles`` collection exists or not. If it does not, this script will create it using the parameters defined by the ``CREATE_COLLECTION_PARAMS`` environment variable. See the reference documents for the `create <http://lucene.apache.org/solr/guide/7_4/collections-api.html#create>`_ function for the Solr collections API for valid parameters.

.. note::

	The ``action=CREATE`` parameter is already passed for you and should not be part of ``CREATE_COLLECTION_PARAMS``

.. note::

  This image was designed to be long running. It will only exit if there was an error detecting or creating the ``nexustiles`` collection.


Environment Variables
""""""""""""""""""""""""""""""""""""

``MINIMUM_NODES``
    *default: 1*

    The minimum number of nodes that must be 'live' before the collection is created.

``SDAP_ZK_SOLR``
    *default: localhost:2181/solr*

    The host:port/chroot of the zookeeper being used by SolrCloud.

``SDAP_SOLR_URL``
    *default: http://localhost:8983/solr/*

    The URL that should be polled to check if a SolrCloud node is running. This should be the URL of the :ref:`solr_cloud` container that is being started alongside this container.
``ZK_LOCK_GUID``
    *default: c4d193b1-7e47-4b32-a169-a596463da0f5*

    A GUID that is used to create a lock in zookeeper so that if more than one of these init containers are started at the same time, only one will attempt to create the collection. This GUID should be the same across all containers that are trying to create the same collection.

``MAX_RETRIES``
    *default: 30*

    The number of times we will try to connect to SolrCloud at ``SDAP_SOLR_URL``. This is roughly equivalent to how many seconds we will wait for the node at ``SDAP_SOLR_URL`` to become available. If ``MAX_RETRIES`` is exceeded, the container will exit with an error.

``CREATE_COLLECTION_PARAMS``
    *default: name=nexustiles&collection.configName=nexustiles&numShards=1*

    The parameters sent to the collection create function. See the reference documents for the `create <http://lucene.apache.org/solr/guide/7_4/collections-api.html#create>`_ function for the Solr collections API for valid parameters.


Example Run
"""""""""""""""

Assuming Zookeeper is running on the host machine port 2181, and a :ref:`solr_cloud` container is also running with port 8983 mapped to the host machine, the easiest way to run this image is:

.. code-block:: bash

    docker run -it --rm --name init -e SDAP_ZK_SOLR="host.docker.internal:2181/solr" -e SDAP_SOLR_URL="http://host.docker.internal:8983/solr/" sdap/solr-cloud-init:${BUILD_VERSION}

After running this image, the ``nexustiles`` collection should be available on the SolrCloud installation. Check the logs for the container to see details.
