.. _solr_cloud:

sdap/solr-cloud
--------------------

This image runs SolrCloud.

How To Build
^^^^^^^^^^^^

This image can be built from the incubator/sdap/solr directory:

.. code-block:: bash

    docker build -t sdap/solr-cloud:${BUILD_VERSION} -f cloud/Dockerfile --build-arg tag_version=${BUILD_VERSION} .

How to Run
^^^^^^^^^^

This Docker container runs Apache Solr v7.4 in cloud mode with the nexustiles collection. It requires a running Zookeeper service in order to work. It will automatically bootstrap Zookeeper by uploading configuration and core properties to Zookeeper when it starts.

It is necessary to decide wether or not you want data to persist when the container is stopped or if the data should be discarded.

.. note::

  There are multiple times that ``host.docker.internal`` is used in the example ``docker run`` commands provided below. This is a special DNS name that is known to work on Docker for Mac for `connecting from a container to a service on the host <https://docs.docker.com/docker-for-mac/networking/#i-want-to-connect-from-a-container-to-a-service-on-the-host>`_. If you are not launching the container with Docker for Mac, there is no guarantee that this DNS name will be resolvable inside the container.

Cloud Specific Environment Variables
""""""""""""""""""""""""""""""""""""

``SDAP_ZK_SERVICE_HOST``
    *default: localhost*

    This is the hostname of the Zookeeper service that Solr should use to connect.

``SDAP_ZK_SERVICE_PORT``
    *default: 2181*

    The port Solr should try to connect to Zookeeper with.

``SDAP_ZK_SOLR_CHROOT``
    *default: solr*

    The Zookeeper chroot under which Solr configuration will be accessed.

``SOLR_HOST``
    *default: localhost*

    The hostname of the Solr instance that will be recored in Zookeeper.

Zookeeper
""""""""""""

Zookeeper can be running on the host machine or anywhere that docker can access (e.g. a bridge network). Take note of the host where Zookeeper is running and use that value for the ``SDAP_ZK_SERVICE_HOST`` environment variable.


Persist Data
""""""""""""""""

To persist the data, we need to provide a volume mount from the host machine to the container path where the collection data is stored. By default, collection data is stored in the location indicated by the ``$SOLR_HOME`` environment variable. If you do not provide a custom ``SOLR_HOME`` location, the default is ``/opt/solr/server/solr``.

Assuming Zookeeper is running on the host machine port 2181, the easiest way to run this image and persist data to a location on the host machine is:

.. code-block:: bash

    docker run --name solr -v ${PWD}/solrhome:/opt/solr/server/solr -p 8983:8983 -d -e SDAP_ZK_SERVICE_HOST="host.docker.internal" -e SOLR_HOST="host.docker.internal" sdap/solr-cloud:${VERSION}

``${PWD}/solrhome`` is the directory on host machine where ``SOLR_HOME`` will be created if it does not already exist.

Don't Persist Data
""""""""""""""""""

If you do not need to persist data between runs of this image, just simply run the image without a volume mount.

Assuming Zookeeper is running on the host machine port 2181, the easiest way to run this image without persisting data is:

.. code-block:: bash

    docker run --name solr -p 8983:8983 -d -e SDAP_ZK_SERVICE_HOST="host.docker.internal" -e SOLR_HOST="host.docker.internal" sdap/solr-cloud:${VERSION}

When the container is removed, the data will be lost.

Collection Initialization
^^^^^^^^^^^^^^^^^^^^^^^^^^

Solr Collections must be created after at least one SolrCloud node is live. When a collection is created, by default Solr will attempt to spread the shards across all of the live nodes at the time of creation. This poses two problems

1) The nexustiles collection can not be created during a "bootstrapping" process in this image.
2) The nexustiles collection should not be created until an appropriate amount of nodes are live.

A helper container has been created to deal with these issues. See :ref:`solr_cloud_init` for more details.

The other option is to create the collection manually after starting as many SolrCloud nodes as desired. This can be done through the Solr Admin UI or by utilizing the `admin collections API <http://lucene.apache.org/solr/guide/7_4/collections-api.html#collections-api>`_.
