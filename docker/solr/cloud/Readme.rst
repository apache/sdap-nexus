sdap/solr-cloud
--------------------

This is the cloud version of Solr.

How To Build
^^^^^^^^^^^^

This image can be built from the incubator/sdap/solr directory:

.. code-block:: bash

    docker build -t sdap/solr-cloud:${BUILD_VERSION} -f cloud/Dockerfile --build-arg tag_version=${BUILD_VERSION} .

How to Run
^^^^^^^^^^

This Docker container runs Apache Solr v7.4 in cloud mode with the nexustiles collection. It requires a running Zookeeper service in order to work. It will automatically bootstrap Zookeeper by uploading configuration and core properties to Zookeeper when it starts.

It is necessary to decide wether or not you want data to persist when the container is stopped or if the data should be discarded.

Cloud Specific Environment Variables
""""""""""""""""""""""""""""""""""""

========================  ===============  =============
       Variable               Default       Description
========================  ===============  =============
``SDAP_ZK_SERVICE_HOST``   ``localhost``    This is the hostname of the Zookeeper service that Solr should use to connect.
``SDAP_ZK_SERVICE_PORT``   ``2181``         The port Solr should try to connect to Zookeeper with.
``SDAP_ZK_SOLR_CHROOT``    ``solr``         The Zookeeper chroot under which Solr configuration will be accessed.
``SOLR_HOST``              ``localhost``    The hostname of the Solr instance that will be recored in Zookeeper.
========================  ===============  =============

Zookeeper
""""""""""""

Zookeeper can be running on the host machine or anywhere that docker can access (e.g. a bridge network). Take note of the host where Zookeeper is running and use that value for the ``SDAP_ZK_SERVICE_HOST`` environment variable.

.. note::

	If you choose to run Zookeeper on the host machine and you are using Docker For Mac, you can use the special hostname ``host.docker.internal`` to access the host from inside a container.


Persist Data
""""""""""""

If you want to persist the data between runs, provide a volume mount from the host machine to the container path where ``SOLR_HOME`` is. If you do not provide a custom ``SOLR_HOME`` location, the default is ``/usr/local/solrhome``.
This also assumes you are running Zookeeper on the host machine and that you are using Docker for Mac.

.. code-block:: bash

    docker run --name solr -v ${PWD}/solrhome:/usr/local/solrhome -p 8083:8083 -d -e SDAP_ZK_SERVICE_HOST="host.docker.internal" sdap/solr-cloud:${VERSION}

``${PWD}/solrhome`` is the directory on host machine where ``SOLR_HOME`` will be created if it does not already exist.

Don't Persist Data
""""""""""""""""""

If you do not need to persist data between runs of this image, just simply run the image without a volume mount.
This also assumes you are running Zookeeper on the host machine and that you are using Docker for Mac.

.. code-block:: bash

    docker run --name solr -p 8083:8083 -d -e SDAP_ZK_SERVICE_HOST="host.docker.internal" sdap/solr-cloud:${VERSION}

When the container is removed, the data will be lost.
