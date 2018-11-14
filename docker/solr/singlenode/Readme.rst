sdap/solr-singlenode
--------------------

This is the singlenode version of Solr.

How To Build
^^^^^^^^^^^^

This image can be built from the incubator/sdap/solr directory:

.. code-block:: bash

    docker build -t sdap/solr-singlenode:${BUILD_VERSION} -f singlenode/Dockerfile --build-arg tag_version=${BUILD_VERSION} .

How to Run
^^^^^^^^^^

This Docker container runs Apache Solr v7.4 as a single node with the nexustiles collection. The main decision when running this image is wether or not you want data to persist when the container is stopped or if the data should be discarded.

Persist Data
""""""""""""

To persist the data, we need to provide a volume mount from the host machine to the container path where `SOLR_HOME` is. If you do not provide a custom `SOLR_HOME` location, the default is `/usr/local/solrhome`. Therefore, the easiest way to run this image and persist data to a location on the host machine is:

.. code-block:: bash

    docker run --name solr -v ${PWD}/solrhome:/usr/local/solrhome -p 8083:8083 -d sdap/solr-singlenode:${VERSION}

${PWD}/solrhome is the directory on host machine where `SOLR_HOME` will be created if it does not already exist. If you have run this container before and ${PWD}/solrhome already contains files, those files will not be overwritten and will be used.

Don't Persist Data
""""""""""""""""""

If you do not need to persist data between runs of this image, just simply run the image without a volume mount.

.. code-block:: bash

    docker run --name solr -p 8083:8083 -d sdap/solr-singlenode:${VERSION}

When the container is removed, the data will be lost.
