.. _testing:

******************
SDAP Testing Guide
******************

This guide covers how to set up and run SDAP testing and how to clean up afterwards.

.. note::

  Unless otherwise specified, all commands run in this guide are run from the ``<repo root>/tests`` directory.

Before You Begin
================

Ensure you have SDAP up and running by running through the :ref:`Quickstart Guide<quickstart>`. For now, you just need to have
Solr, Cassandra, and RabbitMQ started and initialized, but you can run through the complete guide if you desire.

.. note::

  You'll need to use the same shell you used in the quickstart guide here as this guide refers to some of the same environment variables.

Download and Ingest Test Data
=============================

The tests utilize data from multiple source collections. We've prepared a script to download only the necessary input files
and arrange them in subdirectories of your SDAP data directory.

.. code-block:: bash

  ./download_data.sh

Now you will need to define the collections in the collection config. If you've already started the Collection Manager,
you can simply update the config and it should look for the files within about 30 seconds or so.

.. code-block:: bash

  tail -n +2 test_collections.yaml >> ${CONFIG_DIR}/collectionConfig.yml

If the Collection Manager does not appear to be detecting the data, try restarting it.

If you have not started the Collection Manager, start it now:

.. code-block:: bash

  docker run --name collection-manager --network sdap-net -v ${DATA_DIRECTORY}:/data/granules/ -v $(pwd):/home/ingester/config/ -e COLLECTIONS_PATH="/home/ingester/config/test_collections.yaml" -e HISTORY_URL="http://host.docker.internal:8983/" -e RABBITMQ_HOST="host.docker.internal:5672" -e RABBITMQ_USERNAME="user" -e RABBITMQ_PASSWORD="bitnami" -d ${REPO}/sdap-collection-manager:${COLLECTION_MANAGER_VERSION}

Refer to the :ref:`Quickstart Guide<quickstart>` to see how many files are enqueued for ingest, there should be 207 total.
(This may appear to be less if you have ingesters running. We recommend not starting the ingesters until all data is queued.
You may also see more if the Collection Manager was running during the data download. This is a known issue where the Collection
Manager queues downloading files more than once as they're seen as modified.)

Once the data is ready for ingest, start up the ingester(s) and wait for them to finish. After that, you can stop the Collection Manager,
ingester and RabbitMQ containers and start the webapp container if it is not already running.

Set Up pytest
=============

TBA

Run the Tests!
==============

TBA

Cleanup
=======

TBA
