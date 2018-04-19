#!/bin/bash

set -e

sed -i "s/server.socket_host=.*/server.socket_host=$SPARK_LOCAL_IP/g" ${NEXUS_SRC}/analysis/webservice/config/web.ini && \
sed -i "s/host=127.0.0.1/host=$CASSANDRA_CONTACT_POINTS/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini && \
sed -i "s/local_datacenter=.*/local_datacenter=$CASSANDRA_LOCAL_DATACENTER/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini && \
sed -i "s/host=localhost:8983/host=$SOLR_URL_PORT/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini

cd ${NEXUS_SRC}/data-access
python setup.py install --force

cd ${NEXUS_SRC}/analysis
python setup.py install --force

python -m webservice.webapp
