#!/bin/bash

set -e

if [ -n "$TORNADO_HOST" ]; then
  sed -i "s/server.socket_host = .*/server.socket_host = '${TORNADO_HOST}'/g" ${NEXUS_SRC}/analysis/webservice/config/web.ini
fi
sed -i "s/host=127.0.0.1/host=$CASSANDRA_CONTACT_POINTS/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini && \
sed -i "s/local_datacenter=.*/local_datacenter=$CASSANDRA_LOCAL_DATACENTER/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini && \
sed -i "s/host=localhost:8983/host=$SOLR_URL_PORT/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini

# DOMS
sed -i "s/module_dirs=.*/module_dirs=webservice.algorithms,webservice.algorithms_spark,webservice.algorithms.doms/g" ${NEXUS_SRC}/analysis/webservice/config/web.ini && \
sed -i "s/host=.*/host=$CASSANDRA_CONTACT_POINTS/g" ${NEXUS_SRC}/analysis/webservice/algorithms/doms/domsconfig.ini && \
sed -i "s/local_datacenter=.*/local_datacenter=$CASSANDRA_LOCAL_DATACENTER/g" ${NEXUS_SRC}/analysis/webservice/algorithms/doms/domsconfig.ini

cd ${NEXUS_SRC}/data-access
python setup.py install --force

cd ${NEXUS_SRC}/analysis
python setup.py install --force

python -m webservice.webapp
