#!/bin/bash

sed -i "s/server.socket_host.*$/server.socket_host=$SPARK_LOCAL_IP/g" /nexus/analysis/webservice/config/web.ini && \
sed -i "s/cassandra1,cassandra2,cassandra3,cassandra4,cassandra5,cassandra6/$CASSANDRA_CONTACT_POINTS/g" /nexus/data-access/nexustiles/config/datastores.ini && \
sed -i "s/solr1:8983/$SOLR_URL_PORT/g" /nexus/data-access/nexustiles/config/datastores.ini

cd /nexus/data-access
python setup.py install --force

cd /nexus/analysis
python setup.py install --force

python -m webservice.webapp