#!/bin/bash

sed -i "s/server.socket_host.*$/server.socket_host=$SPARK_LOCAL_IP/g" /incubator-sdap-nexus/analysis/webservice/config/web.ini && \
sed -i "s/cassandra1,cassandra2,cassandra3,cassandra4,cassandra5,cassandra6/$CASSANDRA_CONTACT_POINTS/g" /incubator-sdap-nexus/data-access/nexustiles/config/datastores.ini && \
sed -i "s/local_datacenter=.*/local_datacenter=$CASSANDRA_LOCAL_DATACENTER/g" /incubator-sdap-nexus/data-access/nexustiles/config/datastores.ini && \
sed -i "s/solr1:8983/$SOLR_URL_PORT/g" /incubator-sdap-nexus/data-access/nexustiles/config/datastores.ini

cd /incubator-sdap-nexus/data-access
python setup.py install --force

cd /incubator-sdap-nexus/analysis
python setup.py install --force

python -m webservice.webapp
