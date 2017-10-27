#!/bin/bash
ZK="zk1:2181,zk2:2181,zk3:2181"
if solr zk ls /solr -z $ZK ; then
    echo "/solr root exists in ZooKeeper. Skip creation of /solr root."
else
    echo "Create /solr root path in ZooKeeper"
    if solr zk mkroot /solr -z $ZK ; then
        echo "Upload solr.xml to zookeeper"
        solr zk cp file:/opt/solr/server/solr/solr.xml zk:/solr.xml -z $ZK/solr
        echo "Clone nexus repo"
        git clone https://github.com/dataplumber/nexus.git /opt/solr/nexus
        echo "Upload nexustiles config to zookeeper"
        solr zk upconfig -n nexustiles -d /opt/solr/nexus/data-access/config/schemas/solr/nexustiles/conf -z $ZK/solr
    fi
fi
