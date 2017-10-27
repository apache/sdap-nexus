nexus
=====

The next generation cloud-based science data service platform

# Developer Installation

1. Follow instructions for installing [nexusproto](nexus-ingest/nexus-messages/README.md)
2. Follow instructions for installing [data-access](data-access/README.md)
3. Follow instructions for setting up [nexus-ingest](nexus-ingest/developer-box/README.md)
  1. [Ingest some data](nexus-ingest/developer-box/README.md#ingesting-data)
4. Follow instructions for running [analysis](analysis/README.md)


# Installation Instructions

## Zookeeper Setup

1. Install and configure Apache Zookeeper 3.4.x
2. Create chroots `solr`, `xd`, and `kafka`

## Database Setup

### Apache Solr

1. Install and configure [Apache Solr Cloud 5.3.x](http://archive.apache.org/dist/lucene/solr/)
2. Download [JTS Topology Suite v1.13](https://sourceforge.net/projects/jts-topo-suite/files/jts/1.13/) and extract the zip.
3. From the exploded JTS zip, copy `$JTS_ZIP/lib/jts-1.13.jar` and `$JTS_ZIP/lib/jtsio-1.13.jar` into `$SOLR_INSTALL_DIR/server/lib/ext` on all Solr nodes.
4. Configure Solr Cloud to use the `/solr` chroot of zookeeper
5. On one of the Solr cloud nodes, upload the `nexustiles` configuration (located in [data-access/config/schemas/solr](data-access/config/schemas/solr)) as a configset

    ````
    ./zkcli.sh -cmd upconfig -z $ZK_SERVERS/solr -confname nexustiles -confdir /path/to/nexustiles/conf
    ````

6. [Create a new collection](https://cwiki.apache.org/confluence/display/solr/Collections+API#CollectionsAPI-api1) with the name nexustiles. Use the nexustiles configset uploaded previously.

    ````
    curl "http://<SOLR_HOST>/solr/admin/collections?action=CREATE&name=nexustiles&collection.configName=nexustiles"
    ````
7. Repeat steps 5 & 6 for the [datasets](data-access/config/schemas/solr) collection.


### Apache Cassandra

1. Install and configure [Apache Cassandra 2.2.x](http://cassandra.apache.org/download/)
2. Execute the DDL located in [nexustiles.cql](data-access/config/schemas/cassandra/nexustiles.cql)

### HSQLDB

1. Install and run [HSQLDB 2.3.x](http://hsqldb.org/)

### Redis

1. Install and run [Redis 3.0.x](http://redis.io/)

## Apache Kafka

1. Install and configure [Apache Kafka 2.11-0.9.0.1](http://kafka.apache.org/)
2. Configure Kafka to use the `/kafka` chroot of zookeeper

## Spring XD

1. Install [Spring XD 1.3.1.RELEASE](http://docs.spring.io/spring-xd/docs/1.3.1.RELEASE/reference/html/)
2. Optionally install [Flo for Spring XD](https://docs.pivotal.io/spring-flo/installing-flo.html)
3. Configure to use previously installed Apache Kafka as messaging bus
4. Configure to use previously installed Redis for analytics
5. Configure to use previously installed HSQLDB for Job Repository
6. Configure to use previously installed Apache Zookeeper using the `xd` chroot
7. Choose and configure a location for the [custom module registry](http://docs.spring.io/spring-xd/docs/current/reference/html/#_the_module_registry)
8. Create a directory called `none` in `$SPRING_XD_HOME/xd/lib`

### Preparing the XD Containers

In order for streams to use the custom python and groovy scripts found in nexus-ingest, the machines that will be running Spring XD containers need to have some software installed on them.

#### Anaconda

1. Install [Anaconda 4.0.0](https://www.continuum.io/downloads) with Python 2.7
2. Create an Anaconda environment

    ````
    conda create --name nexus-xd-python-modules python
    ````
    
3. Install conda dependencies

    ````
    conda install libnetcdf
    conda install netcdf4
    conda install numpy
    ````
    
4. Install [nexusproto](nexus-ingest/nexus-messages)
5. Install [spring-xd-python](nexus-ingest/spring-xd-python)
6. Install [nexus-xd-python-modules](nexus-ingest/nexus-xd-python-modules)

#### Java 8

1. Install Java 8 JDK. Either Oracle or OpenJDK.

#### nexus-messages

1. The [nexus-messages](nexus-ingest/nexus-messages) JAR needs to be available via Maven. There are a number of ways to do this. One way is to explicitly install the dependency into the local maven cache on each XD Container node.

    ````
    mvn install:install-file -DpomFile=nexus-messages-VERSION.xml -Dfile=nexus-messages-VERSION.jar
    ````

#### Groovy Scripts

1. Place the [groovy-scripts](nexus-ingest/groovy-scripts) in a location that will be accessible to all XD Container nodes

#### Custom Modules

1. Upload the [dataset-tiler](nexus-ingest/dataset-tiler) custom module
2. Upload the [tcpshell](nexus-ingest/tcp-shell) custom module
3. Upload the [nexus-sink](nexus-ingest/nexus-sink) custom module
