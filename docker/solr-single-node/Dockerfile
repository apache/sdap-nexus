FROM nexusjpl/nexus-solr
MAINTAINER Nga Quach "Nga.T.Chung@jpl.nasa.gov"

USER root

RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

RUN cd / && git clone https://github.com/dataplumber/nexus.git && cp -r /nexus/data-access/config/schemas/solr/nexustiles . && rm -rf /nexus

USER $SOLR_USER

RUN cp -r /nexustiles /opt/solr/server/solr/.

VOLUME ["/opt/solr/server/solr/nexustiles/data"]
