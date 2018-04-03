ARG tag_version=latest
FROM sdap/solr:${tag_version}
MAINTAINER Nga Quach "Nga.T.Chung@jpl.nasa.gov"

USER root

RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

RUN cd / && git clone https://github.com/apache/incubator-sdap-nexus.git && cp -r /incubator-sdap-nexus/data-access/config/schemas/solr/nexustiles . && rm -rf /incubator-sdap-nexus

USER $SOLR_USER

RUN cp -r /nexustiles /opt/solr/server/solr/.

VOLUME ["/opt/solr/server/solr/nexustiles/data"]
