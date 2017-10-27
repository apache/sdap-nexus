FROM solr:6.4.2
MAINTAINER Nga Quach "Nga.T.Chung@jpl.nasa.gov"

USER root

RUN cd / && wget https://downloads.sourceforge.net/project/jts-topo-suite/jts/1.14/jts-1.14.zip && unzip jts-1.14.zip -d /jts-1.14 && rm jts-1.14.zip

RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

RUN cd / && git clone https://github.com/dataplumber/nexus.git && cp -r /nexus/data-access/config/schemas/solr/nexustiles /tmp/nexustiles && rm -rf /nexus

RUN mkdir /solr-home

RUN chown -R $SOLR_USER:$SOLR_USER /solr-home

VOLUME /solr-home

RUN cp /jts-1.14/lib/jts-1.14.jar /opt/solr/server/lib/jts-1.14.jar

RUN cp /jts-1.14/lib/jtsio-1.14.jar /opt/solr/server/lib/jtsio-1.14.jar
