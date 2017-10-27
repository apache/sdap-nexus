FROM cassandra:2.2.8

RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

RUN cd / && git clone https://github.com/dataplumber/nexus.git && cp -r /nexus/data-access/config/schemas/cassandra/nexustiles.cql /tmp/. && rm -rf /nexus
