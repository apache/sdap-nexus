# Run example: docker run --net=nexus --name mesos-agent1 nexusjpl/spark-mesos-agent

FROM nexusjpl/spark-mesos-base

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

# Run a Mesos slave.

WORKDIR ${MESOS_HOME}/build

COPY docker-entrypoint.sh /tmp/docker-entrypoint.sh

ENTRYPOINT ["/tmp/docker-entrypoint.sh"]
