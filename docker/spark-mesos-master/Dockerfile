# Run example: docker run --net=nexus --name mesos-master -p 5050:5050 nexusjpl/spark-mesos-master

FROM nexusjpl/spark-mesos-base

MAINTAINER Joseph Jacob "Joseph.Jacob@jpl.nasa.gov"

EXPOSE ${MESOS_MASTER_PORT}

# Run a Mesos master.

WORKDIR ${MESOS_HOME}/build

CMD ["/bin/bash", "-c", "./bin/mesos-master.sh --ip=${MESOS_IP} --hostname=${MESOS_MASTER_NAME} --port=${MESOS_MASTER_PORT} --work_dir=${MESOS_WORKDIR}"]
