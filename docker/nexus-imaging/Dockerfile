FROM nexusjpl/spark-mesos-base

RUN yum -y install unzip aws-cli

RUN cd /tmp
RUN git clone -b image-gen https://github.com/dataplumber/nexus.git
COPY docker-entrypoint.sh /tmp/docker-entrypoint.sh

WORKDIR /tmp
ENTRYPOINT ["/tmp/docker-entrypoint.sh"]
