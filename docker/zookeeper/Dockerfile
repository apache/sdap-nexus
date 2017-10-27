FROM java:openjdk-8-jre-alpine
MAINTAINER Namrata Malarout <namrata.malarout@jpl.nasa.gov>

LABEL name="zookeeper" version="3.4.8"

RUN apk add --no-cache wget bash \
    && mkdir /opt \
    && wget -q -O - http://apache.mirrors.pair.com/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz | tar -xzf - -C /opt \
    && mv /opt/zookeeper-3.4.8 /opt/zookeeper \
    && cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg \
    && mkdir -p /tmp/zookeeper

EXPOSE 2181 2182 2183 2888 3888 3889 3890

WORKDIR /opt/zookeeper

VOLUME ["/opt/zookeeper/conf", "/tmp/zookeeper"]
RUN mkdir /tmp/zookeeper/1
RUN mkdir /tmp/zookeeper/2
RUN mkdir /tmp/zookeeper/3
RUN printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' 'tickTime=2000' 'dataDir=/tmp/zookeeper/1' 'clientPort=2182' 'initLimit=5' 'syncLimit=2' 'server.1=localhost:2888:3888' 'server.2=localhost:2889:3889' 'server.3=localhost:2890:3890' >> /opt/zookeeper/zoo.cfg
RUN printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' 'tickTime=2000' 'dataDir=/tmp/zookeeper/2' 'clientPort=2182' 'initLimit=5' 'syncLimit=2' 'server.1=localhost:2888:3888' 'server.2=localhost:2889:3889' 'server.3=localhost:2890:3890' > /opt/zookeeper/zoo2.cfg
RUN printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' 'tickTime=2000' 'dataDir=/tmp/zookeeper/3' 'clientPort=2183' 'initLimit=5' 'syncLimit=2' 'server.1=localhost:2888:3888' 'server.2=localhost:2889:3889' 'server.3=localhost:2890:3890' > /opt/zookeeper/zoo3.cfg
RUN cd /opt/zookeeper
RUN cp zoo2.cfg conf/zoo2.cfg
RUN cp zoo3.cfg conf/zoo3.cfg
CMD bin/zkServer.sh start zoo.cfg
