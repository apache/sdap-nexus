FROM centos:7

RUN yum -y update && \
    yum -y install wget

# Install Oracle JDK 1.8u121-b13
RUN wget -q --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.rpm" && \
    yum -y install jdk-8u121-linux-x64.rpm && \
    rm jdk-8u121-linux-x64.rpm
ENV JAVA_HOME /usr/java/default

# Install Kafka
RUN groupadd -r kafka && useradd -r -g kafka kafka
WORKDIR /usr/local/kafka
RUN wget -q http://apache.claz.org/kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz && \
    tar -xvzf kafka_2.11-0.9.0.1.tgz && \
    ln -s kafka_2.11-0.9.0.1 current && \
    rm -f kafka_2.11-0.9.0.1.tgz && \
    chown -R kafka:kafka kafka_2.11-0.9.0.1

ENV PATH $PATH:/usr/local/kafka/current/bin
    
USER kafka
COPY kafka.properties /usr/local/kafka/current/config/

ENTRYPOINT ["kafka-server-start.sh"]
