FROM nexusjpl/nexusbase

WORKDIR /tmp

RUN yum -y install unzip nc

# Create conda environment and install dependencies
RUN conda create -y --name nexus-xd-python-modules python && \
    source activate nexus-xd-python-modules && \
    conda install -y scipy=0.18.1 && \
    conda install -y -c conda-forge nco=4.6.4 netcdf4=1.2.7
    
# Install Spring XD
RUN groupadd -r springxd && adduser -r -g springxd springxd

WORKDIR /usr/local/spring-xd
RUN wget -q "http://repo.spring.io/libs-release/org/springframework/xd/spring-xd/1.3.1.RELEASE/spring-xd-1.3.1.RELEASE-dist.zip" && \
    unzip spring-xd-1.3.1.RELEASE-dist.zip && \
    rm spring-xd-1.3.1.RELEASE-dist.zip && \
    ln -s spring-xd-1.3.1.RELEASE current && \
    mkdir current/xd/lib/none

RUN wget -q "https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.0.8.tar.gz" && \
    tar -zxf mysql-connector-java-5.0.8.tar.gz && \
    mv mysql-connector-java-5.0.8/mysql-connector-java-5.0.8-bin.jar current/xd/lib && \
    rm -rf mysql-connector-java-5.0.8 && \
    rm -f mysql-connector-java-5.0.8.tar.gz && \
    chown -R springxd:springxd spring-xd-1.3.1.RELEASE

USER springxd
ENV PATH $PATH:/usr/local/spring-xd/current/xd/bin:/usr/local/spring-xd/current/shell/bin:/usr/local/spring-xd/current/zookeeper/bin
COPY xd-container-logback.groovy /usr/local/spring-xd/current/xd/config/xd-container-logback.groovy
COPY xd-singlenode-logback.groovy /usr/local/spring-xd/current/xd/config/xd-singlenode-logback.groovy
VOLUME ["/usr/local/spring-xd/current/xd/config"]
EXPOSE 9393

# Configure Java Library Repositories
ENV PATH $PATH:/usr/local/anaconda2/bin
ENV M2_HOME /usr/local/apache-maven
ENV M2 $M2_HOME/bin 
ENV PATH $PATH:$M2
USER root
COPY maven_settings.xml $M2_HOME/conf/settings.xml
COPY ivy_settings.xml /usr/local/repositories/.groovy/grapeConfig.xml
RUN mkdir -p /usr/local/repositories/.m2 && mkdir -p /usr/local/repositories/.groovy && chown -R springxd:springxd /usr/local/repositories

# ########################
# # nexus-ingest code   #
# ########################
WORKDIR /tmp
RUN pwd
COPY install-custom-software.sh /tmp/install-custom-software.sh
RUN /bin/bash install-custom-software.sh
RUN chown -R springxd:springxd /usr/local/spring-xd/spring-xd-1.3.1.RELEASE && \
    chown -R springxd:springxd /usr/local/anaconda2/envs/nexus-xd-python-modules/ && \
    chown -R springxd:springxd /usr/local/repositories
VOLUME ["/usr/local/data/nexus"]

COPY nexus-ingest.sh /usr/local/nexus-ingest.sh
USER springxd
ENTRYPOINT ["/usr/local/nexus-ingest.sh"]
CMD ["--help"]