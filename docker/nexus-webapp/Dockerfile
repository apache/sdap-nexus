# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run example: docker run -it --net=host -p 8083:8083 -e MASTER=mesos://127.0.0.1:5050 nexus-webapp
FROM centos:7

MAINTAINER Apache SDAP "dev@sdap.apache.org"

ARG SPARK_VERSION=2.2.0
ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master
ARG APACHE_NEXUS=https://github.com/apache/incubator-sdap-nexus.git
ARG APACHE_NEXUS_BRANCH=master

RUN yum -y update && \
    yum -y install \
    bzip2 \
    gcc \
    git \
    mesa-libGL.x86_64 \
    python-devel \
    wget \
    which && \
    yum clean all

ENV SPARK_LOCAL_IP=127.0.0.1 \
    CASSANDRA_CONTACT_POINTS=127.0.0.1 \
    CASSANDRA_LOCAL_DATACENTER=datacenter1 \
    SOLR_URL_PORT=127.0.0.1:8983 \
    SPARK_DIR=spark-${SPARK_VERSION} \
    SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.7 \
    SPARK_HOME=/usr/local/spark-${SPARK_VERSION} \
    PYSPARK_DRIVER_PYTHON=/usr/local/anaconda2/bin/python \
    PYSPARK_PYTHON=/usr/local/anaconda2/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory=4g pyspark-shell" \
    PYTHONPATH=${PYTHONPATH}:/usr/local/spark-${SPARK_VERSION}/python:/usr/local/spark-${SPARK_VERSION}/python/lib/py4j-0.10.4-src.zip:/usr/local/spark-${SPARK_VERSION}/python/lib/pyspark.zip \
    MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so \
    SPARK_EXECUTOR_URI=/usr/local/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
    NEXUS_SRC=/tmp/incubator-sdap-nexus

# Install Spark
RUN cd /usr/local && \
    wget --quiet http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && \
    chown -R root.root spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && \
    ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 ${SPARK_DIR} && \
    rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && \
    cd /

# Install Miniconda
RUN wget -q https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O install_anaconda.sh && \
    /bin/bash install_anaconda.sh -b -p /usr/local/anaconda2 && \
    rm install_anaconda.sh && \
    /usr/local/anaconda2/bin/conda update -n base conda
ENV PATH /usr/local/anaconda2/bin:$PATH
# Conda dependencies for nexus
RUN conda install -c conda-forge -y netCDF4 && \
    conda install -y numpy cython mpld3 scipy basemap gdal matplotlib && \
    pip install shapely cassandra-driver==3.5.0 && \
    conda install -c conda-forge backports.functools_lru_cache=1.3 && \
    cd /usr/lib64 && ln -s libcom_err.so.2 libcom_err.so.3 && \
    cd /usr/local/anaconda2/lib && \
    ln -s libnetcdf.so.11 libnetcdf.so.7 && \
    ln -s libkea.so.1.4.6 libkea.so.1.4.5 && \
    ln -s libhdf5_cpp.so.12 libhdf5_cpp.so.10 && \
    ln -s libjpeg.so.9 libjpeg.so.8

# Install Oracle JDK 1.8u172-b11
RUN wget -q --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u172-b11/a58eab1ec242421181065cdc37240b08/jdk-8u172-linux-x64.rpm" && \
    yum install -y jdk-8u172-linux-x64.rpm && \
    rm jdk-8u172-linux-x64.rpm

COPY *.sh /tmp/

# Install nexusproto and nexus
RUN /tmp/install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH && \
    /tmp/install_nexus.sh $APACHE_NEXUS $APACHE_NEXUS_BRANCH $NEXUS_SRC

EXPOSE 8083

ENTRYPOINT ["/tmp/docker-entrypoint.sh"]
