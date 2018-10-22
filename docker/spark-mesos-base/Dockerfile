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
FROM centos:7

MAINTAINER Apache SDAP "dev@sdap.apache.org"

WORKDIR /tmp

RUN yum -y update && \
    yum -y install wget \
    git \
    which \
    bzip2 \
    gcc \
    python-devel

# Install Oracle JDK 1.8u121-b13
RUN wget -q --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.rpm" && \
    yum -y install jdk-8u121-linux-x64.rpm && \
    rm jdk-8u121-linux-x64.rpm
ENV JAVA_HOME /usr/java/default

# ########################
# # Apache Maven   #
# ########################
ENV M2_HOME /usr/local/apache-maven
ENV M2 $M2_HOME/bin
ENV PATH $PATH:$M2

RUN mkdir $M2_HOME && \
    wget -q http://mirror.stjschools.org/public/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && \
    tar -xvzf apache-maven-3.3.9-bin.tar.gz -C $M2_HOME --strip-components=1 && \
    rm -f apache-maven-3.3.9-bin.tar.gz

# ########################
# # Anaconda   #
# ########################
RUN wget -q https://repo.continuum.io/archive/Anaconda2-4.3.0-Linux-x86_64.sh -O install_anaconda.sh && \
    /bin/bash install_anaconda.sh -b -p /usr/local/anaconda2 && \
    rm install_anaconda.sh
ENV PATH $PATH:/usr/local/anaconda2/bin

# Set environment variables.  For Mesos, I used MESOS_VER because MESOS_VERSION
# is expected to be a logical TRUE/FALSE flag that tells Mesos whether or not
# to simply print the version number and exit.

ENV INSTALL_LOC=/usr/local \
    HADOOP_VERSION=2.7.3 \
    SPARK_VERSION=2.1.0 \
    MESOS_VER=1.5.0 \
    MESOS_MASTER_PORT=5050 \
    MESOS_AGENT_PORT=5051 \
    MESOS_WORKDIR=/var/lib/mesos \
    MESOS_IP=0.0.0.0 \
    MESOS_MASTER_NAME=mesos-master \
    PYTHON_EGG_CACHE=/tmp

ENV CONDA_HOME=${INSTALL_LOC}/anaconda2 \
    MESOS_HOME=${INSTALL_LOC}/mesos-${MESOS_VER} \
    SPARK_DIR=spark-${SPARK_VERSION} \
    SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.7 \
    MESOS_MASTER=mesos://${MESOS_IP}:${MESOS_PORT} \
    MESOS_PACKAGE=mesos-${MESOS_VER}.tar.gz

ENV SPARK_HOME=${INSTALL_LOC}/${SPARK_DIR} \
    PYSPARK_DRIVER_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory=4g pyspark-shell"

ENV PYTHONPATH=${PYTHONPATH}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.4-src.zip:${SPARK_HOME}/python/lib/pyspark.zip \
    MESOS_NATIVE_JAVA_LIBRARY=${INSTALL_LOC}/lib/libmesos.so \
    SPARK_EXECUTOR_URI=${INSTALL_LOC}/${SPARK_PACKAGE}.tgz

WORKDIR ${INSTALL_LOC}

# Set up Spark

RUN wget --quiet http://d3kbcqa49mib13.cloudfront.net/${SPARK_PACKAGE}.tgz && \
    tar -xzf ${SPARK_PACKAGE}.tgz && \
    chown -R root.root ${SPARK_PACKAGE} && \
    ln -s ${SPARK_PACKAGE} ${SPARK_DIR}

# Set up Mesos

COPY install_mesos.sh .

RUN source ./install_mesos.sh && \
    mkdir ${MESOS_WORKDIR}

# Set up Anaconda environment

ENV PATH=${CONDA_HOME}/bin:${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin

RUN conda install -c conda-forge -y netCDF4 && \
    conda install -y numpy cython mpld3 scipy basemap gdal matplotlib && \
    pip install shapely cassandra-driver==3.5.0 && \
    conda install -c conda-forge backports.functools_lru_cache=1.3

# Workaround missing libcom_err.so (needed for gdal)

RUN cd /usr/lib64 && ln -s libcom_err.so.2 libcom_err.so.3

# Workaround missing conda libs needed for gdal

RUN cd ${CONDA_HOME}/lib && \
    ln -s libnetcdf.so.11 libnetcdf.so.7 && \
    ln -s libkea.so.1.4.6 libkea.so.1.4.5 && \
    ln -s libhdf5_cpp.so.12 libhdf5_cpp.so.10 && \
    ln -s libjpeg.so.9 libjpeg.so.8

RUN yum install -y mesa-libGL.x86_64

# Install nexusproto
ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master
COPY install_nexusproto.sh ./install_nexusproto.sh
RUN ./install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH

# Retrieve NEXUS code and build it.

WORKDIR /

RUN git clone https://github.com/apache/incubator-sdap-nexus.git

RUN sed -i 's/,webservice.algorithms.doms//g' /incubator-sdap-nexus/analysis/webservice/config/web.ini && \
    sed -i 's/127.0.0.1/nexus-webapp/g' /incubator-sdap-nexus/analysis/webservice/config/web.ini && \
    sed -i 's/127.0.0.1/cassandra1,cassandra2,cassandra3,cassandra4,cassandra5,cassandra6/g' /incubator-sdap-nexus/data-access/nexustiles/config/datastores.ini && \
    sed -i 's/localhost:8983/solr1:8983/g' /incubator-sdap-nexus/data-access/nexustiles/config/datastores.ini

WORKDIR /incubator-sdap-nexus/data-access

RUN python setup.py install

WORKDIR /incubator-sdap-nexus/analysis

RUN python setup.py install

WORKDIR /tmp

CMD ["/bin/bash"]
