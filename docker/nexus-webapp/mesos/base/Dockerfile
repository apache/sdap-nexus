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
    yum -y install \
    bzip2 \
    gcc \
    git \
    mesa-libGL.x86_64 \
    python-devel \
    wget \
    which && \
    yum clean all

# Set environment variables.  For Mesos, I used MESOS_VER because MESOS_VERSION
# is expected to be a logical TRUE/FALSE flag that tells Mesos whether or not
# to simply print the version number and exit.

ENV INSTALL_LOC=/usr/local \
    HADOOP_VERSION=2.7.3 \
    SPARK_VERSION=2.2.0 \
    MESOS_VER=1.5.2 \
    MESOS_MASTER_PORT=5050 \
    MESOS_AGENT_PORT=5051 \
    MESOS_WORKDIR=/var/lib/mesos \
    MESOS_IP=0.0.0.0 \
    MESOS_MASTER_NAME=mesos-master \
    PYTHON_EGG_CACHE=/tmp \
    JAVA_HOME=/usr/java/default \
    NEXUS_SRC=/tmp/incubator-sdap-nexus

ENV CONDA_HOME=${INSTALL_LOC}/anaconda2 \
    MESOS_HOME=${INSTALL_LOC}/mesos-${MESOS_VER} \
    SPARK_DIR=spark-${SPARK_VERSION} \
    SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.7 \
    MESOS_MASTER=mesos://${MESOS_IP}:${MESOS_PORT} \
    MESOS_PACKAGE=mesos-${MESOS_VER}.tar.gz

ENV SPARK_HOME=${INSTALL_LOC}/${SPARK_DIR} \
    PYSPARK_DRIVER_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_PYTHON=${CONDA_HOME}/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory=4g pyspark-shell" \
    # Workaround for missing environment variable for basemap https://github.com/conda-forge/basemap-feedstock/issues/30
    PROJ_LIB=${CONDA_HOME}/share/proj

ENV PYTHONPATH=${PYTHONPATH}:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.4-src.zip:${SPARK_HOME}/python/lib/pyspark.zip \
    MESOS_NATIVE_JAVA_LIBRARY=${INSTALL_LOC}/lib/libmesos.so \
    SPARK_EXECUTOR_URI=${INSTALL_LOC}/${SPARK_PACKAGE}.tgz \
    PATH=${CONDA_HOME}/bin:${PATH}

# Install Oracle JDK
COPY install_java.sh ./install_java.sh
RUN ./install_java.sh "https://download.oracle.com/otn-pub/java/jdk/8u201-b09/42970487e3af4f5aa5bca3f542482c60/jdk-8u201-linux-x64.rpm"

# Set up Mesos
COPY mesos/base/install_mesos.sh ./install_mesos.sh
RUN source ./install_mesos.sh

# Install Spark
COPY install_spark.sh ./install_spark.sh
RUN ./install_spark.sh "http://d3kbcqa49mib13.cloudfront.net" ${SPARK_VERSION} ${SPARK_DIR} ${INSTALL_LOC}

# Install Anaconda
COPY install_conda.sh ./install_conda.sh
RUN ./install_conda.sh "https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh" ${CONDA_HOME}

# Conda dependencies for nexus
RUN conda config --add channels conda-forge && \
    conda install -y netCDF4 \
                     backports.functools_lru_cache \
                     numpy \
                     cython \
                     mpld3 \
                     scipy \
                     basemap \
                     gdal \
                     matplotlib && \
    pip install shapely==1.5.16 cassandra-driver==3.5.0

# Install nexusproto and nexus
ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master
ARG APACHE_NEXUS=https://github.com/apache/incubator-sdap-nexus.git
ARG APACHE_NEXUS_BRANCH=master
ARG REBUILD_CODE=1
COPY install_nexusproto.sh ./install_nexusproto.sh
COPY install_nexus.sh ./install_nexus.sh
RUN /tmp/install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH && \
    /tmp/install_nexus.sh $APACHE_NEXUS $APACHE_NEXUS_BRANCH $NEXUS_SRC

CMD ["/bin/bash"]
