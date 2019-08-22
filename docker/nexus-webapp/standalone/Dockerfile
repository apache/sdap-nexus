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

ARG SPARK_VERSION=2.2.0
ENV CONDA=/usr/local/anaconda3 \
    CONDA_ENV_NAME=nexus-webapp
ENV SPARK_LOCAL_IP=127.0.0.1 \
    CASSANDRA_CONTACT_POINTS=127.0.0.1 \
    CASSANDRA_PORT=9042 \
    CASSANDRA_LOCAL_DATACENTER=datacenter1 \
    SOLR_URL_PORT=127.0.0.1:8983 \
    SPARK_DIR=spark-${SPARK_VERSION} \
    SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.7 \
    SPARK_HOME=/usr/local/spark-${SPARK_VERSION} \
    PYSPARK_DRIVER_PYTHON=${CONDA}/envs/${CONDA_ENV_NAME}/bin/python \
    PYSPARK_PYTHON=${CONDA}/envs/${CONDA_ENV_NAME}/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory=4g pyspark-shell" \
    PYTHONPATH=${PYTHONPATH}:/usr/local/spark-${SPARK_VERSION}/python:/usr/local/spark-${SPARK_VERSION}/python/lib/py4j-0.10.4-src.zip:/usr/local/spark-${SPARK_VERSION}/python/lib/pyspark.zip \
    SPARK_EXECUTOR_URI=/usr/local/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
    NEXUS_SRC=/usr/local/incubator-sdap-nexus \
    PATH=${CONDA}/bin:${PATH}

WORKDIR /tmp

# Install Open JDK
COPY install_java.sh ./install_java.sh
RUN ./install_java.sh

# Install Spark
COPY install_spark.sh ./install_spark.sh
RUN ./install_spark.sh "http://d3kbcqa49mib13.cloudfront.net" ${SPARK_VERSION} ${SPARK_DIR} ${INSTALL_LOC}

# Install Anaconda
COPY install_conda.sh ./install_conda.sh
RUN ./install_conda.sh "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" ${CONDA}

# Conda dependencies for nexus
RUN source activate ${CONDA_ENV_NAME} && \
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
RUN source activate ${CONDA_ENV_NAME} && \
    /tmp/install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH && \
    /tmp/install_nexus.sh $APACHE_NEXUS $APACHE_NEXUS_BRANCH $NEXUS_SRC

COPY standalone/docker-entrypoint.sh ./docker-entrypoint.sh
ENTRYPOINT ["/tmp/docker-entrypoint.sh"]
