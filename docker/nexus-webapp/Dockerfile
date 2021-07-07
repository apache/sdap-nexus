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

#FROM nexusjpl/alpine-pyspark:3.1.1
FROM tloubrieu/openshift-pyshark
MAINTAINER Apache SDAP "dev@sdap.apache.org"


ARG CONDA_VERSION="4.7.12.1"
ARG CONDA_MD5="81c773ff87af5cfac79ab862942ab6b3"
ARG CONDA_DIR="/opt/conda"

ENV PATH="/opt/conda/bin:${PATH}"
ARG PATH="/opt/conda/bin:${PATH}"

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda \
    && rm -f Miniconda3-latest-Linux-x86_64.sh




RUN conda install python=3.8

# Change REBUILD_CODE if you want tell Docker not to use cached layers from this line on
ARG REBUILD_CODE=0

ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master

RUN apt-get update && apt-get install -y git
COPY docker/nexus-webapp/install_nexusproto.sh ./install_nexusproto.sh
RUN /tmp/install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH

COPY data-access /incubator-sdap-nexus/data-access
COPY analysis /incubator-sdap-nexus/analysis
COPY tools /incubator-sdap-nexus/tools

WORKDIR /incubator-sdap-nexus/data-access
RUN python3 setup.py install

WORKDIR /incubator-sdap-nexus/analysis
RUN python3 setup.py install

WORKDIR /incubator-sdap-nexus/tools/deletebyquery
RUN pip3 install cassandra-driver==3.20.1 --install-option="--no-cython"
RUN pip3 install pyspark py4j
RUN pip3 install -r requirements.txt
RUN pip3 install cython
RUN rm requirements.txt

RUN chmod -R a+rwX /opt/conda/

ENV PROJ_LIB=/opt/conda/lib/python3.8/site-packages/pyproj/data

WORKDIR /incubator-sdap-nexus
