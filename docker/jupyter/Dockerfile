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
FROM jupyter/scipy-notebook

MAINTAINER Apache SDAP "dev@sdap.apache.org"

USER root
RUN apt-get update && \
    apt-get install -y git libgeos-dev
USER jovyan

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt && \
    conda install -y basemap

ENV CHOWN_HOME_OPTS='-R'
ENV REBUILD_CODE=true

ARG APACHE_NEXUS=https://github.com/apache/incubator-sdap-nexus.git
ARG APACHE_NEXUS_COMMIT=bb037b6f14a3f103aa49394bdf92df061d6becd7
# Pull specific commit that had nexuscli code before it was removed

RUN mkdir -p /home/jovyan/Quickstart && \
    mkdir -p /home/jovyan/nexuscli && \
    cd /home/jovyan/nexuscli && \
    git init && \
    git remote add -f origin ${APACHE_NEXUS} && \
    git config core.sparseCheckout true && \
    echo "client" >> .git/info/sparse-checkout && \
    git pull origin ${APACHE_NEXUS_COMMIT} && \
    cd client && \
    python setup.py install

COPY ["Time Series Example.ipynb", "/home/jovyan/Quickstart/Time Series Example.ipynb"]
