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

FROM eclipse-temurin:8

COPY --chmod=755 docker/nexus-webapp/install_python.sh /tmp/install_python.sh

# Install python
RUN /tmp/install_python.sh
RUN ln -s /opt/python/3.9.7/bin/python3.9  /opt/python/3.9.7/bin/python

ENV PATH="/opt/python/3.9.7/bin:$PATH"

# Python & Poetry env vars
ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    # poetry
    # https://python-poetry.org/docs/configuration/#using-environment-variables
    POETRY_VERSION=1.8.1 \
    # make poetry install to this location
    POETRY_HOME="/opt/poetry" \
    # make poetry create the virtual environment in the project's root
    # it gets named `.venv`
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    \
    VENV_PATH="/incubator-sdap-nexus/.venv" \
    POETRY_CACHE_DIR="/opt/.cache/poetry"

# Edit this to rebuild everything from this point
ARG REBUILD_CODE=0

WORKDIR /incubator-sdap-nexus
COPY poetry.lock pyproject.toml ./
COPY data-access /incubator-sdap-nexus/data-access
COPY analysis /incubator-sdap-nexus/analysis
COPY tools /incubator-sdap-nexus/tools
COPY docker/nexus-webapp/install_nexusproto.sh /tmp/install_nexusproto.sh
COPY docker/nexus-webapp/entrypoint.sh /opt/entrypoint.sh

# Copy readme + ASF legal files
COPY README.md README LICENSE NOTICE ./

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# Install additional dependencies for image
RUN apt-get update && \
    apt-get install --no-install-recommends -y proj-bin tini git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install poetry, install project, cleanup, and remove poetry install from layer
RUN curl -sSL https://install.python-poetry.org -o /tmp/install_poetry.py &&  \
    python /tmp/install_poetry.py && \
    poetry install --no-dev &&  \
    rm -rf $POETRY_CACHE_DIR && \
    python /tmp/install_poetry.py --uninstall && \
    rm /tmp/install_poetry.py

ARG REBUILD_CODE=1

ARG BUILD_NEXUSPROTO

ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master

RUN /tmp/install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH

# Spark, Nexus, etc env vars
ENV  \
    PYTHONPATH=/incubator-sdap-nexus/.venv/share/py4j/py4j0.10.9.3.jar \
    NEXUS_SRC=/tmp/incubator-sdap-nexus \
    \
#    PROJ_LIB=/opt/conda/lib/python3.8/site-packages/pyproj/data	\
    PATH="$POETRY_HOME/bin:$VENV_PATH/bin:/opt/python/3.9.7/bin:$PATH" \
    SPARK_HOME=/incubator-sdap-nexus/.venv/lib/python3.9/site-packages/pyspark \
    PYSPARK_DRIVER_PYTHON=/incubator-sdap-nexus/.venv/bin/python3.9 \
    PYSPARK_PYTHON=/incubator-sdap-nexus/.venv/bin/python3.9 \
    LD_LIBRARY_PATH=/usr/lib \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

# This image has k8s client jar version 5.4.1, idk if we should force-install 4.12.0 or if it's even needed anymore...
#RUN rm $SPARK_HOME/jars/kubernetes-client-4.12.0.jar
#ADD https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.12.0/kubernetes-client-4.12.0.jar $SPARK_HOME/jars

ENTRYPOINT ["/opt/entrypoint.sh"]
