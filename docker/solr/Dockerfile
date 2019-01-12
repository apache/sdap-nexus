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
FROM solr:7.4
MAINTAINER Apache SDAP "dev@sdap.apache.org"

USER root

ENV SOLR_HOME=/opt/solr/server/solr

RUN cd / && \
    apt-get update && \
    apt-get -y install git sudo && \
    rm -rf /var/lib/apt/lists/* && \
    git clone https://github.com/apache/incubator-sdap-nexus.git && \
    cp -r /incubator-sdap-nexus/data-access/config/schemas/solr/nexustiles /tmp/nexustiles && \
    rm -rf /incubator-sdap-nexus && \
    wget http://central.maven.org/maven2/org/locationtech/jts/jts-core/1.15.1/jts-core-1.15.1.jar && \
    cp jts-core-1.15.1.jar /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/jts-core-1.15.1.jar && \
    chown ${SOLR_USER}:${SOLR_GROUP} /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/jts-core-1.15.1.jar && \
    rm jts-core-1.15.1.jar


USER ${SOLR_USER}
