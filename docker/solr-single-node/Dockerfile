
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
ARG tag_version=latest
FROM sdap/solr:${tag_version}
MAINTAINER Apache SDAP "dev@sdap.apache.org"

USER root

RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

RUN cd / && git clone https://github.com/apache/incubator-sdap-nexus.git && cp -r /incubator-sdap-nexus/data-access/config/schemas/solr/nexustiles . && rm -rf /incubator-sdap-nexus

USER $SOLR_USER

RUN cp -r /nexustiles /opt/solr/server/solr/.

VOLUME ["/opt/solr/server/solr/nexustiles/data"]
