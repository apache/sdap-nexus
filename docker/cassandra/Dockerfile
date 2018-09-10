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

FROM cassandra:2.2

MAINTAINER Apache SDAP "dev@sdap.apache.org"

RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /docker-entrypoint-initdb.d && \
  chown -R cassandra:cassandra /docker-entrypoint-initdb.d && \
	chmod 777 /docker-entrypoint-initdb.d && \
  cd /tmp && \
  git clone https://github.com/apache/incubator-sdap-nexus.git && \
  cp -r /tmp/incubator-sdap-nexus/data-access/config/schemas/cassandra/nexustiles.cql /docker-entrypoint-initdb.d/nexustiles.cql && \
  rm -rf /tmp/incubator-sdap-nexus

COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
