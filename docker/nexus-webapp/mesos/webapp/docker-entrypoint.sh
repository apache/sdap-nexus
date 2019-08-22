#!/bin/bash

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

set -ebx

source activate ${CONDA_ENV_NAME}

if [ -n "$TORNADO_HOST" ]; then
  sed -i "s/server.socket_host.*/server.socket_host=${TORNADO_HOST}/g" ${NEXUS_SRC}/analysis/webservice/config/web.ini
fi
if [ -n "$MAX_PROCESSES" ]; then
  sed -i "s/server.num_sub_processes.*/server.num_sub_processes=${MAX_PROCESSES}/g" ${NEXUS_SRC}/analysis/webservice/config/web.ini
fi
sed -i "s/host=127.0.0.1/host=$CASSANDRA_CONTACT_POINTS/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini && \
sed -i "s/local_datacenter.*/local_datacenter=$CASSANDRA_LOCAL_DATACENTER/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini && \
sed -i "s/host=localhost:8983/host=$SOLR_URL_PORT/g" ${NEXUS_SRC}/data-access/nexustiles/config/datastores.ini

# DOMS
sed -i "s/module_dirs=.*/module_dirs=webservice.algorithms,webservice.algorithms_spark,webservice.algorithms.doms/g" ${NEXUS_SRC}/analysis/webservice/config/web.ini && \
sed -i "s/host=.*/host=$CASSANDRA_CONTACT_POINTS/g" ${NEXUS_SRC}/analysis/webservice/algorithms/doms/domsconfig.ini && \
sed -i "s/local_datacenter=.*/local_datacenter=$CASSANDRA_LOCAL_DATACENTER/g" ${NEXUS_SRC}/analysis/webservice/algorithms/doms/domsconfig.ini

cd ${NEXUS_SRC}/data-access
python setup.py install --force

cd ${NEXUS_SRC}/analysis
python setup.py install --force

python -m webservice.webapp
