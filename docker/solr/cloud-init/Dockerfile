
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

FROM python:3.11
MAINTAINER Apache SDAP "dev@sdap.apache.org"

ENV MINIMUM_NODES="1" \
    SDAP_ZK_SOLR="localhost:2181/solr" \
    SDAP_SOLR_URL="http://localhost:8983/solr/" \
    ZK_LOCK_GUID="c4d193b1-7e47-4b32-a169-a596463da0f5" \
    MAX_RETRIES="30" \
    CREATE_COLLECTION_PARAMS="name=nexustiles&collection.configName=nexustiles&numShards=1"


RUN pip install kazoo==2.6.0 'requests>=2.26.0'
COPY ./cloud-init/create-collection.py /tmp/create-collection.py

ENTRYPOINT ["/tmp/create-collection.py"]
