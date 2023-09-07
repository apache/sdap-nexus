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

FROM python:3.8.13

COPY tools/domspurge/purge.py /domspurge/purge.py
COPY tools/domspurge/requirements.txt /domspurge/requirements.txt
COPY tools/domspurge/entrypoint.sh /domspurge/entrypoint.sh
COPY analysis/webservice/algorithms/doms/DomsInitialization.py /domspurge/DomsInitialization.py

# We just need the keyspace & table initialization. Remove references to other SDAP modules.
# Doing this because if the schemas change, we want it to be reflected in this utility
RUN sed '/nexus_initializer/d' -i /domspurge/DomsInitialization.py

WORKDIR /domspurge

RUN pip3 install -r requirements.txt

ENTRYPOINT ["/bin/bash", "/domspurge/entrypoint.sh"]
