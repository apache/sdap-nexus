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
ARG repo=sdap
ARG tag_version=1.0.0-SNAPSHOT
FROM ${repo}/spark-mesos-base:${tag_version}

MAINTAINER Apache SDAP "dev@sdap.apache.org"

COPY /mesos/webapp/docker-entrypoint.sh /tmp/docker-entrypoint.sh
ENV MASTER=mesos://${MESOS_MASTER_NAME}:${MESOS_MASTER_PORT}

ENTRYPOINT ["/tmp/docker-entrypoint.sh"]
