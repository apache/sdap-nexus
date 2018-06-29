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
ARG tag_version=1.0.0-SNAPSHOT
FROM sdap/nexus-webapp:${tag_version}

MAINTAINER Apache SDAP "dev@sdap.apache.org"

RUN yum -y install unzip aws-cli

COPY docker-entrypoint.sh /tmp/docker-entrypoint.sh

WORKDIR /tmp
ENTRYPOINT ["/tmp/docker-entrypoint.sh"]
