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
FROM ${repo}//spark-mesos-base:${tag_version}

MAINTAINER Apache SDAP "dev@sdap.apache.org"

# Run a Mesos slave.

WORKDIR /tmp

COPY mesos/agent/docker-entrypoint.sh /tmp/docker-entrypoint.sh


# This will run docker-entrypoint.sh with the value of CMD as default arguments. However, if any arguments are supplied
# to the docker run command when launching this image, the command line arguments will override these CMD arguments
ENTRYPOINT ["/bin/bash", "-c", "/tmp/docker-entrypoint.sh $(eval echo $@)", "$@"]
CMD ["--master=${MESOS_MASTER_NAME}:${MESOS_MASTER_PORT}", "--port=${MESOS_AGENT_PORT}", "--work_dir=${MESOS_WORKDIR}"]
