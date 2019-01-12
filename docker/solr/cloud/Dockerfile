
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

ENV SDAP_ZK_SERVICE_HOST="localhost" \
    SDAP_ZK_SERVICE_PORT="2181" \
    SDAP_ZK_SOLR_CHROOT="solr" \
    SOLR_HOST="localhost"

COPY ./cloud/docker-entrypoint-initdb.d/* /docker-entrypoint-initdb.d/
COPY ./cloud/tmp/* /tmp/

# This will run docker-entrypoint.sh with the value of CMD as default arguments. However, if any arguments are supplied
# to the docker run command when launching this image, the command line arguments will override these CMD arguments
ENTRYPOINT ["/bin/bash", "-c", "docker-entrypoint.sh $(eval echo $@)", "$@"]
CMD ["solr-foreground", "-c", "-z ${SDAP_ZK_SERVICE_HOST}:${SDAP_ZK_SERVICE_PORT}/${SDAP_ZK_SOLR_CHROOT}"]
