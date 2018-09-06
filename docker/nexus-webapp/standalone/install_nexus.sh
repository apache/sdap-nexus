#!/usr/bin/env bash
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
set -e

APACHE_NEXUS="https://github.com/apache/incubator-sdap-nexus.git"
MASTER="master"
NEXUS_SRC=/incubator-sdap-nexus

GIT_REPO=${1:-$APACHE_NEXUS}
GIT_BRANCH=${2:-$MASTER}
NEXUS_SRC_LOC=${3:-$NEXUS_SRC}

mkdir -p ${NEXUS_SRC_LOC}
pushd ${NEXUS_SRC_LOC}
git init
git pull ${GIT_REPO} ${GIT_BRANCH}

cd data-access
python setup.py install
cd ../analysis
python setup.py install
popd
