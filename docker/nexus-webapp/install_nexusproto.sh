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

if [ ! -z ${BUILD_NEXUSPROTO+x} ]; then
  echo 'Building nexusproto from source...'

  APACHE_NEXUSPROTO="https://github.com/apache/incubator-sdap-nexusproto.git"
  MASTER="master"

  GIT_REPO=${1:-$APACHE_NEXUSPROTO}
  GIT_BRANCH=${2:-$MASTER}

  mkdir nexusproto
  pushd nexusproto
  git init
  git pull ${GIT_REPO} ${GIT_BRANCH}

  ./gradlew pythonInstall --info

  ./gradlew install --info

  rm -rf /root/.gradle
  popd
  rm -rf nexusproto
else
  pip install nexusproto
fi
