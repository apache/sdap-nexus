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

set -ebx

DL_HOST=${1:-"http://d3kbcqa49mib13.cloudfront.net"}
VERSION=${2:-"2.2.0"}
DIR=${3:-"spark-${VERSION}"}
INSTALL_DIR=${4:-"/usr/local"}

pushd ${INSTALL_DIR}
wget --quiet ${DL_HOST}/spark-${VERSION}-bin-hadoop2.7.tgz
tar -xzf spark-${VERSION}-bin-hadoop2.7.tgz
chown -R root.root spark-${VERSION}-bin-hadoop2.7.tgz
ln -s spark-${VERSION}-bin-hadoop2.7 ${DIR}
rm spark-${VERSION}-bin-hadoop2.7.tgz
popd
