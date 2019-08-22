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

URL=${1:-"https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"}
CONDA=${2:-"/usr/local/anaconda3"}

CONDA_ENV_NAME=${CONDA_ENV_NAME:-"nexus-webapp"}

pushd /tmp
wget -q ${URL} -O install_anaconda.sh
/bin/bash install_anaconda.sh -b -p ${CONDA}
rm install_anaconda.sh
ln -s ${CONDA}/etc/profile.d/conda.sh /etc/profile.d/conda.sh
chmod +x /usr/local/anaconda3/etc/profile.d/conda.sh

conda update -n base conda
conda config --set channel_priority strict
conda config --prepend channels conda-forge
conda create -y --name ${CONDA_ENV_NAME} python=2

popd
