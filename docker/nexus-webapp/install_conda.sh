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

echo "**** install dev packages ****"
apk add --no-cache --virtual .build-dependencies bash wget

echo "**** get Miniconda ****" 
mkdir -p "$CONDA_DIR" 
wget "http://repo.continuum.io/miniconda/Miniconda3-${CONDA_VERSION}-Linux-x86_64.sh" -O miniconda.sh 
echo "$CONDA_MD5  miniconda.sh" | md5sum -c 

echo "**** install Miniconda ****" 
bash miniconda.sh -f -b -p "$CONDA_DIR" 
echo "export PATH=$CONDA_DIR/bin:\$PATH" > /etc/profile.d/conda.sh 

echo "**** setup Miniconda ****" 
conda update --all --yes 
conda config --set auto_update_conda False 

echo "**** cleanup ****" 
apk del --purge .build-dependencies 
rm -f miniconda.sh 
conda clean --all --force-pkgs-dirs --yes 
find "$CONDA_DIR" -follow -type f \( -iname '*.a' -o -iname '*.pyc' -o -iname '*.js.map' \) -delete 

echo "**** finalize ****" 
mkdir -p "$CONDA_DIR/locks" 
chmod 777 "$CONDA_DIR/locks" 
conda update -n base conda