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

echo "Installing build dependencies"

apt-get update
apt-get upgrade -y
apt-get install --no-install-recommends -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev liblzma-dev tk-dev libffi-dev
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "Downloading & extracting source tarball"

cd /tmp/
wget https://www.python.org/ftp/python/3.9.7/Python-3.9.7.tgz
tar xzf Python-3.9.7.tgz
cd Python-3.9.7

echo "Running install"

./configure --prefix=/opt/python/3.9.7/ --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi
make -j "$(nproc)"
make altinstall

echo "Cleaning up install dir"

rm /tmp/Python-3.9.7.tgz
cd /tmp/
rm -rf Python-3.9.7

echo "Final steps..."

/opt/python/3.9.7/bin/python3.9 -m pip install --upgrade pip setuptools wheel
