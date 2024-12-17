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

echo "Installing dependencies"

apt-get update
apt-get install --no-install-recommends -y sqlite3 cmake libtiff-dev libcurl4-openssl-dev
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "Building & installing PROJ"

mkdir -p /tmp/gdal-proj/
cd /tmp/gdal-proj/

wget 'https://download.osgeo.org/proj/proj-8.2.1.tar.gz'
tar xf proj-8.2.1.tar.gz

rm proj-8.2.1.tar.gz
cd proj-8.2.1/

mkdir build
cd build/

cmake .. -DCURL_LIBRARY=/usr/lib/x86_64-linux-gnu/libcurl.so.4 -DCURL_INCLUDE_DIR=/usr/include/x86_64-linux-gnu/curl/ -DCMAKE_BUILD_TYPE=Release
cmake --build .
cmake --build . --target install

proj

# I'd like to run these but they seem to hang sometimes
#ctest -E nkg

projsync --system-directory --all

cd /tmp/gdal-proj/
rm -rf proj-8.2.1/

echo "Building & installing GDAL"

wget 'https://github.com/OSGeo/gdal/releases/download/v3.10.0/gdal-3.10.0.tar.gz'
tar xf gdal-3.10.0.tar.gz

rm gdal-3.10.0.tar.gz
cd gdal-3.10.0/

mkdir build
cd build/

cmake .. -DGDAL_BUILD_OPTIONAL_DRIVERS=OFF -DOGR_BUILD_OPTIONAL_DRIVERS=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build .
cmake --build . --target install

gdalinfo --version

cd /tmp
rm -rf gdal-proj/

echo "Done"
