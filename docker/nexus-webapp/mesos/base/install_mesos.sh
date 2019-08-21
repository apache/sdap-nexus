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

# Install a few utility tools
yum install -y tar wget git

# Fetch the Apache Maven repo file.
wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo

# Install the EPEL repo so that we can pull in 'libserf-1' as part of our
# subversion install below.
yum install -y epel-release

# 'Mesos > 0.21.0' requires 'subversion > 1.8' devel package,
# which is not available in the default repositories.
# Create a WANdisco SVN repo file to install the correct version:
bash -c 'cat > /etc/yum.repos.d/wandisco-svn.repo <<EOF
[WANdiscoSVN]
name=WANdisco SVN Repo 1.9
enabled=1
baseurl=http://opensource.wandisco.com/centos/7/svn-1.9/RPMS/\$basearch/
gpgcheck=1
gpgkey=http://opensource.wandisco.com/RPM-GPG-KEY-WANdisco
EOF'

# Parts of Mesos require systemd in order to operate. However, Mesos
# only supports versions of systemd that contain the 'Delegate' flag.
# This flag was first introduced in 'systemd version 218', which is
# lower than the default version installed by centos. Luckily, centos
# 7.1 has a patched 'systemd < 218' that contains the 'Delegate' flag.
# Explicity update systemd to this patched version.
yum update systemd

# Install essential development tools.
yum groupinstall -y "Development Tools"

# Install other Mesos dependencies.
yum install -y apache-maven python-devel java-1.8.0-openjdk-devel zlib-devel libcurl-devel openssl-devel cyrus-sasl-devel cyrus-sasl-md5 apr-devel subversion-devel apr-util-devel

# Retrieve MESOS package
wget --quiet http://www.apache.org/dist/mesos/${MESOS_VER}/${MESOS_PACKAGE}
tar -zxf ${MESOS_PACKAGE} -C ${INSTALL_LOC}
rm -f ${MESOS_PACKAGE}

# Configure and build.
mkdir -p ${MESOS_WORKDIR}
mkdir -p ${MESOS_HOME}
pushd ${MESOS_HOME}
mkdir build
pushd build
echo ${JAVA_HOME}
../configure
make
# Can't run make check until this is fixed: https://issues.apache.org/jira/browse/MESOS-8608
# make check
make install
popd
popd
