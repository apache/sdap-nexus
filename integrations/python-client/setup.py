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


from setuptools import setup, find_packages

__version__ = '1.0'

setup(
    name="nexuscli",
    version=__version__,
    packages=find_packages(),
    url="https://github.com/apache/incubator-sdap-nexus",

    author="Apache SDAP",
    author_email="dev@sdap.apache.org",

    description="NEXUS Client Module",
    long_description=open('README.md').read(),

    platforms='any',

    install_requires=[
        'requests',
        'shapely',
        'numpy',
        'pytz'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
    ]
)
