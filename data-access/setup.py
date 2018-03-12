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

import setuptools
from Cython.Build import cythonize

__version__ = '0.32'

setuptools.setup(
    name="nexus-data-access",
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="NEXUS API.",
    long_description=open('README.md').read(),

    packages=['nexustiles', 'nexustiles.model', 'nexustiles.dao'],
    package_data={'nexustiles': ['config/datastores.ini']},
    platforms='any',
    setup_requires=['cython'],
    install_requires=[
        'cassandra-driver==3.5.0',
        'solrpy==0.9.7',
        'requests',
        'nexusproto==1.0.0-SNAPSHOT',
        'shapely'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ],

    ext_modules=cythonize(["**/*.pyx"]),
    zip_safe=False
)
