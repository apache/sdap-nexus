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

__version__ = '1.5'

setuptools.setup(
    name="nexusanalysis",
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="NEXUS API.",
    long_description=open('README.md').read(),

    packages=['webservice', 'webservice.algorithms', 'webservice.algorithms.doms', 'webservice.algorithms_spark', 'webservice.algorithms.imaging'],
    package_data={'webservice': ['config/web.ini', 'config/algorithms.ini'],
                  'webservice.algorithms.doms': ['domsconfig.ini']},
    data_files=[
        ('static', ['static/index.html'])
    ],
    platforms='any',

    install_requires=[
        'nexus-data-access',
        'tornado',
        'singledispatch',
        'pytz',
        'cython',
        'requests',
        'utm',
        'shapely',
        'mock',
        'backports.functools-lru-cache==1.3',
        'netcdf4',
        'boto3',
        'pyproj==1.9.5.1',
        'pillow==5.0.0'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
