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
from subprocess import check_call, CalledProcessError

__version__ = '1.6'


try:
    check_call(['conda', 'install', '-y', '-c', 'conda-forge', '--file', 'conda-requirements.txt'])
except (CalledProcessError, IOError) as e:
    raise EnvironmentError("Error installing conda packages", e)


setuptools.setup(
    name="nexusanalysis",
    version=__version__,
    url="https://github.com/apache/incubator-sdap-nexus",

    author="Team Nexus",

    description="NEXUS API.",
    long_description=open('README.md').read(),
    packages=setuptools.find_packages(),
    #packages=[
    #    'webservice',
    #    'webservice.algorithms',
    #    'webservice.algorithms.doms',
    #    'webservice.algorithms_spark',
    #    'webservice.metrics',
    #    'webservice.webmodel',
    #    'webservice.tornado_nexus',
    #    'webservice.nexus_tornado',
    #    'webservice.nexus_tornado.request',
    #    'webservice.nexus_tornado.request.handlers',
    #    'webservice.nexus_tornado.request.renderers'
    #],
    package_data={
        'webservice': ['config/web.ini', 'config/algorithms.ini'],
        'webservice.algorithms.doms': ['domsconfig.ini.default']
    },
    data_files=[
        ('static', ['static/index.html'])
    ],
    platforms='any',
    python_requires='~=2.7',
    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
