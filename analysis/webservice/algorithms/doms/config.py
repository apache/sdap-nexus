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

ENDPOINTS = [
    {
        "name": "samos",
        "url": "http://doms.coaps.fsu.edu:8890/ws/search/samos",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 1000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SAMOS&format=umm-json"
    },
    {
        "name": "spurs",
        "url": "https://doms.jpl.nasa.gov/ws/search/spurs",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 25000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SPURS-1&format=umm-json"
    },
    {
        "name": "icoads",
        "url": "http://rda-data.ucar.edu:8890/ws/search/icoads",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 1000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=ICOADS&format=umm-json"
    },
    {
        "name": "spurs2",
        "url": "https://doms.jpl.nasa.gov/ws/search/spurs2",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 25000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SPURS-2&format=umm-json"
    }
]

METADATA_LINKS = {
    "samos": "http://samos.coaps.fsu.edu/html/nav.php?s=2",
    "icoads": "https://rda.ucar.edu/datasets/ds548.0/",
    "spurs": "https://podaac.jpl.nasa.gov/spurs",
    "spurs2": "https://podaac.jpl.nasa.gov/spurs?tab=spurs2-campaign",
}

import os

try:
    env = os.environ['ENV']
    if env == 'dev':
        ENDPOINTS = [
            {
                "name": "samos",
                "url": "http://doms.coaps.fsu.edu:8890/ws/search/samos",
                "fetchParallel": True,
                "fetchThreads": 8,
                "itemsPerPage": 1000,
                "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SAMOS&format=umm-json"
            },
            {
                "name": "spurs",
                "url": "http://127.0.0.1:8890/ws/search/spurs",
                "fetchParallel": True,
                "fetchThreads": 8,
                "itemsPerPage": 25000,
                "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SPURS-1&format=umm-json"
            },
            {
                "name": "icoads",
                "url": "http://rda-data.ucar.edu:8890/ws/search/icoads",
                "fetchParallel": True,
                "fetchThreads": 8,
                "itemsPerPage": 1000,
                "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=ICOADS&format=umm-json"
            },
            {
                "name": "spurs2",
                "url": "https://doms.jpl.nasa.gov/ws/search/spurs2",
                "fetchParallel": True,
                "fetchThreads": 8,
                "itemsPerPage": 25000,
                "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SPURS-2&format=umm-json"
            }
        ]
        METADATA_LINKS = {
            "samos": "http://samos.coaps.fsu.edu/html/nav.php?s=2",
            "icoads": "https://rda.ucar.edu/datasets/ds548.1/",
            "spurs": "https://podaac.jpl.nasa.gov/spurs"
        }
except KeyError:
    pass


def getEndpointByName(name):
    for endpoint in ENDPOINTS:
        if endpoint["name"].upper() == name.upper():
            return endpoint
    return None
