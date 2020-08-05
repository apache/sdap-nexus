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

import json

import requests

import BaseDomsHandler
import config
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler as BaseHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import DatasetNotFoundException


@nexus_handler
class DomsMetadataQueryHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "DOMS Metadata Listing"
    path = "/domsmetadata"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):

        dataset = computeOptions.get_argument("dataset", None)
        if dataset is None or len(dataset) == 0:
            raise Exception("'dataset' parameter not specified")

        metadataUrl = self.__getUrlForDataset(dataset)

        try:
            r = requests.get(metadataUrl)
            results = json.loads(r.text)
            return BaseDomsHandler.DomsQueryResults(results=results)
        except:
            raise DatasetNotFoundException("Dataset '%s' not found")

    def __getUrlForDataset(self, dataset):
        datasetSpec = config.getEndpointByName(dataset)
        if datasetSpec is not None:
            return datasetSpec["metadataUrl"]
        else:

            # KMG: NOT a good hack
            if dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1" or dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1_CLIM":
                dataset = "MUR-JPL-L4-GLOB-v4.1"
            elif dataset == "SMAP_L2B_SSS":
                dataset = "JPL_SMAP-SSS_L2_EVAL-V2"

            return "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=%s&format=umm-json" % dataset
