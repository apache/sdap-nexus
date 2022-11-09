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
import traceback

import requests

from . import BaseDomsHandler
from . import config
from . import values
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler as BaseHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import cached


@nexus_handler
class DomsDatasetListQueryHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "DOMS Dataset Listing"
    path = "/domslist"
    description = ""
    params = {}
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        BaseHandler.__init__(self, tile_service_factory=tile_service_factory)

    def getFacetsForInsituSource(self, source):
        url = source["url"]

        params = {
            "facet": "true",
            "stats": "true",
            "startIndex": 0,
            "itemsPerPage": 0
        }
        try:
            r = requests.get(url, params=params)
            results = json.loads(r.text)

            depths = None
            if "stats_fields" in results and "depth" in results["stats_fields"]:
                depths = results["stats_fields"]["depth"]

            for facet in results["facets"]:
                field = facet["field"]
                for value in facet["values"]:
                    value["value"] = values.getDescByListNameAndId(field, int(value["value"]))

            return depths, results["facets"]
        except:  # KMG: Don't eat the exception. Add better handling...
            traceback.print_exc()
            return None, None

    def getMetadataUrlForDataset(self, dataset):
        datasetSpec = config.getEndpointByName(dataset)
        if datasetSpec is not None:
            return datasetSpec["metadataUrl"]
        else:

            # KMG: NOT a good hack
            if dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1" or dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1_CLIM":
                dataset = "MUR-JPL-L4-GLOB-v4.1"
            elif dataset == "SMAP_L2B_SSS":
                dataset = "JPL_SMAP-SSS_L2_EVAL-V2"
            elif dataset == "AVHRR_OI_L4_GHRSST_NCEI" or dataset == "AVHRR_OI_L4_GHRSST_NCEI_CLIM":
                dataset = "AVHRR_OI-NCEI-L4-GLOB-v2.0"

            return "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=%s&format=umm-json" % dataset

    def getMetadataForSource(self, dataset):
        try:
            r = requests.get(self.getMetadataUrlForDataset(dataset))
            results = json.loads(r.text)
            return results
        except:
            return None

    @cached(ttl=(60 * 60 * 1000))  # 1 hour cached
    def calc(self, computeOptions, **args):

        satellitesList = self._get_tile_service().get_dataseries_list(simple=True)

        insituList = []

        for satellite in satellitesList:
            satellite["metadata"] = self.getMetadataForSource(satellite["shortName"])

        for insitu in config.INSITU_PROVIDER_MAP:
            provider = insitu['name']
            for project in insitu['projects']:
                project_name = project['name']
                insituList.append({
                    'name': project_name,
                    'endpoint': f'{config.INSITU_API_ENDPOINT}?provider={provider}&project={project_name}',
                    'metadata': {
                        'Projects': [
                            {
                                'ShortName': project_name,
                                'LongName': project_name
                            }
                        ]
                    }
                })

        values = {
            "satellite": satellitesList,
            "insitu": insituList
        }

        return BaseDomsHandler.DomsQueryResults(results=values)
