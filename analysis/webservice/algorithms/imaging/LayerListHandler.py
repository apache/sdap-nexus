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
import ConfigParser
import pkg_resources
import uuid

from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler


class LayerListResponse:

    def __init__(self, results):
        self.results = results

    def default_results_type(self):
        return "JSON"

    def toJson(self):
        return json.dumps(self.results, indent=4)


@nexus_handler
class LayerListHandler(BaseHandler):
    name = "LayerListHandler"
    path = "/imaging/layers"
    description = "Provides imagery layers metadata"
    params = {}

    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)
        self.imagery_config = ConfigParser.RawConfigParser()
        self.imagery_config.readfp(pkg_resources.resource_stream(__name__, "config.ini"), filename='config.ini')

    def _build_layer_spec(self, ds):
        layer = {
            "ProductLabel": ds["shortName"],
            "DataID": ds["shortName"],
            "CrmShortName": ds["shortName"],
            "EndPoint": "%s/wmts"%self.imagery_config.get("imaging", "imaging.endpoint"),
            "LayerTitle": ds["shortName"],
            "LayerSubtitle": "",
            "availableForAnalysis": True,
            "instrument": "",
            "mission": "",
            "parameter": "",
            "ProductType": "Mosaic",
            "keywords": [],
            "availability": {
                "start": ds["start"] * 1000,
                "end": ds["end"] * 1000
            },
            "wmts": {
                "tileLayerName": ds["shortName"],
                "tileMatrixSet": "EPSG4326",
                "tileFormat": "image/png"
            },
            "depths": None,
            "wmsLayer": 0,
            "bounding": {
                "eastbc": 180.0,
                "westbc": -180.0,
                "southbc": -90.0,
                "northbc": 90
            },
            "legend": "",
            "LayerProjection": "EPSG:4326",
            "LayerService": "wmtstiled",
            "utilityLayer": False,
            "UUID": str(uuid.uuid4()),
            "type": "observation",
            "enabled": True,
            "ThumbnailImage": "%s/thumbnail?ds=%s"%(self.imagery_config.get("imaging", "imaging.endpoint"), ds["shortName"]),
            "colorbar": "%s/colorbar?ds=%s&ct=rainbow" % (self.imagery_config.get("imaging", "imaging.endpoint"), ds["shortName"]),
            "ServiceProtocol": "GIBS",
            "WMSEndPoint": ""
        }
        return layer

    def calc(self, computeOptions, **args):
        ds_list = self._tile_service.get_dataseries_list()

        layers = []
        for ds in ds_list:
            layer = self._build_layer_spec(ds)
            layers.append(layer)

        return LayerListResponse({
            "Layers": {
                "Layer" : layers
            }
        })
