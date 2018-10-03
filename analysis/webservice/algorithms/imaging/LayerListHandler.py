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
import sys
import uuid
import json
import hashlib
import layer

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

        self.static_layers_base = self.imagery_config.get("tileserver", "mrf.base")

        self.parse_static_layers()

    def build_layer_for_projection(self, layer_def, product_suffix, endpoint_proj, layerProjection, tileMatrixSet):
        layer = {}
        layer["UUID"] = str(uuid.uuid4())
        layer["DataID"] = layer_def["productLabel"]
        layer["ProductLabel"] = "%s%s" % (layer_def["productLabel"], product_suffix)
        layer["CrmShortName"] = layer_def["crmShortName"] if layer_def["crmShortName"] is not None else layer_def[
            "productLabel"]
        layer["enabled"] = layer_def["enabled"]
        layer["mission"] = layer_def["mission"]
        layer["instrument"] = layer_def["instrument"]
        layer["parameter"] = layer_def["parameter"]
        layer["ProductType"] = layer_def["productType"]
        layer["ServiceProtocol"] = layer_def["serviceProtocol"]
        layer["EndPoint"] = layer_def["endpoint"].format(proj=endpoint_proj)
        layer["WMSEndPoint"] = ""
        layer["wmsLayer"] = "0"
        layer["LayerTitle"] = layer_def["layerTitle"]
        layer["LayerService"] = layer_def["layerService"]
        layer["LayerProjection"] = layerProjection
        layer["availableForAnalysis"] = layer_def["availableForAnalysis"]
        layer["analysisDatasetName"] = layer_def["analysisDatasetName"]
        layer["ThumbnailImage"] = layer_def["thumbnailImage"]
        layer["bounding"] = {
            "westbc": layer_def["bounding"]["west"],
            "eastbc": layer_def["bounding"]["east"],
            "northbc": layer_def["bounding"]["north"],
            "southbc": layer_def["bounding"]["south"]
        }
        layer["LayerSubtitle"] = layer_def["layerSubtitle"]
        layer["legend"] = ""
        layer["wmts"] = {
            "tileMatrixSet": tileMatrixSet,
            "tileLayerName": layer_def["wmtsLayerName"],
            "tileFormat": layer_def["wmtsFormat"]
        }
        layer["colorbar"] = layer_def["colorbar"]
        layer["nativeResolution"] = "9km"
        if layer_def["availability"] is not None:
            layer["availability"] = {
                "start": layer_def["availability"]["start"],
                "end": layer_def["availability"]["end"]
            }
        else:
            layer["availability"] = None
        layer["keywords"] = layer_def["keywords"]
        layer["utilityLayer"] = layer_def["utilityLayer"]
        layer["depths"] = layer_def["depthSpec"]
        layer["isGrayscale"] = layer_def["isGrayscale"]
        layer["type"] = layer_def["type"]
        return layer

    def build_layer(self, layer_def, basemap):
        layers = []
        if layer_def["hasGlobal"] is True and basemap == layer_def["baseLayer"]:
            layers.append(self.build_layer_for_projection(layer_def,
                                                     "",
                                                     "geo",
                                                     layer_def["layerProjectionGlobal"],
                                                     layer_def["wmtsGlobalMatrixSet"]))
        if layer_def["hasNorth"] is True and basemap == layer_def["baseLayer"]:
            layers.append(self.build_layer_for_projection(layer_def,
                                                     "-arctic",
                                                     "arctic",
                                                     layer_def["layerProjectionNorth"],
                                                     layer_def["wmtsNorthMatrixSet"]))
        if layer_def["hasSouth"] is True and basemap == layer_def["baseLayer"]:
            layers.append(self.build_layer_for_projection(layer_def,
                                                     "-antarctic",
                                                     "antarctic",
                                                     layer_def["layerProjectionSouth"],
                                                     layer_def["wmtsSouthMatrixSet"]))
        return layers

    def build_layer_config(self, layer_list, basemap):
        compiled_layers = []

        for entry in layer_list:
            compiled_layers += self.build_layer(entry, basemap)

        return compiled_layers

    def parse_static_layers(self):
        sys.path.insert(0, self.static_layers_base)
        import layers




    def _build_layer_spec(self, ds):
        layer = {
            "ProductLabel": ds["shortName"],
            "DataID": ds["shortName"],
            "CrmShortName": ds["shortName"],
            "EndPoint": "%s/imaging/wmts"%self.imagery_config.get("imaging", "imaging.endpoint"),
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
            "ThumbnailImage": "%s/imaging/thumbnail?ds=%s&ct=rainbow"%(self.imagery_config.get("imaging", "imaging.endpoint"), ds["shortName"]),
            "colorbar": "%s/imaging/colortable?ds=%s&ct=rainbow" % (self.imagery_config.get("imaging", "imaging.endpoint"), ds["shortName"]),
            "ServiceProtocol": "GIBS",
            "WMSEndPoint": ""
        }
        return layer

    def calc(self, computeOptions, **args):
        basemaps = computeOptions.get_boolean_arg("basemaps", False)

        layers = []

        if basemaps is False:
            ds_list = self._tile_service.get_dataseries_list()
            for ds in ds_list:
                sdap_layer = self._build_layer_spec(ds)
                layers.append(sdap_layer)

        compiled_layers = self.build_layer_config(layer.getLayers(), basemaps)
        layers += compiled_layers

        return LayerListResponse({
            "Layers": {
                "Layer" : layers
            }
        })
