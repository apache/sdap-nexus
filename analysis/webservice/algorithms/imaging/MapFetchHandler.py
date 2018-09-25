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

"""
NOTE: This code is an experimental proof-of-concept. The algorithms and methods have not yet been vetted.
"""

import json
import time
import colortables
import numpy as np

from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler

import io


import mapprocessing

@nexus_handler
class MapFetchHandler(BaseHandler):
    name = "MapFetchHandler"
    path = "/map"
    description = "Creates a map image"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "A supported dataset shortname identifier"
        },
        "t": {
            "name": "Time",
            "type": "int",
            "description": "Data observation date"
        },
        "output": {
            "name": "Output Format",
            "type": "string",
            "description": "Output format. Use 'PNG' for this endpoint"
        },
        "min": {
            "name": "Minimum Value",
            "type": "float",
            "description": "Minimum value to use when computing color scales"
        },
        "max": {
            "name": "Maximum Value",
            "type": "float",
            "description": "Maximum value to use when computing color scales"
        },
        "ct": {
            "name": "Color Table",
            "type": "string",
            "description": "Identifier of a supported color table"
        },
        "interp": {
            "name": "Interpolation filter",
            "type": "string",
            "description": "Interpolation filter to use when rescaling image data. Can be 'nearest', 'lanczos', 'bilinear', or 'bicubic'."
        },
        "width": {
            "name": "Width",
            "type": "int",
            "description": "Output image width (max: 8192)"
        },
        "height": {
            "name": "Height",
            "type": "int",
            "description": "Output image height (max: 8192)"
        }
    }
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        data_time_end = computeOptions.get_datetime_arg("t", None)
        if data_time_end is None:
            raise Exception("Missing 't' option for time")

        data_time_end = time.mktime(data_time_end.timetuple())
        data_time_start = data_time_end - 86400.0

        color_table_identifier = computeOptions.get_argument("ct", "grayscale")
        color_table = colortables.get_color_table(color_table_identifier)

        interpolation = computeOptions.get_argument("interp", "nearest")

        force_min = computeOptions.get_float_arg("min", None)
        force_max = computeOptions.get_float_arg("max", None)

        width = np.min([8192, computeOptions.get_int_arg("width", None)])
        height = np.min([8192, computeOptions.get_int_arg("height", None)])

        minLat = computeOptions.get_min_lat()
        maxLat = computeOptions.get_max_lat()
        minLon = computeOptions.get_min_lon()
        maxLon = computeOptions.get_max_lon()

        img = mapprocessing.create_map(self._tile_service, (float(maxLat), float(minLon), float(minLat), float(maxLon)), ds, data_time_start, data_time_end, width, height, force_min, force_max, color_table, interpolation)

        print "F"
        imgByteArr = io.BytesIO()

        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()
        print "G"

        class SimpleResult(object):
            def toJson(self):
                return json.dumps({"status": "Please specify output type as PNG."})

            def toImage(self):
                return imgByteArr

        return SimpleResult()
