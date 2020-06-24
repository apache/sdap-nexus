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
import math
import time

import numpy as np

import colortables
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler as BaseHandler
from webservice.NexusHandler import nexus_handler


@nexus_handler
class ColorBarCalcHandler(BaseHandler):
    name = "ColorBarHandler"
    path = "/colorbar"
    description = "Creates a CMC colorbar spec for a dataset"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "A supported dataset shortname identifier"
        },
        "t": {
            "name": "Time",
            "type": "int",
            "description": "Data observation date if not specifying a min/max"
        },
        "min": {
            "name": "Minimum Value",
            "type": "float",
            "description": "Minimum value to use when computing color scales. Will be computed if not specified"
        },
        "max": {
            "name": "Maximum Value",
            "type": "float",
            "description": "Maximum value to use when computing color scales. Will be computed if not specified"
        },
        "ct": {
            "name": "Color Table",
            "type": "string",
            "description": "Identifier of a supported color table"
        }
    }
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def __get_dataset_minmax(self, ds, dataTime):
        dataTimeStart = dataTime - 86400.0  # computeOptions.get_datetime_arg("t", None)
        dataTimeEnd = dataTime

        daysinrange = self._get_tile_service().find_days_in_range_asc(-90.0, 90.0, -180.0, 180.0, ds, dataTimeStart,
                                                                dataTimeEnd)

        ds1_nexus_tiles = self._get_tile_service().get_tiles_bounded_by_box_at_time(-90.0, 90.0, -180.0, 180.0,
                                                                              ds,
                                                                              daysinrange[0])

        data_min = 100000
        data_max = -1000000

        for tile in ds1_nexus_tiles:
            data_min = np.min((data_min, np.ma.min(tile.data)))
            data_max = np.max((data_max, np.ma.max(tile.data)))

        return data_min, data_max

    def __produce_color_list(self, colorbarDef, numColors, min, max, units):
        colors = []
        labels = []
        values = []
        for i in range(0, numColors):
            index = float(i) / float(numColors)
            index = index * (len(colorbarDef) - 1)
            prev = int(math.floor(index))
            next = int(math.ceil(index))
            f = index - prev
            prevColor = colorbarDef[prev]
            nextColor = colorbarDef[next]

            color = [0, 0, 0, 255]
            color[0] = nextColor[0] * f + (prevColor[0] * (1.0 - f))
            color[1] = nextColor[1] * f + (prevColor[1] * (1.0 - f))
            color[2] = nextColor[2] * f + (prevColor[2] * (1.0 - f))

            colors.append('%02x%02x%02xFF' % (color[0], color[1], color[2]))

            value = (float(i) / float(numColors - 1)) * (max - min) + min
            valueHigh = (float(i + 1) / float(numColors - 1)) * (max - min) + min
            labels.append("%3.2f %s" % (value, units))
            values.append((value, valueHigh))

        return colors, labels, values

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        dataTime = computeOptions.get_datetime_arg("t", None)
        if dataTime is None:
            raise Exception("Missing 't' option for time")

        dataTime = time.mktime(dataTime.timetuple())

        color_table_name = computeOptions.get_argument("ct", "smap")
        color_table = colortables.__dict__[color_table_name]

        min = computeOptions.get_float_arg("min", np.nan)
        max = computeOptions.get_float_arg("max", np.nan)

        num_colors = computeOptions.get_int_arg("num", 255)

        units = computeOptions.get_argument("units", "")

        if np.isnan(min) or np.isnan(max):
            data_min, data_max = self.__get_dataset_minmax(ds, dataTime)

            if np.isnan(min):
                min = data_min
            if np.isnan(max):
                max = data_max

        colors, labels, values = self.__produce_color_list(color_table, num_colors, min, max, units)

        obj = {
            "scale": {
                "colors": colors,
                "labels": labels,
                "values": values
            },
            "id": ds
        }

        class SimpleResult(object):
            def toJson(self):
                return json.dumps(obj, indent=4)

        return SimpleResult()
