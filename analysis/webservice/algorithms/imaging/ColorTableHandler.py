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
import colortables
import numpy as np

from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler


class ColorTableResponse:

    def __init__(self, results):
        self.results = results

    def default_results_type(self):
        return "JSON"

    def toJson(self):
        return json.dumps(self.results, indent=4)


@nexus_handler
class ColorTableHandler(BaseHandler):
    name = "ColorTableHandler"
    path = "/imaging/colortable"
    description = "Provides imagery color table specifications"
    params = {}

    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    @staticmethod
    def _create_hex_color_list(colortable, num_colors=255):
        colors = []

        for i in range(0, num_colors):
            rgba = colortable.get_color(float(i) / float(num_colors))
            colors.append('%02x%02x%02x%02x' % (rgba[0], rgba[1], rgba[2], rgba[3]))

        return colors

    @staticmethod
    def _create_labels(min_value, max_value, units, num_labels=255):
        labels = []
        for i in range(0, num_labels):
            f = float(i) / float(num_labels)
            value = f * (max_value - min_value) + min_value
            labels.append("%3.2f %s" % (value, units))

        return labels

    @staticmethod
    def _create_values(min_value, max_value, num_values=255):
        values = []
        for i in range(0, num_values):
            f = float(i) / float(num_values)
            value = f * (max_value - min_value) + min_value
            value_high = float(i + 1) / float(num_values) * (max_value - min_value) + min_value
            values.append((value, value_high))
        return values

    def _create_colortable(self, color_table, min_value, max_value, num_items, units):
        colors = ColorTableHandler._create_hex_color_list(color_table, num_items)
        labels = ColorTableHandler._create_labels(min_value, max_value, units, num_items)
        values = ColorTableHandler._create_values(min_value, max_value, num_items)
        return colors, labels, values

    def _create_from_sdap(self, ds, color_table, force_min, force_max, num_items, units):
        stats = self._tile_service.get_dataset_overall_stats(ds)
        min_value = force_min if force_min is not None else stats["minValue"]
        max_value = force_max if force_max is not None else stats["maxValue"]
        return self._create_colortable(color_table, min_value, max_value, num_items, units)

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds")

        generic = computeOptions.get_boolean_arg("generic", False)

        color_table_identifier = computeOptions.get_argument("ct", "rainbow")
        color_table = colortables.get_color_table(color_table_identifier)

        force_min = computeOptions.get_float_arg("min", None)
        force_max = computeOptions.get_float_arg("max", None)

        num_items = computeOptions.get_int_arg("n", 255)
        num_items = np.array((2048, num_items)).min()
        num_items = np.array((255, num_items)).max()

        units = computeOptions.get_argument("units", "&deg;c")

        if generic is False:
            colors, labels, values = self._create_from_sdap(ds, color_table, force_min, force_max, num_items, units)
        else:
            colors, labels, values = self._create_colortable(color_table, force_min, force_max, num_items, units)

        return ColorTableResponse({
            "units": units,
            "scale": {
                "colors": colors,
                "labels": labels,
                "values": values
            },
            "id": ds
        })

