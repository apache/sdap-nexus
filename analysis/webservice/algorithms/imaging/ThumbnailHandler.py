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

import colortables
import numpy as np
import ConfigParser
import pkg_resources
import io

from webservice.NexusHandler import nexus_handler
from ResourceCachingHandler import ResourceCachingHandler
from ImageResult import ImageResult
import mapprocessing




@nexus_handler
class ThumbnailHandler(ResourceCachingHandler):
    name = "ThumbnailHandler"
    path = "/imaging/thumbnail"
    description = "Creates a map thumbnail"
    params = {

    }
    singleton = True

    def __init__(self):
        ResourceCachingHandler.__init__(self)
        self.imagery_config = ConfigParser.RawConfigParser()
        self.imagery_config.readfp(pkg_resources.resource_stream(__name__, "config.ini"), filename='config.ini')

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        color_table_identifier = computeOptions.get_argument("ct", "grayscale")
        color_table = colortables.get_color_table(color_table_identifier)

        interpolation = computeOptions.get_argument("interp", "nearest")

        force_min = computeOptions.get_float_arg("min", None)
        force_max = computeOptions.get_float_arg("max", None)

        width = np.min([512, computeOptions.get_int_arg("width", 256)])
        height = np.min([512, computeOptions.get_int_arg("height", 256)])

        no_cache = computeOptions.get_boolean_arg("no_cache", False)

        min_lat = 10
        max_lat = 50
        min_lon = -90
        max_lon = -50

        background = (0, 0, 0, 255)

        s3_key = "{ds}/{colortable}/thumbnail/{ds}-{width}-{height}-{min}-{max}.png".format(
            ds=ds,
            width=width,
            height=height,
            min=(force_min if force_min is not None else "x"),
            max=(force_max if force_max is not None else "x"),
            colortable=color_table_identifier
        )

        img_data = self._get_from_cache(s3_key) if not no_cache else None

        if img_data is None:
            stats = self._tile_service.get_dataset_overall_stats(ds)

            data_time_start = stats["start"]
            data_time_end = data_time_start + 86400.0

            img = mapprocessing.create_map(self._tile_service, (float(max_lat), float(min_lon), float(min_lat), float(max_lon)),
                                           ds, data_time_start, data_time_end, width, height, force_min, force_max,
                                           color_table, interpolation, background)
            img_data = io.BytesIO()
            img.save(img_data, format='PNG')
            img_data = img_data.getvalue()

            if not no_cache:
                self._put_to_cache(s3_key, img_data)

        return ImageResult(img_data)