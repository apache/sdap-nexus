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
from webservice.NexusHandler import nexus_handler
import io

import mapprocessing
import tilespecs
from datetime import datetime
from pytz import UTC

from ResourceCachingHandler import ResourceCachingHandler
from ImageResult import ImageResult


@nexus_handler
class WmtsTileServiceHandler(ResourceCachingHandler):
    name = "WmtsTileServiceHandler"
    path = "/imaging/wmts"
    description = "Emulates a WMTS service"
    params = {}

    singleton = True

    def __init__(self):
        ResourceCachingHandler.__init__(self)

    def calc(self, computeOptions, **args):

        ds = computeOptions.get_argument("layer")
        tilematrixset = computeOptions.get_argument("tilematrixset")
        format = computeOptions.get_argument("Format")
        tile_matrix = computeOptions.get_int_arg("TileMatrix")
        tile_col = computeOptions.get_int_arg("TileCol")
        tile_row = computeOptions.get_int_arg("TileRow")

        tile_date = computeOptions.get_argument("TIME", None)
        tile_date = datetime.strptime(tile_date, "%Y-%m-%d").replace(tzinfo=UTC)
        data_time_end = time.mktime(tile_date.timetuple())
        data_time_start = data_time_end - 86400.0

        color_table_identifier = computeOptions.get_argument("ct", "rainbow")
        color_table = colortables.get_color_table(color_table_identifier)

        interpolation = computeOptions.get_argument("interp", "nearest")
        force_min = computeOptions.get_float_arg("min", None)
        force_max = computeOptions.get_float_arg("max", None)

        no_cache = computeOptions.get_boolean_arg("no_cache", False)

        tms = tilespecs.TILE_MATRIX_SETS[tilematrixset]
        tm = tms.get_tile_matrix_at_level(tile_matrix)
        tile_tllr = tm.get_tllr_for_tile(tile_col, tile_row)

        s3_key = "{ds}/{colortable}/{tilematrixset}/{date}/{tile_matrix}/{ds}-{tilematrixset}-{tile_matrix}-{col}-{row}-{time}-{min}-{max}.png".format(
            ds=ds,
            tilematrixset=tilematrixset,
            tile_matrix=tile_matrix,
            col=tile_col,
            row=tile_row,
            time=data_time_end,
            min=(force_min if force_min is not None else "x"),
            max=(force_max if force_max is not None else "x"),
            date=int(data_time_end),
            colortable=color_table_identifier
        )

        img_data = self._get_from_cache(s3_key) if not no_cache else None

        if img_data is None:
            img = mapprocessing.create_map(self._tile_service, tile_tllr,
                                           ds, data_time_start, data_time_end, tm.tile_width, tm.tile_height, force_min, force_max, color_table, interpolation)

            img_data = io.BytesIO()
            img.save(img_data, format='PNG')
            img_data = img_data.getvalue()

            if not no_cache:
                self._put_to_cache(s3_key, img_data)

        return ImageResult(img_data)

