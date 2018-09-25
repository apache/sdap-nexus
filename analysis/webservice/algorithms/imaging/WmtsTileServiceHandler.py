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
import os
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusResults
import io
import boto3
import botocore
import mapprocessing
import tilespecs
import datetime
import tempfile
from pytz import UTC, timezone

class TileResult:

    def __init__(self, meta, img):
        self.meta = meta
        self.img = img

    def default_results_type(self):
        return "PNG"

    def toJson(self):
        return json.dumps(self.meta, indent=4)

    def toImage(self):
        return self.img


@nexus_handler
class WmtsTileServiceHandler(BaseHandler):
    name = "WmtsTileServiceHandler"
    path = "/wmts"
    description = "Emulates a WMTS service"
    params = {}

    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)
        self.__s3_bucketname = "sea-level-mrf"
        self.__s3_region = "us-east-2"
        self.__s3 = boto3.resource('s3')


    def upload_tile_to_s3(self, key, img):
        temp = tempfile.NamedTemporaryFile(delete=False)
        temp.write(img)
        temp.close()

        s3 = boto3.client('s3')

        s3.upload_file(temp.name, self.__s3_bucketname, key)

        os.unlink(temp.name)

    def fetch_tile_from_s3(self, key):
        s3 = boto3.client('s3')

        temp = tempfile.TemporaryFile()

        try:
            s3.download_fileobj(self.__s3_bucketname, key, temp)
            temp.seek(0)
            data = temp.read()
            return data
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None
            else:
                raise

    def calc(self, computeOptions, **args):

        ds = computeOptions.get_argument("layer")
        tilematrixset = computeOptions.get_argument("tilematrixset")
        format = computeOptions.get_argument("Format")
        tile_matrix = computeOptions.get_int_arg("TileMatrix")
        tile_col = computeOptions.get_int_arg("TileCol")
        tile_row = computeOptions.get_int_arg("TileRow")

        tile_date = computeOptions.get_argument("TIME", None)
        tile_date = datetime.datetime.strptime(tile_date, "%Y-%m-%d").replace(tzinfo=UTC)
        data_time_end = time.mktime(tile_date.timetuple())
        data_time_start = data_time_end - 86400.0

        color_table_identifier = computeOptions.get_argument("ct", "rainbow")
        color_table = colortables.get_color_table(color_table_identifier)

        interpolation = computeOptions.get_argument("interp", "nearest")
        force_min = computeOptions.get_float_arg("min", None)
        force_max = computeOptions.get_float_arg("max", None)

        tms = tilespecs.TILE_MATRIX_SETS[tilematrixset]
        tm = tms.get_tile_matrix_at_level(tile_matrix)
        tile_tllr = tm.get_tllr_for_tile(tile_col, tile_row)

        s3_key = "{ds}/{colortable}/{tilematrixset}/{tile_matrix}/{ds}-{tilematrixset}-{tile_matrix}-{col}-{row}-{time}.png".format(
            ds=ds,
            tilematrixset=tilematrixset,
            tile_matrix=tile_matrix,
            col=tile_col,
            row=tile_row,
            time=data_time_end,
            colortable=color_table_identifier
        )

        img_data = self.fetch_tile_from_s3(s3_key)

        if img_data is None:
            img = mapprocessing.create_map(self._tile_service, tile_tllr,
                                           ds, data_time_start, data_time_end, tm.tile_width, tm.tile_height, force_min, force_max, color_table, interpolation)

            img_data = io.BytesIO()
            img.save(img_data, format='PNG')
            img_data = img_data.getvalue()

            self.upload_tile_to_s3(s3_key, img_data)

        meta = {
            "ds":ds,
            "tilematrixset": tilematrixset,
            "format": format,
            "tile_matrix": tile_matrix,
            "tile_col": tile_col,
            "tile_row": tile_row,
            "tile_tllr": tile_tllr,
            "interp": interpolation,
            "min": force_min,
            "max": force_max,
            "time": data_time_start,
            "colortable": color_table_identifier
        }

        return TileResult(meta, img_data)

