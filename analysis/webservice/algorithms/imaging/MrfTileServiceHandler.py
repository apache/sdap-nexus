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


import time
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import NexusHandler as BaseHandler
import io

from datetime import datetime
from pytz import UTC

from ImageResult import ImageResult
import ConfigParser
import pkg_resources
from mrf_reader import MrfReader

@nexus_handler
class MrfTileServiceHandler(BaseHandler):
    name = "MrfTileServiceHandler"
    path = "/imaging/wmts_static"
    description = "Emulates a WMTS service"
    params = {}

    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)
        self._imagery_config = ConfigParser.RawConfigParser()
        self._imagery_config.readfp(pkg_resources.resource_stream(__name__, "config.ini"), filename='config.ini')

        self._imagery_base = self._imagery_config.get("tileserver", "mrf.base")

    def calc(self, computeOptions, **args):

        ds = computeOptions.get_argument("layer")
        tilematrixset = computeOptions.get_argument("tilematrixset")
        format = computeOptions.get_argument("Format")
        tile_matrix = computeOptions.get_int_arg("TileMatrix")
        tile_col = computeOptions.get_int_arg("TileCol")
        tile_row = computeOptions.get_int_arg("TileRow")

        mrf_path = "%s/%s/%s/%s.mrf"%(self._imagery_base, tilematrixset, ds, ds)
        mrf = MrfReader(mrf_path)

        image_bytes = mrf.get_tile_bytes(tile_matrix, tile_row, tile_col)

        return ImageResult(image_bytes)




