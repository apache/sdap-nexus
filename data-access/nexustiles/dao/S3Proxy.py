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

import uuid

import boto3
import nexusproto.DataTile_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array
from .NexusTileDataBase import NexusTileDataBase


class NexusTileData(object):
    __nexus_tile = None
    __data = None
    tile_id = None

    def __init__(self, data, _tile_id):
        if self.__data is None:
            self.__data = data
        if self.tile_id is None:
            self.tile_id = _tile_id

    def _get_nexus_tile(self):
        if self.__nexus_tile is None:
            self.__nexus_tile = nexusproto.TileData.FromString(self.__data)

        return self.__nexus_tile


class S3Proxy(object):
    def __init__(self, config):
        self.config = config
        self.__s3_bucketname = config.get("s3", "bucket")
        self.__s3_region = config.get("s3", "region")
        self.__s3 = boto3.resource('s3')
        self.__nexus_tile = None

    def fetch_nexus_tiles(self, *tile_ids):
        tile_ids = [uuid.UUID(str(tile_id)) for tile_id in tile_ids if
                    (isinstance(tile_id, str) or isinstance(tile_id, str))]
        res = []
        for tile_id in tile_ids:
            obj = self.__s3.Object(self.__s3_bucketname, str(tile_id))
            data = obj.get()['Body'].read()
            nexus_tile = NexusTileData(data, str(tile_id))
            res.append(nexus_tile)

        return res
