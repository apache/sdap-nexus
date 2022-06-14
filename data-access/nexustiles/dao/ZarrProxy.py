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

# IMPORTS HERE

import boto3
import nexusproto.DataTile_pb2 as nexusproto

from botocore import UNSIGNED
from botocore.client import Config

import xarray as xr
import fsspec, s3fs, json
import numpy as np

class NexusDataTile(object):
    __nexus_tile = None
    __data = None
    tile_id = None

    def __int__(self, data, _tile_id):
        if self.__data is None:
            self.__data = data

        if self.tile_id is None:
            self.tile_id = _tile_id

    def _get_nexus_tile(self):
        if self.__nexus_tile is None:
            raise NotImplementedError('Still need to implement Zarr arrays -> Nexus tile translation')
            #TODO: Init TileData object here (see: https://github.com/apache/incubator-sdap-nexusproto/blob/master/src/main/proto/DataTile.proto#L172)

        return self.__nexus_tile

    def get_lat_lon_time_data_meta(self):
        raise NotImplementedError('Still need to implement Zarr arrays -> Nexus tile translation')

class ZarrProxy(object):
    def __init__(self, config):
        self.config = config
        self.__s3_bucketname = config.get("s3", "bucket")
        self.__s3_region = config.get("s3", "region")
        self.__s3_public = config.getboolean("s3", "public", fallback=False)
        self.__s3 = boto3.resource('s3')
        self.__nexus_tile = None

    def fetch_nexus_tiles(self, *tile_ids):
        pass

    def list(self):
        s3 = s3fs.S3FileSystem(anon=self.__s3_public)

        return s3.ls(self.__s3_bucketname + '/' + self.config.get("s3", "key"))

    def cat(self):
        s3 = s3fs.S3FileSystem(anon=self.__s3_public)

        return s3.cat(self.__s3_bucketname + '/' + self.config.get("s3", "key"))

    def getZarrConfig(self, array_key):
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        if not array_key.endswith('/'):
            array_key = array_key + '/'

        zarrayobject = s3.get_object(Bucket=self.__s3_bucketname, Key=array_key + '.zarray')
        return json.loads(zarrayobject['Body'].read().decode('utf-8'))

    def getZarrMetadata(self, array_key):
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        if not array_key.endswith('/'):
            array_key = array_key + '/'

        zarrayobject = s3.get_object(Bucket=self.__s3_bucketname, Key=array_key + '.zattrs')
        return json.loads(zarrayobject['Body'].read().decode('utf-8'))

    #TODO: Determine how Zarr arrays/chunks will be addressed (Tues 6/14 mtg)