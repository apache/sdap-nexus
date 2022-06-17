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

import boto3
import nexusproto.DataTile_pb2 as nexusproto
from nexusproto.serialization import to_shaped_array, from_shaped_array

from botocore import UNSIGNED
from botocore.client import Config

import xarray as xr
import numpy as np

import s3fs, json

class NexusDataTile(object):
    __nexus_tile = None
    __data = None
    tile_id = None

    chunk_coords= None
    zarr_cfg = None

    def __init__(self, data, _tile_id, zarr_cfg):
        import re

        if self.__data is None:
            self.__data = data

        if self.tile_id is None:
            self.tile_id = _tile_id

        if not re.search("^MUR_[0-9-T]*_[0-9]*_[0-9]", self.tile_id):
            raise ValueError("Bad tile id")

        c = re.split("_", self.tile_id)

        self.chunk_coords = {
            'time': self._get_time_index(c[1]),
            'lat_chunk': int(c[2]),
            'lon_chunk': int(c[3])
        }

        self.zarr_cfg = zarr_cfg

    def _get_time_index(self, time):
        if not isinstance(time, np.datetime64):
            time = np.datetime64(time)

        min_delta = None
        min_td = None

        for i in range(0, len(self.__data.time) - 1):
            t = np.array([self.__data.time.item(i)], dtype='datetime64[ns]')[0]
            delta = abs(time - t)

            if min_delta is None or delta < min_delta:
                min_delta = delta
                min_td = i

        return min_td

    def _get_nexus_tile(self):
        if self.__nexus_tile is None:
            tile = nexusproto.TileData()

            tile.tile_id = self.tile_id

            metadata = self.__data.attrs

            tile_type = 'grid_tile'

            if tile_type == 'grid_tile':
                tdata = nexusproto.GridTile()

                tdata.latitude = to_shaped_array(self.__data.lat)
                tdata.longitude = to_shaped_array(self.__data.lon)
                tdata.time = self.__data.time[self.chunk_coords['time']]

                min_lat = self.chunk_coords['lat_chunk'] * self.zarr_cfg['chunks'][1]
                max_lat = min_lat + self.zarr_cfg['chunks'][1] - 1

                min_lon = self.chunk_coords['lon_chunk'] * self.zarr_cfg['chunks'][2]
                max_lon = min_lon + self.zarr_cfg['chunks'][2] - 1

                tdata.variable_data = to_shaped_array(self.__data.analysed_sst[self.chunk_coords['time'], min_lat:max_lat, min_lon:max_lon]) #POC for MUR data

                tdata.meta_data = metadata

                tile.grid_tile = tdata
            else:
                raise NotImplementedError("Only supports grid_tile")

            self.__nexus_tile = tile

        return self.__nexus_tile

    def get_raw_data_array(self):
        pass

    def get_lat_lon_time_data_meta(self):
        isMultiVar = True

        # Following code should eventually be wrapped in a check for tile format
        # Copied from CassandraProxy/S3Proxy
        grid_tile = self._get_nexus_tile().grid_tile

        grid_tile_data = np.ma.masked_invalid(from_shaped_array(grid_tile.variable_data))
        latitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.latitude))
        longitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.longitude))

        if len(grid_tile_data.shape) == 2:
            grid_tile_data = grid_tile_data[np.newaxis, :]

        return latitude_data, longitude_data, np.array(grid_tile.time), grid_tile_data, grid_tile.meta_data, isMultiVar

class ZarrProxy(object):
    def __init__(self, config):
        self.config = config
        self.__s3_bucketname = config.get("s3", "bucket")
        self.__s3_region = config.get("s3", "region")
        self.__s3_public = config.getboolean("s3", "public", fallback=False)
        self.__s3 = boto3.resource('s3')
        self.__nexus_tile = None

    #Interpreting tile id's as: MUR_<time>_<lat_chunk>_<lon_chunk>
    def fetch_nexus_tiles(self, *tile_ids):
        s3path = f"s3://{self.__s3_bucketname}/{self.config.get('s3', 'key')}/"
        s3 = s3fs.S3FileSystem(self.__s3_public)
        store = s3fs.S3Map(root=s3path, s3=s3, check=False)

        zarr_data = xr.open_zarr(store=store, consolidated=True)

        res = []

        zarr_config = self.getZarrConfig(self.config.get("s3", "key") + "analysed_sst")

        for tid in tile_ids:
            nexus_tile = NexusDataTile(zarr_data, tid, zarr_config)
            res.append(nexus_tile)

        return res

    def getZarrConfig(self, array_key):
        s3 = None

        if self.__s3_public:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        else:
            pass  # configure s3 w/ auth data (or perhaps it's in ~/.aws/credentials???)

        if not array_key.endswith('/'):
            array_key = array_key + '/'

        zarrayobject = s3.get_object(Bucket=self.__s3_bucketname, Key=array_key + '.zarray')
        return json.loads(zarrayobject['Body'].read().decode('utf-8'))

    def getZarrMetadata(self, array_key):
        s3 = None

        if self.__s3_public:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        else:
            pass  # configure s3 w/ auth data (or perhaps it's in ~/.aws/credentials???)

        if not array_key.endswith('/'):
            array_key = array_key + '/'

        zarrayobject = s3.get_object(Bucket=self.__s3_bucketname, Key=array_key + '.zattrs')
        return json.loads(zarrayobject['Body'].read().decode('utf-8'))
