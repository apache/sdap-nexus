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

    def get_raw_data_array(self):

        nexus_tile = self._get_nexus_tile()
        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        return from_shaped_array(the_tile_data.variable_data)

    def get_lat_lon_time_data_meta(self):
        if self._get_nexus_tile().HasField('grid_tile'):
            grid_tile = self._get_nexus_tile().grid_tile

            grid_tile_data = np.ma.masked_invalid(from_shaped_array(grid_tile.variable_data))
            latitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.longitude))

            if len(grid_tile_data.shape) == 2:
                grid_tile_data = grid_tile_data[np.newaxis, :]

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in grid_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                if len(meta_array.shape) == 2:
                    meta_array = meta_array[np.newaxis, :]
                meta_data[name] = meta_array

            return latitude_data, longitude_data, np.array([grid_tile.time]), grid_tile_data, meta_data
        elif self._get_nexus_tile().HasField('swath_tile'):
            swath_tile = self._get_nexus_tile().swath_tile

            latitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.latitude)).reshape(-1)
            longitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.longitude)).reshape(-1)
            time_data = np.ma.masked_invalid(from_shaped_array(swath_tile.time)).reshape(-1)

            # Simplify the tile if the time dimension is the same value repeated
            if np.all(time_data == np.min(time_data)):
                time_data = np.array([np.min(time_data)])

            swath_tile_data = np.ma.masked_invalid(from_shaped_array(swath_tile.variable_data))

            tile_data = self._to_standard_index(swath_tile_data,
                                                (len(time_data), len(latitude_data), len(longitude_data)))

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in swath_tile.meta_data:
                name = meta_data_obj.name
                actual_meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                reshaped_meta_array = self._to_standard_index(actual_meta_array, tile_data.shape)
                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data
        else:
            raise NotImplementedError("Only supports grid_tile and swath_tile")

    @staticmethod
    def _to_standard_index(data_array, desired_shape):

        if desired_shape[0] == 1:
            reshaped_array = np.ma.masked_all((desired_shape[1], desired_shape[2]))
            row, col = np.indices(data_array.shape)

            reshaped_array[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array[
                row.flat, col.flat]
            reshaped_array.mask[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array.mask[
                row.flat, col.flat]
            reshaped_array = reshaped_array[np.newaxis, :]
        else:
            reshaped_array = np.ma.masked_all(desired_shape)
            row, col = np.indices(data_array.shape)

            reshaped_array[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array[
                row.flat, col.flat]
            reshaped_array.mask[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array.mask[
                row.flat, col.flat]

        return reshaped_array


class S3Proxy(object):
    def __init__(self, config):
        self.config = config
        self.__s3_bucketname = config.get("s3", "bucket")
        self.__s3_region = config.get("s3", "region")
        self.__s3 = boto3.resource('s3')
        self.__nexus_tile = None

    def fetch_nexus_tiles(self, *tile_ids):
        tile_ids = [uuid.UUID(str(tile_id)) for tile_id in tile_ids if
                    (isinstance(tile_id, str) or isinstance(tile_id, unicode))]
        res = []
        for tile_id in tile_ids:
            obj = self.__s3.Object(self.__s3_bucketname, str(tile_id))
            data = obj.get()['Body'].read()
            nexus_tile = NexusTileData(data, str(tile_id))
            res.append(nexus_tile)

        return res
