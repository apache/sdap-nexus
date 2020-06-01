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
from ConfigParser import NoOptionError
from multiprocessing.synchronize import Lock

import nexusproto.DataTile_pb2 as nexusproto
import numpy as np
from cassandra.cqlengine import columns, connection, CQLEngineException
from cassandra.cqlengine.models import Model
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, WhiteListRoundRobinPolicy
from nexusproto.serialization import from_shaped_array

INIT_LOCK = Lock()


class NexusTileData(Model):
    __table_name__ = 'sea_surface_temp'
    tile_id = columns.UUID(primary_key=True)
    tile_blob = columns.Blob()

    __nexus_tile = None

    def _get_nexus_tile(self):
        if self.__nexus_tile is None:
            self.__nexus_tile = nexusproto.TileData.FromString(self.tile_blob)

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
        elif self._get_nexus_tile().HasField('time_series_tile'):
            time_series_tile = self._get_nexus_tile().time_series_tile

            time_series_tile_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.variable_data))
            time_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.time)).reshape(-1)
            latitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.longitude))

            reshaped_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
            idx = np.arange(len(latitude_data))
            reshaped_array[:, idx, idx] = time_series_tile_data
            tile_data = reshaped_array
            # Extract the meta data
            meta_data = {}
            for meta_data_obj in time_series_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))

                reshaped_meta_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
                idx = np.arange(len(latitude_data))
                reshaped_meta_array[:, idx, idx] = meta_array

                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data
        else:
            raise NotImplementedError("Only supports grid_tile, swath_tile, and time_series_tile")

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


class CassandraProxy(object):
    def __init__(self, config):
        self.config = config
        self.__cass_url = config.get("cassandra", "host")
        self.__cass_keyspace = config.get("cassandra", "keyspace")
        self.__cass_local_DC = config.get("cassandra", "local_datacenter")
        self.__cass_protocol_version = config.getint("cassandra", "protocol_version")
        self.__cass_dc_policy = config.get("cassandra", "dc_policy")

        try:
            self.__cass_port = config.getint("cassandra", "port")
        except NoOptionError:
            self.__cass_port = 9042

        with INIT_LOCK:
            try:
                connection.get_cluster()
            except CQLEngineException:
                self.__open()

    def __open(self):

        if self.__cass_dc_policy == 'DCAwareRoundRobinPolicy':
            dc_policy = DCAwareRoundRobinPolicy(self.__cass_local_DC)
        elif self.__cass_dc_policy == 'WhiteListRoundRobinPolicy':
            dc_policy = WhiteListRoundRobinPolicy([self.__cass_url])

        token_policy = TokenAwarePolicy(dc_policy)
        connection.setup([host for host in self.__cass_url.split(',')], self.__cass_keyspace,
                         protocol_version=self.__cass_protocol_version, load_balancing_policy=token_policy,
                         port=self.__cass_port)

    def fetch_nexus_tiles(self, *tile_ids):
        tile_ids = [uuid.UUID(str(tile_id)) for tile_id in tile_ids if
                    (isinstance(tile_id, str) or isinstance(tile_id, unicode))]

        res = []
        for tile_id in tile_ids:
            filterResults = NexusTileData.objects.filter(tile_id=tile_id)
            if len(filterResults) > 0:
                res.append(filterResults[0])

        return res
