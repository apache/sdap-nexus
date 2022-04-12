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

import logging
import uuid
from configparser import NoOptionError

import nexusproto.DataTile_pb2 as nexusproto
import numpy as np
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import columns, connection, CQLEngineException
from cassandra.cqlengine.models import Model
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, WhiteListRoundRobinPolicy
from multiprocessing.synchronize import Lock
from nexusproto.serialization import from_shaped_array
from .NexusTileDataBase import NexusTileDataBase

INIT_LOCK = Lock(ctx=None)

logger = logging.getLogger(__name__)


class NexusTileData(Model, NexusTileDataBase):
    __table_name__ = 'sea_surface_temp'
    tile_id = columns.UUID(primary_key=True)
    tile_blob = columns.Blob()

    __nexus_tile = None

    def _get_nexus_tile(self):
        if self.__nexus_tile is None:
            self.__nexus_tile = nexusproto.TileData.FromString(self.tile_blob)

        return self.__nexus_tile


class CassandraProxy(object):
    def __init__(self, config):
        self.config = config
        self.__cass_url = config.get("cassandra", "host")
        self.__cass_username = config.get("cassandra", "username")
        self.__cass_password = config.get("cassandra", "password")
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
            token_policy = TokenAwarePolicy(dc_policy)
        elif self.__cass_dc_policy == 'WhiteListRoundRobinPolicy':
            token_policy = WhiteListRoundRobinPolicy([self.__cass_url])

        if self.__cass_username and self.__cass_password:
            auth_provider = PlainTextAuthProvider(username=self.__cass_username, password=self.__cass_password)
        else:
            auth_provider = None

        connection.setup([host for host in self.__cass_url.split(',')], self.__cass_keyspace,
                         protocol_version=self.__cass_protocol_version, load_balancing_policy=token_policy,
                         port=self.__cass_port,
                         auth_provider=auth_provider)

    def fetch_nexus_tiles(self, *tile_ids):
        tile_ids = [uuid.UUID(str(tile_id)) for tile_id in tile_ids if
                    (isinstance(tile_id, str) or isinstance(tile_id, str))]

        res = []
        for tile_id in tile_ids:
            filterResults = NexusTileData.objects.filter(tile_id=tile_id)
            if len(filterResults) > 0:
                res.append(filterResults[0])

        return res
