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

import datetime
from pytz import timezone
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

import nexusproto.NexusContent_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array, to_shaped_array

EPOCH = timezone('UTC').localize(datetime.datetime(1970, 1, 1))

def transform(self, tile_data):
    nexus_tile = nexusproto.NexusTile.FromString(tile_data)

    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    time = the_tile_data.time

    timeObj = datetime.datetime.utcfromtimestamp(time)

    timeObj = timeObj.replace(day=1)

    timeObj = timezone('UTC').localize(timeObj)

    the_tile_data.time = long((timeObj - EPOCH).total_seconds())

    yield nexus_tile.SerializeToString()


def start():
    start_server(transform, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
