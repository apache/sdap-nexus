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

import os
from math import cos
from math import radians
from math import sin
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from nexusproto.serialization import from_shaped_array, to_shaped_array


def enum(**enums):
    return type('Enum', (), enums)


U_OR_V_ENUM = enum(U='u', V='v')

u_or_v = os.environ['U_OR_V'].lower()


def calculate_u_component_value(direction, speed):
    if direction is numpy.ma.masked or speed is numpy.ma.masked:
        return numpy.ma.masked

    return speed * sin(direction)


def calculate_v_component_value(direction, speed):
    if direction is numpy.ma.masked or speed is numpy.ma.masked:
        return numpy.ma.masked

    return speed * cos(direction)


def transform(self, tile_data):
    nexus_tile = nexusproto.NexusTile.FromString(tile_data)

    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    wind_speed = from_shaped_array(the_tile_data.variable_data)

    wind_dir = from_shaped_array(next(meta for meta in the_tile_data.meta_data if meta.name == 'wind_dir').meta_data)

    assert wind_speed.shape == wind_dir.shape

    wind_u_component = numpy.ma.empty(wind_speed.shape, dtype=float)
    wind_v_component = numpy.ma.empty(wind_speed.shape, dtype=float)
    wind_speed_iter = numpy.nditer(wind_speed, flags=['multi_index'])
    while not wind_speed_iter.finished:
        speed = wind_speed_iter[0]
        current_index = wind_speed_iter.multi_index
        direction = wind_dir[current_index]

        # Convert degrees to radians
        direction = radians(direction)

        # Calculate component values
        wind_u_component[current_index] = calculate_u_component_value(direction, speed)
        wind_v_component[current_index] = calculate_v_component_value(direction, speed)

        wind_speed_iter.iternext()

    # Stick the original data into the meta data
    wind_speed_meta = the_tile_data.meta_data.add()
    wind_speed_meta.name = 'wind_speed'
    wind_speed_meta.meta_data.CopyFrom(to_shaped_array(wind_speed))

    # The u_or_v variable specifies which component variable is the 'data variable' for this tile
    # Replace data with the appropriate component value and put the other component in metadata
    if u_or_v == U_OR_V_ENUM.U:
        the_tile_data.variable_data.CopyFrom(to_shaped_array(wind_u_component))
        wind_component_meta = the_tile_data.meta_data.add()
        wind_component_meta.name = 'wind_v'
        wind_component_meta.meta_data.CopyFrom(to_shaped_array(wind_v_component))
    elif u_or_v == U_OR_V_ENUM.V:
        the_tile_data.variable_data.CopyFrom(to_shaped_array(wind_v_component))
        wind_component_meta = the_tile_data.meta_data.add()
        wind_component_meta.name = 'wind_u'
        wind_component_meta.meta_data.CopyFrom(to_shaped_array(wind_u_component))

    yield nexus_tile.SerializeToString()


def start():
    assert u_or_v.lower() == U_OR_V_ENUM.U or u_or_v.lower() == U_OR_V_ENUM.V
    start_server(transform, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
