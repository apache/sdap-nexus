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

import numpy as np
import math

import struct


def pack_float_to_int24(fv):
    """
    Maps a float value into an array of int values representative of their byte values.
    :param fv: A float value
    :return: An int array representative of byte values
    """

    return map(int, bytearray(struct.pack("f", fv)))


def colorize_tile_matrix(tile_data, data_min, data_max, table):
    """
    Performs the colorization of a tile data matrix
    :param tile_data: The tile's data matrix
    :param data_min: The minimum value for determining color range
    :param data_max: The maximum value for determining color range
    :param table: A color table
    :return: A colorized image data matrix
    """

    height = tile_data.shape[0]
    width = tile_data.shape[1]

    # Reshaping the matrix into a single dimensional array
    tile_data = np.reshape(tile_data, (width * height))

    # Replace with min/max values
    tile_data[np.nan_to_num(tile_data) < data_min] = data_min
    tile_data[np.nan_to_num(tile_data) > data_max] = data_max

    def f(v):
        if np.isnan(v) or np.ma.is_masked(v):
            return (0, 0, 0, 0)
        #return pack_float_to_int24(v)
        v = (v - data_min) / (data_max - data_min)
        return table.get_color(v)

    data = np.array(list(map(f, tile_data)))
    data = np.reshape(data, (height, width, 4))
    data = np.flip(data, axis=0)
    return data




