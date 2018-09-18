"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import colortables
import numpy as np
import math

import struct


def pack_float_to_int24(fv):
    return map(int, bytearray(struct.pack("f", fv)))


def colorize_tile_matrix(tile_data, min, max, table):
    height = tile_data.shape[0]
    width = tile_data.shape[1]

    # Reshaping the matrix into a single dimensional array
    tile_data = np.reshape(tile_data, (width * height))

    # Replace with min/max values
    tile_data[np.nan_to_num(tile_data) < min] = min
    tile_data[np.nan_to_num(tile_data) > max] = max

    def f(v):
        if np.isnan(v) or np.ma.is_masked(v):
            return (0, 0, 0, 0)
        #return pack_float_to_int24(v)
        v = int(round((v - min) / (max - min) * 255.0))
        return get_color(v, table)

    data = np.array(list(map(f, tile_data)))
    data = np.reshape(data, (height, width, 4))
    data = np.flip(data, axis=0)
    return data


def get_color(value, table):
    index = (value / 255.0) * (len(table) - 1.0)
    low_index = int(math.floor(index))
    high_index = int(math.ceil(index))

    f = index - low_index
    low_color = table[low_index]
    high_color = table[high_index]

    rgb = high_color * f + low_color * (1.0 - f)
    rgb = np.append(rgb, 255)
    return rgb


