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

TILE_WIDTH = 512
TILE_HEIGHT = 512


TILE_MATRIX_SETS = {}



class TileMatrix:

    def __init__(self, level, resolution, scale, top_left_lon, top_left_lat, tile_width, tile_height, num_columns, num_rows):
        self.level = int(level)
        self.scale = float(scale)
        self.resolution = float(resolution)
        self.top_left_lon = float(top_left_lon)
        self.top_left_lat = float(top_left_lat)
        self.tile_width = int(tile_width)
        self.tile_height = int(tile_height)
        self.num_columns = int(num_columns)
        self.num_rows = int(num_rows)

        self._lon_range = resolution * tile_width * num_columns
        self._lat_range = resolution * tile_height * num_rows

        # Hardcoding for global for now...
        self._tile_width_deg = self._lon_range / self.num_columns
        self._tile_height_deg = self._lat_range / self.num_rows


    def get_tllr_for_tile(self, col, row):
        """
        Determins the tllr (top left, lower right) for a tile at this matrix level given a column and row index. Hard coding for a global projection for now...
        :param level:
        :param col:
        :param row:
        :return: tllr as expressed as (maxLat, minLon, minLat, maxLon)
        """

        tl_lon = -180.0 + col * self._tile_width_deg
        tl_lat = 90 - row * self._tile_height_deg

        lr_lon = tl_lon + self._tile_width_deg
        lr_lat = tl_lat - self._tile_height_deg

        return (tl_lat, tl_lon, lr_lat, lr_lon)


class TileMatrixSet:
    """
    Defines a basic WMS tile matrix set
    """

    def __init__(self, identifier, tile_matrixes=[]):
        self.idendifier = identifier
        self.tile_matrixes = tile_matrixes

        TILE_MATRIX_SETS[self.idendifier] = self

    def get_tile_matrix_at_level(self, level):
        """
        Retrieved the tile matrix definition at the specified level
        :param level:
        :return:
        """
        assert level >= 0 and len(self.tile_matrixes) > level, "Requested level exceeds configured levels"
        tm = self.tile_matrixes[level]
        assert tm.level == level, "Configuration error, level found at incorrect index"
        return tm

    def get_tllr_for_tile(self, level, col, row):
        """
        Determins the tllr (top left, lower right) for a tile at a given level, column, and row. Hard coding for a global projection for now...
        :param level:
        :param col:
        :param row:
        :return: tllr as expressed as (maxLat, minLon, minLat, maxLon)
        """
        tm = self.get_tile_matrix_at_level(level)
        return tm.get_tllr_for_tile(col, row)






TileMatrixSet("EPSG4326",
              [
                TileMatrix(0, 0.5631315220428238, 223632905.6114871, -180, 90, TILE_WIDTH, TILE_HEIGHT, 2, 1),
                TileMatrix(1, 0.28156576102141206, 111816452.8057436, -180, 90, TILE_WIDTH, TILE_HEIGHT, 3, 2),
                TileMatrix(2, 0.14078288051070598, 55908226.40287178, -180, 90, TILE_WIDTH, TILE_HEIGHT, 5, 3),
                TileMatrix(3, 0.07039144025535299, 27954113.20143589, -180, 90, TILE_WIDTH, TILE_HEIGHT, 10, 5),
                TileMatrix(4, 0.03519572012767651, 13977056.60071795, -180, 90, TILE_WIDTH, TILE_HEIGHT, 20, 10),
                TileMatrix(5, 0.017597860063838247, 6988528.300358973, -180, 90, TILE_WIDTH, TILE_HEIGHT, 40, 20),
                TileMatrix(6, 0.008798930031919122, 3494264.150179486, -180, 90, TILE_WIDTH, TILE_HEIGHT, 80, 40),
                TileMatrix(7, 0.004399465015959561, 1747132.075089743, -180, 90, TILE_WIDTH, TILE_HEIGHT, 160, 80),
                TileMatrix(8, 0.0021997325079797805, 873566.0375448716, -180, 90, TILE_WIDTH, TILE_HEIGHT, 320, 160)
              ])




equi_lods_rectangular = [
    {"level": 0, "resolution": 0.5631315220428238, "scale": 223632905.6114871, "startTileCol": 0, "startTileRow": 0, "endTileCol": 1, "endTileRow": 0},
    {"level": 1, "resolution": 0.28156576102141206, "scale": 111816452.8057436, "startTileCol": 0, "startTileRow": 0, "endTileCol": 2, "endTileRow": 1},
    {"level": 2, "resolution" : 0.14078288051070598, "scale" : 55908226.40287178, "startTileCol": 0, "startTileRow": 0, "endTileCol": 4, "endTileRow": 2},
    {"level": 3, "resolution" : 0.07039144025535299, "scale" : 27954113.20143589, "startTileCol": 0, "startTileRow": 0, "endTileCol": 9, "endTileRow": 4},
    {"level": 4, "resolution" : 0.03519572012767651, "scale" : 13977056.60071795, "startTileCol": 0, "startTileRow": 0, "endTileCol": 19, "endTileRow": 9},
    {"level": 5, "resolution" : 0.017597860063838247, "scale" : 6988528.300358973, "startTileCol": 0, "startTileRow": 0, "endTileCol": 39, "endTileRow": 19},
    {"level": 6, "resolution" : 0.008798930031919122, "scale" : 3494264.150179486, "startTileCol": 0, "startTileRow": 0, "endTileCol": 79, "endTileRow": 39},
    {"level": 7, "resolution" : 0.004399465015959561, "scale" : 1747132.075089743, "startTileCol": 0, "startTileRow": 0, "endTileCol": 158, "endTileRow": 78},
    {"level": 8, "resolution" : 0.0021997325079797805, "scale" : 873566.0375448714, "startTileCol": 0, "startTileRow": 0, "endTileCol": 316, "endTileRow": 156},
    {"level": 9, "resolution" : 0.0010998662539898902, "scale" : 436783.0187724357, "startTileCol": 0, "startTileRow": 0, "endTileCol": 632, "endTileRow": 312}
]

equi_lods_polar = [
    {"level": 0, "resolution": 8192, "scale": 29257142.85714286, "startTileCol": 0, "startTileRow": 0, "endTileCol": 2, "endTileRow": 2},
    {"level": 1, "resolution": 4096, "scale": 14628571.42857143, "startTileCol": 0, "startTileRow": 0, "endTileCol": 4, "endTileRow": 4},
    {"level": 2, "resolution": 2048, "scale": 7314285.714285715, "startTileCol": 0, "startTileRow": 0, "endTileCol": 8, "endTileRow": 8},
    {"level": 3, "resolution": 1023.9999999999999, "scale": 3657142.857142857, "startTileCol": 0, "startTileRow": 0, "endTileCol": 16, "endTileRow": 16},
    {"level": 4, "resolution": 512.0000000000001, "scale": 1828571.428571429, "startTileCol": 0, "startTileRow": 0, "endTileCol": 31, "endTileRow": 31},
    {"level": 5, "resolution": 256.00000000000006, "scale": 914285.7142857146, "startTileCol": 0, "startTileRow": 0, "endTileCol": 62, "endTileRow": 62},
    {"level": 6, "resolution": 128.00000000000003, "scale": 457142.8571428573, "startTileCol": 0, "startTileRow": 0, "endTileCol": 124, "endTileRow": 124},
    {"level": 7, "resolution": 64.00000000000001, "scale": 228571.42857142864, "startTileCol": 0, "startTileRow": 0, "endTileCol": 248, "endTileRow": 248},
    {"level": 8, "resolution": 32.00000000000001, "scale": 114285.71428571432, "startTileCol": 0, "startTileRow": 0, "endTileCol": 496, "endTileRow": 496},
    {"level": 9, "resolution": 16.000000000000004, "scale": 57142.85714285716, "startTileCol": 0, "startTileRow": 0, "endTileCol": 992, "endTileRow": 992}
]