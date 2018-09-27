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
import math
import xml.etree.ElementTree as ET
import struct

class MrfMeta:

    def __init__(self, mrf_meta_path):
        self.__mrf_meta_path = mrf_meta_path

        tree = ET.parse(mrf_meta_path)
        root = tree.getroot()
        size_el = root.find('./Raster/Size')
        self.size_x = size_el.attrib["x"]
        self.size_y = size_el.attrib["y"]
        self.size_c = size_el.attrib["c"]

        compression_el = root.find("./Raster/Compression")
        if compression_el is not None:
            self.compression = compression_el.text
        else:
            self.compression = None

        data_values_el = root.find("./Raster/DataValues")
        if data_values_el is not None:
            if "NoData" in data_values_el.attrib:
                self.no_data = data_values_el.attrib["NoData"]

        quality_el = root.find("./Raster/Quality")
        if quality_el is not None:
            self.quality = int(quality_el.text)

        page_size_el = root.find("./Raster/PageSize")
        if page_size_el is not None:
            self.page_size_x = int(page_size_el.attrib["x"])
            self.page_size_y = int(page_size_el.attrib["y"])
            self.page_size_c = int(page_size_el.attrib["c"])

        bbox_el = root.find("./GeoTags/BoundingBox")
        self.minx = float(bbox_el.attrib["minx"])
        self.miny = float(bbox_el.attrib["miny"])
        self.maxx = float(bbox_el.attrib["maxx"])
        self.maxy = float(bbox_el.attrib["maxy"])

        proj_el = root.find("./GeoTags/Projection")
        if proj_el is not None:
            self.projection = proj_el.text

        rsets = root.find("./Rsets")
        self.rsets_model = rsets.attrib["model"]
        self.rsets_scale = rsets.attrib["scale"]




class MrfReader:

    def __init__(self, mrf_path):
        self.__mrf_path = mrf_path
        self.__idx_path = "%sidx"%mrf_path[:-3]

        self.__meta = MrfMeta(mrf_path)

        data_extension = "pjg" if self.__meta.compression == "JPEG" else "ppg"
        self.__data_path = "%s%s"%(mrf_path[:-3], data_extension)

        mrf_x = self.__meta.size_x
        mrf_y = self.__meta.size_y

        w = int(math.ceil(float(mrf_x) / 512.0))
        h = int(math.ceil(float(mrf_y) / 512.0))

        len_base = w * h
        low = 1
        idx_size = int(os.path.getsize(self.__idx_path))
        len_tiles = idx_size / 16
        print "Number of tiles:", len_tiles

        self.__levels = []
        self.__rows = []
        self.__cols = []

        self.__levels.append(0)
        self.__rows.append(h)
        self.__cols.append(w)

        while len_base > low:
            self.__levels.append(len_base)
            w = int(math.ceil(float(w) / 2.0))
            h = int(math.ceil(float(h) / 2.0))
            self.__rows.append(h)
            self.__cols.append(w)
            len_base = w * h

        self.__levels.append(1)



    def get_tile_bytes(self, tilematrix, tilerow, tilecol):
        tilematrix = tilematrix + 2
        level = self.__levels[len(self.__levels) - tilematrix]
        row = self.__rows[len(self.__levels) - tilematrix]
        col = self.__cols[len(self.__levels) - tilematrix]
        #print tilerow, tilecol, level, row, col
        #assert tilerow > row - 1, "Tile row exceeds the maximum"
        #assert tilecol > col - 1, "Tole col exceeds the maximum"

        level_start = 0

        for idx in range(0, len(self.__levels)):
            val = self.__levels[idx]
            if idx > 0 and self.__levels[idx - 1] == level:
                break
            level_start += val

        tile = (tilerow * col) + tilecol + level_start

        offset = 0
        size = 0
        buffer = None

        with open(self.__idx_path) as idx_file:
            idx_file.seek(16 * tile)
            offset = struct.unpack('L', idx_file.read(8))[0]
            size = struct.unpack('L', idx_file.read(8))[0]

        with open(self.__mrf_path) as mrf_file:
            print offset, size
            mrf_file.seek(offset)
            buffer = mrf_file.read(size)

        return buffer


if __name__ == "__main__":

    mrf_path = "/Users/kgill/data/ASCATB-L2-Coastal/ASCATB-L2-Coastal/MRF-GEO/2018/ASCATBL2Coastal_2018254_.mrf"

    reader = MrfReader(mrf_path)