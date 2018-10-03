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
import numpy as np


class MrfMeta:
    """
    Implements a basic MRF metadata reader
    """

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
        if rsets is not None:
            if "model" in rsets.attrib:
                self.rsets_model = rsets.attrib["model"]
            if "scale" in rsets.attrib:
                self.rsets_scale = rsets.attrib["scale"]


class MrfReader:
    """
    Implements a basic MRF tile data reader
    """

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

    def __read_offset_and_size(self, tile):
        with open(self.__idx_path, "rb") as idx_file:
            idx_file.seek(16 * tile, os.SEEK_SET)
            offset, size = struct.unpack(">2Q", idx_file.read(16))
            return offset, size

    def __read_tile_data(self, offset, size):
        with open(self.__data_path) as mrf_file:
            mrf_file.seek(offset, os.SEEK_SET)
            return mrf_file.read(size)

    def get_tile_bytes(self, tilematrix, tilerow, tilecol):
        col = self.__cols[-tilematrix]
        level_start = np.array(self.__levels[: - tilematrix - 1]).sum()
        tile = (tilerow * col) + tilecol + level_start

        offset, size = self.__read_offset_and_size(tile)
        tile_data = self.__read_tile_data(offset, size)
        return tile_data

