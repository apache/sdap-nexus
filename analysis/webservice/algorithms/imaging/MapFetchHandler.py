"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import json
import time
import colortables
import numpy as np
import math
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler

from scipy.misc import imresize
import io
from PIL import Image
from PIL import ImageFont
from PIL import ImageDraw
import multiprocessing

import struct


import mapprocessing
import colorization




@nexus_handler
class MapFetchHandler(BaseHandler):
    name = "MapFetchHandler"
    path = "/map"
    description = "Creates a map image"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "A supported dataset shortname identifier"
        },
        "t": {
            "name": "Time",
            "type": "int",
            "description": "Data observation date"
        },
        "output": {
            "name": "Output Format",
            "type": "string",
            "description": "Output format. Use 'PNG' for this endpoint"
        },
        "min": {
            "name": "Minimum Value",
            "type": "float",
            "description": "Minimum value to use when computing color scales"
        },
        "max": {
            "name": "Maximum Value",
            "type": "float",
            "description": "Maximum value to use when computing color scales"
        },
        "ct": {
            "name": "Color Table",
            "type": "string",
            "description": "Identifier of a supported color table"
        },
        "interp": {
            "name": "Interpolation filter",
            "type": "string",
            "description": "Interpolation filter to use when rescaling image data. Can be 'nearest', 'lanczos', 'bilinear', or 'bicubic'."
        },
        "width": {
            "name": "Width",
            "type": "int",
            "description": "Output image width (max: 8192)"
        },
        "height": {
            "name": "Height",
            "type": "int",
            "description": "Output image height (max: 8192)"
        }
    }
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        dataTimeEnd = computeOptions.get_datetime_arg("t", None)
        if dataTimeEnd is None:
            raise Exception("Missing 't' option for time")

        dataTimeEnd = time.mktime(dataTimeEnd.timetuple())
        dataTimeStart = dataTimeEnd - 86400.0

        color_table_name = computeOptions.get_argument("ct", "smap")
        color_table = np.array(colortables.__dict__[color_table_name], dtype=np.float32)

        interpolation = computeOptions.get_argument("interp", "nearest")

        force_min = computeOptions.get_float_arg("min", np.nan)
        force_max = computeOptions.get_float_arg("max", np.nan)

        width = np.min([8192, computeOptions.get_int_arg("width", 1024)])
        height = np.min([8192, computeOptions.get_int_arg("height", 512)])

        minLat = computeOptions.get_min_lat()
        maxLat = computeOptions.get_max_lat()
        minLon = computeOptions.get_min_lon()
        maxLon = computeOptions.get_max_lon()

        img = mapprocessing.create_map(self._tile_service, (maxLat, minLon, minLat, maxLon), ds, dataTimeStart, dataTimeEnd, width, height, force_min, force_max, color_table, interpolation)

        #print "C"
        #img = self.__create_no_data(width, height)

        print "F"
        imgByteArr = io.BytesIO()

        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()
        print "G"

        class SimpleResult(object):
            def toJson(self):
                return json.dumps({"status": "Please specify output type as PNG."})

            def toImage(self):
                return imgByteArr

        return SimpleResult()
