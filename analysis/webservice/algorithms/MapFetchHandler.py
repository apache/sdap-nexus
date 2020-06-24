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

import calendar
import errno
import io
import json
import math
import os
import time
from subprocess import call

import boto3
import numpy as np
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
from dateutil.relativedelta import *

import colortables
import webservice.GenerateImageMRF as MRF
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler as BaseHandler
from webservice.NexusHandler import nexus_handler


@nexus_handler
class MapFetchCalcHandler(BaseHandler):
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

    NO_DATA_IMAGE = None

    def __init__(self):
        BaseHandler.__init__(self)

    @staticmethod
    def __tile_to_image(img_data, tile, min, max, table, x_res, y_res):
        width = len(tile.longitudes)
        height = len(tile.latitudes)

        d = np.ma.filled(tile.data[0], np.nan)

        for y in range(0, height):
            for x in range(0, width):
                value = d[y][x]
                if not np.isnan(value) and value != 0:
                    lat = tile.latitudes[y]
                    lon = tile.longitudes[x]

                    pixel_y = int(math.floor(180.0 - ((lat + 90.0) * y_res)))
                    pixel_x = int(math.floor((lon + 180.0) * x_res))

                    value = np.max((min, value))
                    value = np.min((max, value))
                    value255 = int(round((value - min) / (max - min) * 255.0))
                    rgba = MapFetchCalcHandler.__get_color(value255, table)
                    img_data.putpixel((pixel_x, pixel_y), (rgba[0], rgba[1], rgba[2], 255))

    @staticmethod
    def __translate_interpolation(interp):
        if interp.upper() == "LANCZOS":
            return Image.LANCZOS
        elif interp.upper() == "BILINEAR":
            return Image.BILINEAR
        elif interp.upper() == "BICUBIC":
            return Image.BICUBIC
        else:
            return Image.NEAREST

    @staticmethod
    def __make_tile_img(tile):
        width = len(tile.longitudes)
        height = len(tile.latitudes)
        img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        return img

    @staticmethod
    def __get_xy_resolution(tile):
        x_res = abs(tile.longitudes[0] - tile.longitudes[1])
        y_res = abs(tile.latitudes[0] - tile.latitudes[1])
        return x_res, y_res

    @staticmethod
    def __create_global(nexus_tiles, stats, width=2048, height=1024, force_min=np.nan, force_max=np.nan,
                        table=colortables.grayscale, interpolation="nearest"):

        data_min = stats["minValue"] if np.isnan(force_min) else force_min
        data_max = stats["maxValue"] if np.isnan(force_max) else force_max

        x_res, y_res = MapFetchCalcHandler.__get_xy_resolution(nexus_tiles[0])
        x_res = 1
        y_res = 1

        canvas_width = int(360.0 / x_res)
        canvas_height = int(180.0 / y_res)
        img = Image.new("RGBA", (canvas_width, canvas_height), (0, 0, 0, 0))
        img_data = img.getdata()

        for tile in nexus_tiles:
            MapFetchCalcHandler.__tile_to_image(img_data, tile, data_min, data_max, table, x_res, y_res)

        final_image = img.resize((width, height), MapFetchCalcHandler.__translate_interpolation(interpolation))

        return final_image

    @staticmethod
    def __get_color(value, table):
        index = (float(value) / float(255)) * (len(table) - 1)
        prev = int(math.floor(index))
        next = int(math.ceil(index))

        f = index - prev
        prevColor = table[prev]
        nextColor = table[next]

        r = int(round(nextColor[0] * f + (prevColor[0] * (1.0 - f))))
        g = int(round(nextColor[1] * f + (prevColor[1] * (1.0 - f))))
        b = int(round(nextColor[2] * f + (prevColor[2] * (1.0 - f))))

        return (r, g, b, 255)

    @staticmethod
    def __colorize(img, table):
        data = img.getdata()

        for x in range(0, img.width):
            for y in range(0, img.height):
                if data[x + (y * img.width)][3] == 255:
                    value = data[x + (y * img.width)][0]
                    rgba = MapFetchCalcHandler.__get_color(value, table)
                    data.putpixel((x, y), (rgba[0], rgba[1], rgba[2], 255))

    @staticmethod
    def __create_no_data(width, height):

        if MapFetchCalcHandler.NO_DATA_IMAGE is None:
            img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)

            fnt = ImageFont.load_default()

            for x in range(10, width, 100):
                for y in range(10, height, 100):
                    draw.text((x, y), "NO DATA", (180, 180, 180), font=fnt)
            MapFetchCalcHandler.NO_DATA_IMAGE = img

        return MapFetchCalcHandler.NO_DATA_IMAGE

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        dataTimeEnd = computeOptions.get_datetime_arg("t", None)
        if dataTimeEnd is None:
            raise Exception("Missing 't' option for time")

        dataTimeEnd = time.mktime(dataTimeEnd.timetuple())
        dataTimeStart = dataTimeEnd - 86400.0

        color_table_name = computeOptions.get_argument("ct", "smap")
        color_table = colortables.__dict__[color_table_name]

        interpolation = computeOptions.get_argument("interp", "nearest")

        force_min = computeOptions.get_float_arg("min", np.nan)
        force_max = computeOptions.get_float_arg("max", np.nan)

        width = np.min([8192, computeOptions.get_int_arg("width", 1024)])
        height = np.min([8192, computeOptions.get_int_arg("height", 512)])

        stats = self._get_tile_service().get_dataset_overall_stats(ds)

        daysinrange = self._get_tile_service().find_days_in_range_asc(-90.0, 90.0, -180.0, 180.0, ds, dataTimeStart,
                                                                dataTimeEnd)

        if len(daysinrange) > 0:
            ds1_nexus_tiles = self._get_tile_service().get_tiles_bounded_by_box_at_time(-90.0, 90.0, -180.0, 180.0,
                                                                                  ds,
                                                                                  daysinrange[0])

            img = self.__create_global(ds1_nexus_tiles, stats, width, height, force_min, force_max, color_table,
                                       interpolation)
        else:
            img = self.__create_no_data(width, height)

        imgByteArr = io.BytesIO()
        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()

        class SimpleResult(object):
            def toJson(self):
                return json.dumps({"status": "Please specify output type as PNG."})

            def toImage(self):
                return imgByteArr

        return SimpleResult()

    def generate(self, ds, granule_name, prefix, ct, interp, _min, _max, width, height, time_interval):

        color_table_name = ct
        if ct is None:
            color_table_name = "smap"
        color_table = colortables.__dict__[color_table_name]

        interpolation = interp
        if interp is None:
            interpolation = "near"

        force_min = _min
        force_max = _max
        if _min is None:
            force_min = np.nan
        if _max is None:
            force_max = np.nan

        temp_width = width
        temp_height = height
        if width is None:
            temp_width = 1024
        if height is None:
            temp_height = 512

        width = np.min([8192, temp_width])
        height = np.min([8192, temp_height])

        if time_interval == 'day':
            time_interval = relativedelta(days=+1)
        else:
            time_interval = relativedelta(months=+1)

        stats = self._get_tile_service().get_dataset_overall_stats(ds)

        start_time, end_time = self._get_tile_service().get_min_max_time_by_granule(ds, granule_name)

        MRF.create_all(ds, prefix)

        # Make a temporary directory for storing the .png and .tif files
        temp_dir = '/tmp/tmp/'
        try:
            os.makedirs(temp_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        while start_time <= end_time:
            one_interval_later = start_time + time_interval
            temp_end_time = one_interval_later - relativedelta(minutes=+1)  # prevent getting tiles for 2 intervals
            ds1_nexus_tiles = self._get_tile_service().find_tiles_in_box(-90.0, 90.0, -180.0, 180.0, ds, start_time,
                                                                   temp_end_time)

            if ds1_nexus_tiles is not None:
                img = self.__create_global(ds1_nexus_tiles, stats, width, height, force_min, force_max, color_table,
                                           interpolation)
            else:
                img = self.__create_no_data(width, height)

            imgByteArr = io.BytesIO()
            img.save(imgByteArr, format='PNG')
            imgByteArr = imgByteArr.getvalue()

            arr = str(start_time).split()  # arr[0] should contain a string of the date in format 'YYYY-MM-DD'
            fulldate = arr[0]
            temp_png = temp_dir + fulldate + '.png'
            temp_tif = temp_dir + fulldate + '.tif'
            open(temp_png, 'wb').write(imgByteArr)

            arr = fulldate.split('-')
            year = arr[0]
            month = calendar.month_abbr[int(arr[1])]
            dt = month + '_' + year

            retcode = MRF.png_to_tif(temp_png, temp_tif)
            if retcode == 0:
                retcode = MRF.geo_to_mrf(temp_tif, prefix, year, dt, ds)
            if retcode == 0:
                retcode = MRF.geo_to_arctic_mrf(temp_tif, prefix, year, dt, ds)
            if retcode == 0:
                retcode = MRF.geo_to_antarctic_mrf(temp_tif, prefix, year, dt, ds, interp)
            if retcode != 0:
                break

            start_time = one_interval_later

        tar_file = ds + '.tar.gz'
        retcode = call(["tar", "-zcvf", tar_file, ds])
        if retcode == 0:
            # Delete temporary files/folders if tar.gz is created successfully
            call(["rm", "-rf", ds])
            call(["rm", "-rf", temp_dir])
        else:
            print("Error creating tar.gz")

        # Upload the tar.gz file to the sea-level-mrf S3 bucket
        s3bucket = 'sea-level-mrf'
        s3client = boto3.client('s3')
        try:
            with open(tar_file, 'rb') as data:
                s3client.upload_fileobj(data, s3bucket, tar_file)
        except Exception as e:
            print("Unable to add tar.gz to S3: \n" + str(e))

        call(["rm", "-rf", tar_file])  # Delete the tar.gz from local storage
