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

import types
from scipy.misc import imresize
from PIL import Image
from PIL import ImageFont
from PIL import ImageDraw
import multiprocessing

import colortables
import colorization
import reprojection

NO_DATA_IMAGE = None


def translate_interpolation(interp):
    if interp.upper() == "LANCZOS":
        return Image.LANCZOS
    elif interp.upper() == "BILINEAR":
        return Image.BILINEAR
    elif interp.upper() == "BICUBIC":
        return Image.BICUBIC
    else:
        return Image.NEAREST


def get_first_valid_pair(coord_array):
    """
    Locates a contiguous pair of coordinates in a masked numpy array.
    :param coord_array: A numpy array of numpy.float32 values
    :return: a pair of values
    :except: When no contiguous pair of coordinates are found.
    """
    for i in range(0, len(coord_array)-1):
        if isinstance(coord_array[i], np.float32) and isinstance(coord_array[i+1], np.float32):
            return coord_array[i], coord_array[i+1]

    raise Exception("No valid coordinate pair found!")

def get_xy_resolution(tile):
    """
    Computes the x/y (lon, lat) resolution of a tile
    :param tile: A tile
    :return: Resolution as (x_res, y_res)
    """
    lon_0, lon_1 = get_first_valid_pair(tile.longitudes)
    lat_0, lat_1 = get_first_valid_pair(tile.latitudes)

    x_res = abs(lon_0 - lon_1)
    y_res = abs(lat_0 - lat_1)

    return x_res, y_res


def positioning_for_tile(tile, tllr, canvas_height, canvas_width):
    """
    Computes the x/y position of a tile matrix within the larger canvas
    :param tile: A tile
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param canvas_height: Height of the canvas
    :param canvas_width: Width of the canvas
    :return: The top left pixels as (tl_pixel_y, tl_pixel_x)
    """

    tl_lat = tile.bbox.max_lat
    tl_lon = tile.bbox.min_lon

    max_lat = tllr[0] + 90.0
    min_lon = tllr[1] + 180.0
    min_lat = tllr[2] + 90.0
    max_lon = tllr[3] + 180.0

    tl_pixel_y = int(round((max_lat - (tl_lat + 90.0)) / (max_lat - min_lat) * canvas_height))
    tl_pixel_x = int(round((tl_lon + 180.0 - min_lon) / (max_lon - min_lon) * canvas_width))

    return tl_pixel_y, tl_pixel_x


def process_tile(tile, tllr, data_min, data_max, table, canvas_height, canvas_width, background):
    """
    Processes a tile for colorization and positioning
    :param tile: The tile
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param data_min: Minimum value
    :param data_max: Maximum value
    :param table: A color table
    :param canvas_height: Height of the canvas
    :param canvas_width: Width of the canvas
    :param background: Default color as RGBA
    :return: The tile image data and top left pixel positions as (tile_img_data, tl_pixel_y, tl_pixel_x)
    """

    tile_img_data = colorization.colorize_tile_matrix(tile.data[0], data_min, data_max, table, background)
    tl_pixel_y, tl_pixel_x = positioning_for_tile(tile, tllr, canvas_height, canvas_width)
    return (tile_img_data, tl_pixel_y, tl_pixel_x)


def process_tile_async(args):
    """
    A proxy for process_tile for use in multiprocessing. Accepts a list of parameters
    :param args: The list of parameters in the order accepted by process_tile
    :return: The results of process_tile
    """

    return process_tile(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7])


def process_tiles(tiles, tllr, data_min, data_max, table, canvas_height, canvas_width, background):
    """
    Loops through a list of tiles and calls process_tile on each
    :param tiles: A list of tiles
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param data_min: The minimum value
    :param data_max: The maximum value
    :param table: A color table
    :param canvas_height: The height of the canvas
    :param canvas_width: The width of the canvas
    :param background: Default color as RGBA
    :return: The results of each call to process_tile in a list
    """

    results = []
    for tile in tiles:
        result = process_tile(tile, tllr, data_min, data_max, table, canvas_height, canvas_width, background)
        results.append(result)
    return results


def process_tiles_async(args):
    """
    A proxy for process_tiles for use in multiprocessing. Accepts a list of parameters.
    :param args: The list of parameters in the order accepted by process_tiles
    :return: The results of process_tiles
    """

    return process_tiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7])


def compute_canvas_size(tllr, x_res, y_res):
    """
    Computes the necessary size of the canvas given a tllr and spatial resolutions
    :param tllr: The top left, lower right coordinates as (maxLat, minLon, minLat, maxLon)
    :param x_res: The longitudinal (x) resolution
    :param y_res: The latitudinal (y) resolution
    :return: The canvas dimentions as (height, width)
    """

    max_lat = tllr[0]
    min_lon = tllr[1]
    min_lat = tllr[2]
    max_lon = tllr[3]

    canvas_width = int(round((max_lon - min_lon) / x_res))
    canvas_height = int(round((max_lat - min_lat) / y_res))

    return canvas_height, canvas_width


def compute_tiles_tllr(nexus_tiles):
    """
    Computes a tllr for a given list of nexus tiles.
    :param nexus_tiles: A list of nexus tiles
    :return: The top left, lower right coordinate boundaries of the tiles as (maxLat, minLon, minLat, maxLon)
    """

    min_lat = 90.0
    max_lat = -90.0
    min_lon = 180.0
    max_lon = -180.0

    for tile in nexus_tiles:
        tile_max_lat = tile.bbox.max_lat
        tile_min_lat = tile.bbox.min_lat
        tile_max_lon = tile.bbox.max_lon
        tile_min_lon = tile.bbox.min_lon

        min_lat = np.array((min_lat, tile_min_lat)).min()
        max_lat = np.array((max_lat, tile_max_lat)).max()
        min_lon = np.array((min_lon, tile_min_lon)).min()
        max_lon = np.array((max_lon, tile_max_lon)).max()

    return (max_lat, min_lon, min_lat, max_lon)


def trim_map_to_requested_tllr(data, reqd_tllr, data_tllr):
    """
    Trims a canvas to the requested tllr. Only trims (crops), will not expand.
    :param data: A canvas image data
    :param reqd_tllr: Requested top left, lower right boundaries as (maxLat, minLon, minLat, maxLon)
    :param data_tllr: Data (canvas) top left, lower right boundaries as (maxLat, minLon, minLat, maxLon)
    :return: The trimmed canvas data
    """

    data_height = data.shape[0]
    data_width = data.shape[1]

    max_lat = data_tllr[0]
    min_lat = data_tllr[2]

    max_lon = data_tllr[3]
    min_lon = data_tllr[1]

    reqd_max_lat = reqd_tllr[0]
    reqd_min_lat = reqd_tllr[2]

    reqd_max_lon = reqd_tllr[3]
    reqd_min_lon = reqd_tllr[1]

    t_pixel_y = int(round((max_lat - reqd_max_lat) / (max_lat - min_lat) * data_height))
    b_pixel_y = int(math.ceil((max_lat - reqd_min_lat) / (max_lat - min_lat) * data_height))

    l_pixel_x = int(round((reqd_min_lon - min_lon) / (max_lon - min_lon) * data_width))
    r_pixel_x = int(math.ceil((reqd_max_lon - min_lon) / (max_lon - min_lon) * data_width))

    # Make sure the top and left pixels are at least 0
    t_pixel_y = np.array((0, t_pixel_y)).max()
    l_pixel_x = np.array((0, l_pixel_x)).max()

    # Make sure the bottom and right pixels are at most the highest index in data
    b_pixel_y = np.array((len(data) - 1, b_pixel_y)).min()
    r_pixel_x = np.array((len(data[0]) - 1, r_pixel_x)).min()


    data = data[t_pixel_y:b_pixel_y, l_pixel_x:r_pixel_x]
    return data


def expand_map_to_requested_tllr(data, reqd_tllr, data_tllr, x_res, y_res):
    output_canvas_height, output_canvas_width = compute_canvas_size(reqd_tllr, x_res, y_res)

    output_canvas_height = np.array((output_canvas_height, data.shape[0])).max()
    output_canvas_width = np.array((output_canvas_width, data.shape[1])).max()

    expanded_data = np.zeros((output_canvas_height, output_canvas_width, 4))
    expanded_data[0:data.shape[0], 0:data.shape[1]] = data

    return expanded_data


def process_tiles_to_map(nexus_tiles, stats, reqd_tllr, width=None, height=None, force_min=None, force_max=None, table=colortables.get_color_table("grayscale"), interpolation="nearest", background=(0, 0, 0, 0)):
    """
    Processes a list of tiles into a colorized image map.
    :param nexus_tiles: A list of nexus tiles
    :param stats: Stats from Solr
    :param reqd_tllr: Requested top left, lower right image boundaries as (maxLat, minLon, minLat, maxLon)
    :param width: Requested output width. Will use native data resolution if 'None'
    :param height: Requested output height. Will use native data resolution if 'None'
    :param force_min: A forced minimum value for the data. Will use data minimum from 'stats' if 'None'
    :param force_max: A forced maximum value for the data. Will use data maximum from 'stats' if 'None'
    :param table: A color table
    :param interpolation: Resizing interpolation mode. Defaults to "nearest"
    :param background: Default color as RGBA
    :return: A colorized image map as a PIL Image object
    """

    data_min = stats["minValue"] if force_min is None else force_min
    data_max = stats["maxValue"] if force_max is None else force_max

    x_res, y_res = get_xy_resolution(nexus_tiles[0])

    tiles_tllr = compute_tiles_tllr(nexus_tiles)
    canvas_height, canvas_width = compute_canvas_size(tiles_tllr, x_res, y_res)

    data = np.zeros((canvas_height, canvas_width, 4))
    data[:,:,:] = background

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    proc_results = None

    try:
        n = int(math.ceil(len(nexus_tiles) / float(multiprocessing.cpu_count())))
        tile_chunks = np.array_split(np.array(nexus_tiles), n)
        params = [(tiles, tiles_tllr, data_min, data_max, table, canvas_height, canvas_width, background) for tiles in tile_chunks]
        proc_results = pool.map(process_tiles_async, params)
    except:
        raise
    finally:
        pool.terminate()

    for results in proc_results:
        for result in results:
            tile_img_data, tl_pixel_y, tl_pixel_x = result
            # Subset the tile image data matrix to the max allowable size (prevent overflows on the 'data' matrix.)
            data_shape = data[tl_pixel_y:(tl_pixel_y + tile_img_data.shape[0]),tl_pixel_x:(tl_pixel_x + tile_img_data.shape[1])].shape
            tile_img_data = tile_img_data[:data_shape[0],:data_shape[1],:]

            data[tl_pixel_y:(tl_pixel_y + tile_img_data.shape[0]),tl_pixel_x:(tl_pixel_x + tile_img_data.shape[1])] = tile_img_data

    data = trim_map_to_requested_tllr(data, reqd_tllr, tiles_tllr)
    data = expand_map_to_requested_tllr(data, reqd_tllr, tiles_tllr, x_res, y_res)

    #data = reprojection.reproject_rgba_map(data, reprojection.TargetProjection.NORTH)

    if width is not None and height is not None:
        data = imresize(data, (height, width), interp=interpolation)

    im = Image.fromarray(np.asarray(data, dtype=np.uint8))
    return im


def create_no_data(width, height):
    """
    Creates a 'No Data' image at the given width and height
    :param width: Output width
    :param height: Output height
    :return: A 'No Data' image as a PIL Image object
    """

    global NO_DATA_IMAGE
    if NO_DATA_IMAGE is None:
        img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        fnt = ImageFont.truetype('webservice/algorithms/imaging/Roboto/Roboto-Bold.ttf', 10)

        #for x in range(0, width, 100):
        for y in range(0, height, 150):
            draw.text((20, y), "NO DATA", (180, 180, 180), font=fnt)
        NO_DATA_IMAGE = img

    return NO_DATA_IMAGE


def check_within(v, mn, mx):
    """
    Fit a value within two bounds
    :param v: The value needing to be fit
    :param mn: The minimum valid value
    :param mx: The maximum valid value
    :return: The fit value
    """
    v = np.array((mn, v)).max()
    v = np.array((mx, v)).min()
    return v


def fetch_nexus_tiles(tile_service, min_lat, max_lat, min_lon, max_lon, ds, dataTimeStart, dataTimeEnd):
    """
    Fetches Nexus tiles given a nexus tile service instance and search parameters
    :param tile_service:
    :param min_lat:
    :param max_lat:
    :param min_lon:
    :param max_lon:
    :param ds:
    :param dataTimeStart:
    :param dataTimeEnd:
    :return:
    """

    min_lat = check_within(min_lat, -90, 90)
    max_lat = check_within(max_lat, -90, 90)
    min_lon = check_within(min_lon, -180, 180)
    max_lon = check_within(max_lon, -180, 180)

    daysinrange = tile_service.find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, ds, dataTimeStart, dataTimeEnd)
    if len(daysinrange) > 0:
        ds1_nexus_tiles = tile_service.find_all_tiles_in_box_at_time(min_lat, max_lat, min_lon, max_lon, ds, daysinrange[0])
        return ds1_nexus_tiles
    else:
        None

def create_map(tile_service, tllr, ds, dataTimeStart, dataTimeEnd, width=None, height=None, force_min=None, force_max=None, table=colortables.get_color_table("grayscale"), interpolation="nearest", background=(0, 0, 0, 0)):
    """
    Creates a colorized data map given a dataset, tllr boundaries, timeframe, etc.
    :param tile_service: The Nexus tile service instance.
    :param tllr: The requested top left, lower right boundaries as (maxLat, minLon, minLat, maxLon)
    :param ds: The Nexus shortname for the requested dataset
    :param dataTimeStart: An allowable minimum date
    :param dataTimeEnd:  An allowable maximum date.
    :param width: An output width in pixels. Will use native data resolution if 'None'
    :param height: An output height in pixels. Will use native data resolution if 'None'
    :param force_min: Force a minimum data value. Will use data resultset minimum if 'None'
    :param force_max: Force a maximum data value. Will use data resultset maximum if 'None'
    :param table: A colortable
    :param interpolation: A image resize interpolation model. Defaults to 'nearest'
    :param background: Default color as RGBA
    :return: A colorized map image as a PIL Image object. Image will contain 'No Data' if no data was found within the given parameters.
    """

    assert len(tllr) == 4, "Invalid number of parameters for tllr"

    max_lat = tllr[0]
    min_lon = tllr[1]
    min_lat = tllr[2]
    max_lon = tllr[3]

    stats = tile_service.get_dataset_overall_stats(ds)

    nexus_tiles = fetch_nexus_tiles(tile_service, min_lat, max_lat, min_lon, max_lon, ds, dataTimeStart, dataTimeEnd)

    if nexus_tiles is not None and len(nexus_tiles) > 0:
        img = process_tiles_to_map(nexus_tiles, stats, tllr, width, height, force_min, force_max, table, interpolation, background)
    else:
        img = create_no_data(width, height)

    return img