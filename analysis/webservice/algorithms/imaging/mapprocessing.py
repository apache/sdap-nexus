"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import numpy as np
import math

from scipy.misc import imresize
from PIL import Image
from PIL import ImageFont
from PIL import ImageDraw
import multiprocessing

import colortables
import colorization


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


def get_xy_resolution(tile):
    # Sometimes there are NaN values in the latitude and longitude lists....
    # This is a quick hack to make it work for now. Don't actually keep it like this.

    lon_idx = int(round(len(tile.longitudes) / 2))
    lat_idx = int(round(len(tile.latitudes) / 2))

    x_res = abs(tile.longitudes[lon_idx] - tile.longitudes[lon_idx+1])
    y_res = abs(tile.latitudes[lat_idx] - tile.latitudes[lat_idx+1])

    return x_res, y_res


def positioning_for_tile(tile, tllr, canvas_height, canvas_width):

    tl_lat = tile.bbox.max_lat
    tl_lon = tile.bbox.min_lon

    max_lat = float(tllr[0]) + 90.0
    min_lon = float(tllr[1]) + 180.0
    min_lat = float(tllr[2]) + 90.0
    max_lon = float(tllr[3]) + 180.0

    tl_pixel_y = int(round((max_lat - (tl_lat + 90.0)) / (max_lat - min_lat) * canvas_height))
    tl_pixel_x = int(round((tl_lon + 180.0 - min_lon) / (max_lon - min_lon) * canvas_width))

    return tl_pixel_y, tl_pixel_x


def process_tile(tile, tllr, data_min, data_max, table, canvas_height, canvas_width):
    tile_img_data = colorization.colorize_tile_matrix(tile.data[0], data_min, data_max, table)
    tl_pixel_y, tl_pixel_x = positioning_for_tile(tile, tllr, canvas_height, canvas_width)
    return (tile_img_data, tl_pixel_y, tl_pixel_x)


def process_tile_async(args):
    return process_tile(args[0], args[1], args[2], args[3], args[4], args[5], args[6])


def process_tiles(tiles, tllr, data_min, data_max, table, canvas_height, canvas_width):
    results = []
    for tile in tiles:
        result = process_tile(tile, tllr, data_min, data_max, table, canvas_height, canvas_width)
        results.append(result)
    return results


def process_tiles_async(args):
    return process_tiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6])


def compute_canvas_size(tllr, x_res, y_res):
    max_lat = float(tllr[0])
    min_lon = float(tllr[1])
    min_lat = float(tllr[2])
    max_lon = float(tllr[3])

    canvas_width = int(math.ceil((max_lon - min_lon) / x_res))
    canvas_height = int(math.ceil((max_lat - min_lat) / y_res))

    return canvas_height, canvas_width


def compute_tiles_tllr(nexus_tiles):

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


def process_tiles_to_map(nexus_tiles, stats, width=2048, height=1024, force_min=np.nan, force_max=np.nan, table=colortables.grayscale, interpolation="nearest"):

    data_min = stats["minValue"] if np.isnan(force_min) else force_min
    data_max = stats["maxValue"] if np.isnan(force_max) else force_max

    x_res, y_res = get_xy_resolution(nexus_tiles[0])

    tiles_tllr = compute_tiles_tllr(nexus_tiles)
    canvas_height, canvas_width = compute_canvas_size(tiles_tllr, x_res, y_res)

    print tiles_tllr, canvas_height, canvas_width
    data = np.zeros((canvas_height, canvas_width, 4))

    pool = multiprocessing.Pool(8)
    n = int(math.ceil(len(nexus_tiles) / 8))
    tile_chunks = [nexus_tiles[i * n:(i + 1) * n] for i in range((len(nexus_tiles) + n - 1) // n)]
    params = [(tiles, tiles_tllr, data_min, data_max, table, canvas_height, canvas_width) for tiles in tile_chunks]
    proc_results = pool.map(process_tiles_async, params)

    for results in proc_results:
        for result in results:
            tile_img_data, tl_pixel_y, tl_pixel_x = result

            # Subset the tile image data matrix to the max allowable size (prevent overflows on the 'data' matrix.)
            data_shape = data[tl_pixel_y:(tl_pixel_y + tile_img_data.shape[0]),tl_pixel_x:(tl_pixel_x + tile_img_data.shape[1])].shape
            tile_img_data = tile_img_data[:data_shape[0],:data_shape[1],:]

            data[tl_pixel_y:(tl_pixel_y + tile_img_data.shape[0]),tl_pixel_x:(tl_pixel_x + tile_img_data.shape[1])] = tile_img_data

    data = imresize(data, (height, width), interp=interpolation)
    im = Image.fromarray(np.asarray(data, dtype=np.uint8))
    return im


def create_no_data(width, height):
    global NO_DATA_IMAGE
    if NO_DATA_IMAGE is None:
        img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        fnt = ImageFont.truetype('webservice/algorithms/imaging/Roboto/Roboto-Bold.ttf', 40)

        for x in range(0, width, 500):
            for y in range(0, height, 500):
                draw.text((x, y), "NO DATA", (180, 180, 180), font=fnt)
        NO_DATA_IMAGE = img

    return NO_DATA_IMAGE



def create_map(tile_service, tllr, ds, dataTimeStart, dataTimeEnd, width=2048, height=1024, force_min=np.nan, force_max=np.nan, table=colortables.grayscale, interpolation="nearest"):
    """

    :param tile_service:
    :param tllr:  (maxLat, minLon, minLat, maxLon)
    :param ds:
    :param dataTimeStart:
    :param dataTimeEnd:
    :param width:
    :param height:
    :param force_min:
    :param force_max:
    :param table:
    :param interpolation:
    :return:
    """

    assert len(tllr) == 4, "Invalid number of parameters for tllr"

    max_lat = tllr[0]
    min_lon = tllr[1]
    min_lat = tllr[2]
    max_lon = tllr[3]

    print "A"
    stats = tile_service.get_dataset_overall_stats(ds)

    print "B"
    daysinrange = tile_service.find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, ds, dataTimeStart, dataTimeEnd)

    if len(daysinrange) > 0:
        print "D"
        ds1_nexus_tiles = tile_service.get_tiles_bounded_by_box_at_time(min_lat, max_lat, min_lon, max_lon, ds, daysinrange[0])

        print "E"
        img = process_tiles_to_map(ds1_nexus_tiles, stats, width, height, force_min, force_max,
                                   table, interpolation)
    else:
        img = create_no_data(width, height)

    return img