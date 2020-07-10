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


import logging
import math
from datetime import datetime

from pytz import timezone
from shapely.geometry import box

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException

SENTINEL = 'STOP'
EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
tile_service = None


@nexus_handler
class LongitudeLatitudeMapCalcHandlerImpl(NexusCalcHandler):
    name = "Longitude/Latitude Time Average Map"
    path = "/longitudeLatitudeMap"
    description = "Computes a Latitude/Longitude Time Average plot given an arbitrary geographical area and time range"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "One or more comma-separated dataset shortnames"
        },
        "minLat": {
            "name": "Minimum Latitude",
            "type": "float",
            "description": "Minimum (Southern) bounding box Latitude"
        },
        "maxLat": {
            "name": "Maximum Latitude",
            "type": "float",
            "description": "Maximum (Northern) bounding box Latitude"
        },
        "minLon": {
            "name": "Minimum Longitude",
            "type": "float",
            "description": "Minimum (Western) bounding box Longitude"
        },
        "maxLon": {
            "name": "Maximum Longitude",
            "type": "float",
            "description": "Maximum (Eastern) bounding box Longitude"
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since epoch (Jan 1st, 1970)"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since epoch (Jan 1st, 1970)"
        }
    }
    singleton = True

    def __init__(self):
        NexusCalcHandler.__init__(self, skipCassandra=True)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")
        try:
            ds = request.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        try:
            bounding_polygon = box(request.get_min_lon(), request.get_min_lat(), request.get_max_lon(),
                                   request.get_max_lat())
        except:
            raise NexusProcessingException(
                reason="'minLon', 'minLat', 'maxLon', and 'maxLat' arguments are required.",
                code=400)

        try:
            start_time = request.get_start_datetime()
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value milliseconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value milliseconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        return ds, bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch

    def calc(self, request, **args):

        ds, bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch = self.parse_arguments(request)

        boxes = self._get_tile_service().get_distinct_bounding_boxes_in_polygon(bounding_polygon, ds,
                                                                          start_seconds_from_epoch,
                                                                          end_seconds_from_epoch)
        point_avg_over_time = lat_lon_map_driver(bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch, ds,
                                                 [a_box.bounds for a_box in boxes])

        kwargs = {
            "minLon": bounding_polygon.bounds[0],
            "minLat": bounding_polygon.bounds[1],
            "maxLon": bounding_polygon.bounds[2],
            "maxLat": bounding_polygon.bounds[3],
            "ds": ds,
            "startTime": datetime.utcfromtimestamp(start_seconds_from_epoch).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "endTime": datetime.utcfromtimestamp(end_seconds_from_epoch).strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        return LongitudeLatitudeMapResults(
            results=LongitudeLatitudeMapCalcHandlerImpl.results_to_dicts(point_avg_over_time), meta=None,
            **kwargs)

    @staticmethod
    def results_to_dicts(results):

        # ((lon, lat), (slope, intercept, r_value, p_value, std_err, mean, pmax, pmin, pstd, pcnt))
        return [{
            'lon': result[0][0],
            'lat': result[0][1],
            'slope': result[1][0] if not math.isnan(result[1][0]) else 'NaN',
            'intercept': result[1][1] if not math.isnan(result[1][1]) else 'NaN',
            'r': result[1][2],
            'p': result[1][3],
            'stderr': result[1][4] if not math.isinf(result[1][4]) else 'Inf',
            'avg': result[1][5],
            'max': result[1][6],
            'min': result[1][7],
            'std': result[1][8],
            'cnt': result[1][9],
        } for result in results]


def pool_initializer():
    from nexustiles.nexustiles import NexusTileService
    global tile_service
    tile_service = NexusTileService()
    # TODO This is a hack to make sure each sub-process uses it's own connection to cassandra. data-access needs to be updated
    from cassandra.cqlengine import connection
    from multiprocessing import current_process

    connection.register_connection(current_process().name, [host.address for host in connection.get_session().hosts])
    connection.set_default_connection(current_process().name)


def lat_lon_map_driver(search_bounding_polygon, search_start, search_end, ds, distinct_boxes):
    from functools import partial
    from nexustiles.nexustiles import NexusTileService
    # Start new processes to handle the work
    # pool = Pool(5, pool_initializer)

    func = partial(regression_on_tiles, search_bounding_polygon_wkt=search_bounding_polygon.wkt,
                   search_start=search_start, search_end=search_end, ds=ds)

    global tile_service
    tile_service = NexusTileService()
    map_result = map(func, distinct_boxes)
    return [item for sublist in map_result for item in sublist]
    # TODO Use for multiprocessing:
    # result = pool.map_async(func, distinct_boxes)
    #
    # pool.close()
    # while not result.ready():
    #     print "Not ready"
    #     time.sleep(5)
    #
    # print result.successful()
    # print result.get()[0]
    # pool.join()
    # pool.terminate()
    #
    # return [item for sublist in result.get() for item in sublist]


def calc_linear_regression(arry, xarry):
    from scipy.stats import linregress
    slope, intercept, r_value, p_value, std_err = linregress(arry, xarry)
    return slope, intercept, r_value, p_value, std_err


def regression_on_tiles(tile_bounds, search_bounding_polygon_wkt, search_start, search_end, ds):
    if len(tile_bounds) < 1:
        return []

    import numpy as np
    import operator
    from shapely import wkt
    from shapely.geometry import box
    search_bounding_shape = wkt.loads(search_bounding_polygon_wkt)
    tile_bounding_shape = box(*tile_bounds)

    # Load all tiles for given (exact) bounding box across the search time range
    tiles = tile_service.find_tiles_by_exact_bounds(tile_bounds, ds, search_start, search_end)
    if search_bounding_shape.contains(tile_bounding_shape):
        # The tile bounds are totally contained in the search area, we don't need to mask it.
        pass
    else:
        # The tile bounds cross the search area borders, we need to mask the tiles to the search area
        tiles = tile_service.mask_tiles_to_polygon(wkt.loads(search_bounding_polygon_wkt), tiles)
    # If all tiles end up being masked, there is no work to do
    if len(tiles) < 1:
        return []
    tiles.sort(key=operator.attrgetter('min_time'))

    stacked_tile_data = np.stack(tuple([np.squeeze(tile.data, 0) for tile in tiles]))
    stacked_tile_lons = np.stack([tile.longitudes for tile in tiles])
    stacked_tile_lats = np.stack([tile.latitudes for tile in tiles])

    x_array = np.arange(stacked_tile_data.shape[0])
    point_regressions = np.apply_along_axis(calc_linear_regression, 0, stacked_tile_data, x_array)
    point_means = np.nanmean(stacked_tile_data, 0)
    point_maximums = np.nanmax(stacked_tile_data, 0)
    point_minimums = np.nanmin(stacked_tile_data, 0)
    point_st_deviations = np.nanstd(stacked_tile_data, 0)
    point_counts = np.ma.count(np.ma.masked_invalid(stacked_tile_data), 0)

    results = []
    for lat_idx, lon_idx in np.ndindex(point_means.shape):
        lon, lat = np.max(stacked_tile_lons[:, lon_idx]), np.max(stacked_tile_lats[:, lat_idx])
        pcnt = point_counts[lat_idx, lon_idx]

        if pcnt == 0:
            continue

        mean = point_means[lat_idx, lon_idx]
        pmax = point_maximums[lat_idx, lon_idx]
        pmin = point_minimums[lat_idx, lon_idx]
        pstd = point_st_deviations[lat_idx, lon_idx]
        slope, intercept, r_value, p_value, std_err = point_regressions[:, lat_idx, lon_idx]

        results.append(((lon, lat), (slope, intercept, r_value, p_value, std_err, mean, pmax, pmin, pstd, pcnt)))

    return results


class LongitudeLatitudeMapResults(NexusResults):
    def __init__(self, results=None, meta=None, computeOptions=None, **kwargs):
        NexusResults.__init__(self, results=results, meta=meta, stats=None, computeOptions=computeOptions, **kwargs)
