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


import json
import logging
from datetime import datetime
from functools import partial

from nexustiles.nexustiles import NexusTileServiceException
from pytz import timezone

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusProcessingException, CustomEncoder

SENTINEL = 'STOP'
EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))


@nexus_handler
class StandardDeviationSearchCalcHandlerImpl(NexusCalcHandler):
    name = "Standard Deviation Search"
    path = "/standardDeviation"
    description = "Retrieves the pixel standard deviation if it exists for a given longitude and latitude"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "One or more comma-separated dataset shortnames. Required."
        },
        "longitude": {
            "name": "Longitude",
            "type": "float",
            "description": "Longitude in degrees from -180 to 180. Required."
        },
        "latitude": {
            "name": "Latitude",
            "type": "float",
            "description": "Latitude in degrees from -90 to 90. Required."
        },
        "day": {
            "name": "Day of Year",
            "type": "int",
            "description": "Day of year to search from 0 to 365. One of day or date are required but not both."
        },
        "date": {
            "name": "Date",
            "type": "string",
            "description": "Datetime in format YYYY-MM-DDTHH:mm:ssZ or seconds since epoch (Jan 1st, 1970). One of day "
                           "or date are required but not both."
        },
        "allInTile": {
            "name": "Get all Standard Deviations in Tile",
            "type": "boolean",
            "description": "Optional True/False flag. If true, return the standard deviations for every pixel in the "
                           "tile that contains the searched lon/lat point. If false, return the "
                           "standard deviation only for the searched lon/lat point. Default: True"
        }

    }
    singleton = True

    def __init__(self):
        NexusCalcHandler.__init__(self)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")
        try:
            ds = request.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        try:
            longitude = float(request.get_decimal_arg("longitude", default=None))
        except:
            raise NexusProcessingException(reason="'longitude' argument is required", code=400)

        try:
            latitude = float(request.get_decimal_arg("latitude", default=None))
        except:
            raise NexusProcessingException(reason="'latitude' argument is required", code=400)

        search_datetime = request.get_datetime_arg('date', default=None)
        day_of_year = request.get_int_arg('day', default=None)
        if (search_datetime is not None and day_of_year is not None) \
                or (search_datetime is None and day_of_year is None):
            raise NexusProcessingException(
                reason="At least one of 'day' or 'date' arguments are required but not both.",
                code=400)

        if search_datetime is not None:
            day_of_year = search_datetime.timetuple().tm_yday

        return_all = request.get_boolean_arg("allInTile", default=True)

        return ds, longitude, latitude, day_of_year, return_all

    def calc(self, request, **args):
        raw_args_dict = {k: request.get_argument(k) for k in request.requestHandler.request.arguments}
        ds, longitude, latitude, day_of_year, return_all = self.parse_arguments(request)

        if return_all:
            func = partial(get_all_std_dev, tile_service=self._get_tile_service(), ds=ds, longitude=longitude,
                           latitude=latitude, day_of_year=day_of_year)
        else:
            func = partial(get_single_std_dev, tile_service=self._get_tile_service(), ds=ds, longitude=longitude,
                           latitude=latitude, day_of_year=day_of_year)

        try:
            results = StandardDeviationSearchCalcHandlerImpl.to_result_dict(func())
        except (NoTileException, NoStandardDeviationException):
            return StandardDeviationSearchResult(raw_args_dict, [])

        return StandardDeviationSearchResult(raw_args_dict, results)

    @staticmethod
    def to_result_dict(list_of_tuples):
        # list_of_tuples = [(lon, lat, st_dev)]
        return [
            {
                "longitude": lon,
                "latitude": lat,
                "standard_deviation": st_dev
            } for lon, lat, st_dev in list_of_tuples]


class NoTileException(Exception):
    pass


class NoStandardDeviationException(Exception):
    pass


def find_tile_and_std_name(tile_service, ds, longitude, latitude, day_of_year):
    from shapely.geometry import Point
    point = Point(longitude, latitude)

    try:
        tile = tile_service.find_tile_by_polygon_and_most_recent_day_of_year(point, ds, day_of_year)[0]
    except NexusTileServiceException:
        raise NoTileException

    # Check if this tile has any meta data that ends with 'std'. If it doesn't, just return nothing.
    try:
        st_dev_meta_name = next(iter([key for key in tile.meta_data.keys() if key.endswith('std')]))
    except StopIteration:
        raise NoStandardDeviationException

    return tile, st_dev_meta_name


def get_single_std_dev(tile_service, ds, longitude, latitude, day_of_year):
    from scipy.spatial import distance

    tile, st_dev_meta_name = find_tile_and_std_name(tile_service, ds, longitude, latitude, day_of_year)

    # Need to find the closest point in the tile to the input lon/lat point and return only that result
    valid_indices = tile.get_indices()
    tile_points = [tuple([tile.longitudes[lon_idx], tile.latitudes[lat_idx]]) for time_idx, lat_idx, lon_idx in
                   valid_indices]
    closest_point_index = distance.cdist([(longitude, latitude)], tile_points).argmin()
    closest_lon, closest_lat = tile_points[closest_point_index]
    closest_point_tile_index = tuple(valid_indices[closest_point_index])
    std_at_point = tile.meta_data[st_dev_meta_name][closest_point_tile_index]
    return [tuple([closest_lon, closest_lat, std_at_point])]


def get_all_std_dev(tile_service, ds, longitude, latitude, day_of_year):
    tile, st_dev_meta_name = find_tile_and_std_name(tile_service, ds, longitude, latitude, day_of_year)

    valid_indices = tile.get_indices()
    return [tuple([tile.longitudes[lon_idx], tile.latitudes[lat_idx],
                   tile.meta_data[st_dev_meta_name][time_idx, lat_idx, lon_idx]]) for time_idx, lat_idx, lon_idx in
            valid_indices]


class StandardDeviationSearchResult(object):
    def __init__(self, request_params, results):
        self.request_params = request_params
        self.results = results

    def toJson(self):
        data = {
            'meta': self.request_params,
            'data': self.results,
            'stats': {}
        }
        return json.dumps(data, indent=4, cls=CustomEncoder)
