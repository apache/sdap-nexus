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
import math
from itertools import groupby

import numpy as np
from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon
from scipy.stats import linregress
from shapely.geometry import box

from webservice.NexusHandler import nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusProcessingException, NexusResults


@nexus_handler
class LongitudeLatitudeMapCalcHandlerImpl(NexusCalcHandler):
    name = "Correlation Map"
    path = "/correlationMap"
    description = "Computes a correlation map between two datasets given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    params["res"] = {
        "name": "Resolution",
        "type": "float",
        "description": "The resolution of the resulting correlation map"
    }
    singleton = True

    def calc(self, computeOptions, **args):
        minLat = computeOptions.get_min_lat()
        maxLat = computeOptions.get_max_lat()
        minLon = computeOptions.get_min_lon()
        maxLon = computeOptions.get_max_lon()
        ds = computeOptions.get_dataset()
        startTime = computeOptions.get_start_time()
        endTime = computeOptions.get_end_time()
        resolution = computeOptions.get_decimal_arg("res", default=1.0)

        if not len(ds) == 2:
            raise Exception("Requires two datasets for comparison. Specify request parameter ds=Dataset_1,Dataset_2")

        ds1tiles = self._get_tile_service().find_tiles_in_polygon(box(minLon, minLat, maxLon, maxLat), ds[0], startTime,
                                                            endTime)
        ds2tiles = self._get_tile_service().find_tiles_in_polygon(box(minLon, minLat, maxLon, maxLat), ds[1], startTime,
                                                            endTime)

        matches = self._match_tiles(ds1tiles, ds2tiles)

        if len(matches) == 0:
            raise NexusProcessingException(reason="Could not find any data temporally co-located")

        results = [[{
            'cnt': 0,
            'slope': 0,
            'intercept': 0,
            'r': 0,
            'p': 0,
            'stderr': 0,
            'lat': float(lat),
            'lon': float(lon)
        } for lon in np.arange(minLon, maxLon, resolution)] for lat in
            np.arange(minLat, maxLat, resolution)]

        for stats in results:
            for stat in stats:
                values_x = []
                values_y = []
                for tile_matches in matches:

                    tile_1_list = tile_matches[0]
                    value_1 = get_approximate_value_for_lat_lon(tile_1_list, stat["lat"], stat["lon"])

                    tile_2_list = tile_matches[1]
                    value_2 = get_approximate_value_for_lat_lon(tile_2_list, stat["lat"], stat["lon"])

                    if not (math.isnan(value_1) or math.isnan(value_2)):
                        values_x.append(value_1)
                        values_y.append(value_2)

                if len(values_x) > 2 and len(values_y) > 2:
                    stats = linregress(values_x, values_y)

                    stat["slope"] = stats[0] if not math.isnan(stats[0]) and not math.isinf(stats[0]) else str(stats[0])
                    stat["intercept"] = stats[1] if not math.isnan(stats[1]) and not math.isinf(stats[1]) else str(
                        stats[1])
                    stat["r"] = stats[2] if not math.isnan(stats[2]) and not math.isinf(stats[2]) else str(stats[2])
                    stat["p"] = stats[3] if not math.isnan(stats[3]) and not math.isinf(stats[3]) else str(stats[3])
                    stat["stderr"] = stats[4] if not math.isnan(stats[4]) and not math.isinf(stats[4]) else str(
                        stats[4])
                    stat["cnt"] = len(values_x)

        return CorrelationResults(results)

    def _match_tiles(self, tiles_1, tiles_2):

        date_map = {}

        tiles_1 = sorted(tiles_1, key=lambda tile: tile.min_time)
        for tile_start_time, tiles in groupby(tiles_1, lambda tile: tile.min_time):
            if not tile_start_time in date_map:
                date_map[tile_start_time] = []
            date_map[tile_start_time].append(list(tiles))

        tiles_2 = sorted(tiles_2, key=lambda tile: tile.min_time)
        for tile_start_time, tiles in groupby(tiles_2, lambda tile: tile.min_time):
            if not tile_start_time in date_map:
                date_map[tile_start_time] = []
            date_map[tile_start_time].append(list(tiles))

        matches = [tiles for a_time, tiles in date_map.iteritems() if len(tiles) == 2]

        return matches


class CorrelationResults(NexusResults):
    def __init__(self, results):
        NexusResults.__init__(self)
        self.results = results

    def toJson(self):
        json_d = {
            "stats": {},
            "meta": [None, None],
            "data": self.results
        }
        return json.dumps(json_d, indent=4)
