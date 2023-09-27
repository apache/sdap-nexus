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
from typing import Dict, Literal, Union
from numbers import Number
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.algorithms.DataInBoundsSearch import DataInBoundsSearchCalcHandlerImpl as Subset
from webservice.webmodel import NexusResults, NexusProcessingException


logger = logging.getLogger(__name__)


class TomogramBaseClass(NexusCalcHandler):
    def __init__(
            self,
            tile_service_factory,
            slice_bounds: Dict[Literal['lat', 'lon', 'elevation'], Union[slice, Number]],
            **kwargs
    ):
        NexusCalcHandler.__init__(self, tile_service_factory)
        self.__slice_bounds = slice_bounds
        self.__margin = kwargs.get('margin', 0.001)

        # slice bounds: dict relating dimension names to desired slicing
        # When dealing with multi-var tiles, optional parameter to pick variable to plot,
        # otherwise issue warning and pick the first one

    def parse_args(self, compute_options):
        try:
            ds = compute_options.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        parameter_s = compute_options.get_argument('parameter', None)

        return ds, parameter_s

    def do_subset(self, compute_options):
        tile_service = self._get_tile_service()

        ds, parameter = self.parse_args(compute_options)

        bounds = self.__slice_bounds
        sel = {}

        if isinstance(bounds['lat'], slice):
            min_lat = bounds['lat'].start
            max_lat = bounds['lat'].stop
        else:
            min_lat = bounds['lat'] - self.__margin
            max_lat = bounds['lat'] + self.__margin

            sel = dict(lat=bounds['lat'])

        if isinstance(bounds['lon'], slice):
            min_lon = bounds['lon'].start
            max_lon = bounds['lon'].stop
        else:
            min_lon = bounds['lon'] - self.__margin
            max_lon = bounds['lon'] + self.__margin

            sel = dict(lon=bounds['lon'])

        if isinstance(bounds['elevation'], slice):
            min_elevation = bounds['elevation'].start
            max_elevation = bounds['elevation'].stop
        else:
            min_elevation = bounds['elevation'] - self.__margin
            max_elevation = bounds['elevation'] + self.__margin

            sel = dict(elevation=bounds['elevation'])

        tiles = tile_service.find_tiles_in_box(
            min_lat, max_lat, min_lon, max_lon,
            ds=ds,
            min_elevation=min_elevation,
            max_elevation=max_elevation,
            fetch_data=False
        )

        logger.info(f'Fetched {len(tiles):,}')

        data = []

        for i in range(len(tiles)-1, -1, -1): # tile in tiles:
            tile = tiles.pop(i)

            tile_id = tile.tile_id

            logger.info(f'Processing tile {tile_id} | {i=}')

            tile = tile_service.fetch_data_for_tiles(tile)[0]
            tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, [tile])
            # tile = tile_service.mask_tiles_to_time_range(start_time, end_time, tile)

            if min_elevation and max_elevation:
                tile = tile_service.mask_tiles_to_elevation(min_elevation, max_elevation, tile)

            if len(tile) == 0:
                logger.info(f'Skipping empty tile {tile_id}')
                continue

            tile = tile[0]

            for nexus_point in tile.nexus_point_generator():
                data_vals = nexus_point.data_vals if tile.is_multi else [nexus_point.data_vals]
                data_val = None

                for value, variable in zip(data_vals, tile.variables):
                    if parameter is None or variable == parameter:
                        data_val = value
                        break

                if data_val is None:
                    logger.warning(f'No variable {parameter} found at point {nexus_point.index} for tile {tile.tile_id}')
                    data_val = np.nan

                data.append({
                    'latitude': nexus_point.latitude,
                    'longitude': nexus_point.longitude,
                    'data': data_val
                })

        logger.info('Fetched data, organizing it for plotting')

        return data


# @nexus_handler
class LatitudeTomogramImpl(TomogramBaseClass):
    pass


# @nexus_handler
class LongitudeTomogramImpl(TomogramBaseClass):
    pass


@nexus_handler
class ElevationTomogramImpl(TomogramBaseClass):
    name = "Elevation-sliced Tomogram"
    path = "/tomogram/elevation"
    description = "Fetches point values for a given dataset and geographical area"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation. Required"
        },
        "parameter": {
            "name": "Parameter",
            "type": "string",
            "description": "The parameter of interest."
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required if 'metadataFilter' not provided"
        },
        "elevation": {
            "name": "Slice elevation",
            "type": "float",
            "description": "The "
        }
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        TomogramBaseClass.__init__(self, tile_service_factory, **kwargs)

    def parse_args(self, compute_options):
        pass

    def calc(self, computeOptions, **args):
        pass
