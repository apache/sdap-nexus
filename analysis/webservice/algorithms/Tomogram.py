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
from io import BytesIO
from typing import Dict, Literal, Union

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException

logger = logging.getLogger(__name__)


class TomogramBaseClass(NexusCalcHandler):
    def __init__(
            self,
            tile_service_factory,
            **kwargs
    ):
        NexusCalcHandler.__init__(self, tile_service_factory)
        self.__slice_bounds = None
        self.__margin = None

    def _set_params(
            self,
            slice_bounds: Dict[Literal['lat', 'lon', 'elevation'], Union[slice, float]],
            margin: float
    ):
        self.__slice_bounds = slice_bounds
        self.__margin = margin

    def parse_args(self, compute_options):
        try:
            ds = compute_options.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        parameter_s = compute_options.get_argument('parameter', None)

        return ds, parameter_s

    def do_subset(self, ds, parameter):
        tile_service = self._get_tile_service()

        bounds = self.__slice_bounds

        if isinstance(bounds['lat'], slice):
            min_lat = bounds['lat'].start
            max_lat = bounds['lat'].stop
        else:
            min_lat = bounds['lat'] - self.__margin
            max_lat = bounds['lat'] + self.__margin

        if isinstance(bounds['lon'], slice):
            min_lon = bounds['lon'].start
            max_lon = bounds['lon'].stop
        else:
            min_lon = bounds['lon'] - self.__margin
            max_lon = bounds['lon'] + self.__margin

        if isinstance(bounds['elevation'], slice):
            min_elevation = bounds['elevation'].start
            max_elevation = bounds['elevation'].stop
        else:
            min_elevation = bounds['elevation'] - self.__margin
            max_elevation = bounds['elevation'] + self.__margin

        tiles = tile_service.find_tiles_in_box(
            min_lat, max_lat, min_lon, max_lon,
            ds=ds,
            min_elevation=min_elevation,
            max_elevation=max_elevation,
            fetch_data=False
        )

        logger.info(f'Fetched {len(tiles):,}')

        data = []

        for i in range(len(tiles)-1, -1, -1):
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


# # @nexus_handler
# class LatitudeTomogramImpl(TomogramBaseClass):
#     pass
#
#
# # @nexus_handler
# class LongitudeTomogramImpl(TomogramBaseClass):
#     pass


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
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required."
        },
        "elevation": {
            "name": "Slice elevation",
            "type": "float",
            "description": "The desired elevation of the tomogram slice"
        },
        "margin": {
            "name": "Margin",
            "type": "float",
            "description": "Margin +/- desired elevation to include in output. Default: 0.5m"
        }
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        TomogramBaseClass.__init__(self, tile_service_factory, **kwargs)

    def parse_args(self, compute_options):
        try:
            bounding_poly = compute_options.get_bounding_polygon()
        except:
            raise NexusProcessingException(reason='Missing required parameter: b', code=400)

        elevation = compute_options.get_float_arg('elevation', None)

        if elevation is None:
            raise NexusProcessingException(reason='Missing required parameter: elevation', code=400)

        margin = compute_options.get_float_arg('margin', 0.5)

        ds, parameter = super().parse_args(compute_options)

        return ds, parameter, bounding_poly, elevation, margin

    def calc(self, computeOptions, **args):
        ds, parameter, bounding_poly, elevation, margin = self.parse_args(computeOptions)

        min_lat = bounding_poly.bounds[1]
        max_lat = bounding_poly.bounds[3]
        min_lon = bounding_poly.bounds[0]
        max_lon = bounding_poly.bounds[2]

        slices = dict(
            lat=slice(min_lat, max_lat),
            lon=slice(min_lon, max_lon),
            elevation=float(elevation)
        )

        self._set_params(slices, margin)
        data_in_bounds = self.do_subset(ds, parameter)

        lats = np.unique([d['latitude'] for d in data_in_bounds])
        lons = np.unique([d['longitude'] for d in data_in_bounds])

        vals = np.empty((len(lats), len(lons)))

        data_dict = {(d['latitude'], d['longitude']): d['data'] for d in data_in_bounds}

        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                vals[i, j] = data_dict.get((lat, lon), np.nan)

        ds = xr.Dataset(
            data_vars=dict(
                tomo=(('latitude', 'longitude'), vals)
            ),
            coords=dict(
                latitude=(['latitude'], lats),
                longitude=(['longitude'], lons)
            ),
            attrs=dict(
                ds=ds,
                elevation=elevation,
                margin=margin
            )
        )

        result = ElevationTomoResults(
            (vals, lats, lons, dict(ds=ds, elevation=elevation, margin=margin)),
        )

        return result

class ElevationTomoResults(NexusResults):
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, **args):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **args)

    def toImage(self):
        data, lats, lons, attrs = self.results()

        lats = lats.tolist()
        lons = lons.tolist()

        plt.figure(figsize=(15,11))
        plt.imshow(np.squeeze(10*np.log10(data)), vmax=-10, vmin=-30)
        plt.colorbar(label=f'Tomogram, z={attrs["elevation"]} ± {attrs["margin"]} m (dB)')
        plt.title(f'Tomogram @ {attrs["elevation"]} ± {attrs["margin"]} m elevation')
        plt.ylabel('Latitude')
        plt.xlabel('Longitude')

        xticks, xlabels = plt.xticks()
        xlabels = [f'{lons[int(t)]:.4f}' if int(t) in range(len(lons)) else '' for t in xticks]
        plt.xticks(xticks, xlabels, rotation=-90)

        yticks, ylabels = plt.yticks()
        ylabels = [f'{lats[int(t)]:.4f}' if int(t) in range(len(lats)) else '' for t in yticks]
        plt.yticks(yticks, ylabels, )

        buffer = BytesIO()

        plt.savefig(buffer, format='png', facecolor='white')

        buffer.seek(0)
        return buffer.read()

    def toJson(self):
        pass
