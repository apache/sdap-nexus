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


import contextlib
import logging
import os
import random
import re
import zipfile
from bisect import bisect_left
from io import BytesIO
from math import ceil, sqrt, isnan
from tempfile import TemporaryDirectory
from typing import Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from PIL import Image
from geopy.distance import geodesic
from shapely.geometry import Point, LineString

from nexustiles.exception import NexusTileServiceException
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

logger = logging.getLogger(__name__)
FLOAT_PATTERN = re.compile(r'^[+-]?(\d+(\.\d*)?|\.\d+)$')


class TomogramBaseClass(NexusCalcHandler):
    def __init__(
            self,
            tile_service_factory,
            **kwargs
    ):
        NexusCalcHandler.__init__(self, tile_service_factory)

    def parse_args(self, compute_options):
        try:
            ds = compute_options.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        parameter_s = compute_options.get_argument('parameter', None)

        return ds, parameter_s

    def do_subset(self, ds, parameter, bounds, margin):
        tile_service = self._get_tile_service()

        # bounds = self.__slice_bounds

        if isinstance(bounds['lat'], slice):
            min_lat = bounds['lat'].start
            max_lat = bounds['lat'].stop
        else:
            min_lat = bounds['lat'] - margin
            max_lat = bounds['lat'] + margin

        if isinstance(bounds['lon'], slice):
            min_lon = bounds['lon'].start
            max_lon = bounds['lon'].stop
        else:
            min_lon = bounds['lon'] - margin
            max_lon = bounds['lon'] + margin

        if isinstance(bounds['elevation'], slice):
            min_elevation = bounds['elevation'].start
            max_elevation = bounds['elevation'].stop
        else:
            min_elevation = bounds['elevation'] - margin
            max_elevation = bounds['elevation'] + margin

        # In case slices are unbounded (ie, start/stop is None) replace the associated values with max/min for coord
        min_lat, min_lon, max_lat, max_lon = [v if v is not None else d for v, d in
                                              zip([min_lat, min_lon, max_lat, max_lon], [-90, -180, 90, 180])]

        tiles = tile_service.find_tiles_in_box(
            min_lat, max_lat, min_lon, max_lon,
            ds=ds,
            min_elevation=min_elevation,
            max_elevation=max_elevation,
            fetch_data=False
        )

        logger.info(f'Matched {len(tiles):,} tiles from Solr')

        if len(tiles) == 0:
            raise NoDataException(reason='No tiles matched the selected parameters')

        data = []

        for i in range(len(tiles)-1, -1, -1):
            tile = tiles.pop(i)

            tile_id = tile.tile_id

            logger.info(f'Processing tile {tile_id} | {i=}')

            tile = tile_service.fetch_data_for_tiles(tile)[0]
            tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, [tile])

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
                    'elevation': nexus_point.depth,
                    'data': data_val
                })

        if len(data) == 0:
            raise NoDataException(reason='No data fit the selected parameters')

        return data

    @staticmethod
    def data_subset_to_ds(data_in_bounds: list) -> Tuple[np.ndarray, np.ndarray, np.ndarray, xr.Dataset]:
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
            )
        )

        return lats, lons, vals, ds

    @staticmethod
    def bin_subset_elevations_to_range(data_in_bounds: list, elevation_range: np.ndarray, margin):
        binned_subset = []

        for d in data_in_bounds:
            elevation: float = d['elevation']
            d_copy = dict(**d)

            pos = bisect_left(elevation_range, elevation)

            if pos == 0:
                binned_elev = elevation_range[0]
            elif pos == len(elevation_range):
                binned_elev = elevation_range[-1]
            else:
                before = elevation_range[pos - 1]
                after = elevation_range[pos]
                if after - elevation < elevation - before:
                    binned_elev = after
                else:
                    binned_elev = before

            if abs(binned_elev - elevation) > margin:
                logger.warning(f'Could not bin point {d_copy}')
            else:
                d_copy['elevation'] = binned_elev
                binned_subset.append(d_copy)

        return binned_subset

    @staticmethod
    def data_subset_to_ds_with_elevation(
            data_in_bounds: list
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, xr.Dataset]:
        lats = np.unique([d['latitude'] for d in data_in_bounds])
        lons = np.unique([d['longitude'] for d in data_in_bounds])
        elevations = np.unique([d['elevation'] for d in data_in_bounds])

        # Commented out is the old gridding method
        #
        # vals = np.empty((len(elevations), len(lats), len(lons)))
        #
        # time = elapsed('data_subset_to_ds_with_elevation: val init', time)
        #
        # data_dict = {(d['elevation'], d['latitude'], d['longitude']): d['data'] for d in data_in_bounds}
        #
        # time = elapsed('data_subset_to_ds_with_elevation: dict build', time)
        #
        # print(f'{len(elevations) * len(lats) * len(lons):,}')
        #
        # for i, elev in enumerate(elevations):
        #     for j, lat in enumerate(lats):
        #         for k, lon in enumerate(lons):
        #             vals[i, j, k] = data_dict.get((elev, lat, lon), np.nan)
        #
        # time = elapsed('data_subset_to_ds_with_elevation: populate', time)

        vals = np.full((len(elevations), len(lats), len(lons)), np.nan)

        elevation_idxs = {}
        lat_idxs = {}
        lon_idxs = {}

        for point in data_in_bounds:
            elevation = point['elevation']
            lat = point['latitude']
            lon = point['longitude']

            if elevation not in elevation_idxs:
                e_i = np.argwhere(elevations == elevation)
                elevation_idxs[elevation] = e_i
            else:
                e_i = elevation_idxs[elevation]

            if lat not in lat_idxs:
                lat_i = np.argwhere(lats == lat)
                lat_idxs[lat] = lat_i
            else:
                lat_i = lat_idxs[lat]

            if lon not in lon_idxs:
                lon_i = np.argwhere(lons == lon)
                lon_idxs[lon] = lon_i
            else:
                lon_i = lon_idxs[lon]

            vals[e_i, lat_i, lon_i] = point['data']

        ds = xr.Dataset(
            data_vars=dict(
                tomo=(('elevation', 'latitude', 'longitude'), vals)
            ),
            coords=dict(
                latitude=(['latitude'], lats),
                longitude=(['longitude'], lons),
                elevation=(['elevation'], elevations)
            )
        )

        return lats, lons, vals, ds

    @staticmethod
    def add_variables_to_tomo(tomo_ds, **variables):
        X, Y = np.meshgrid(tomo_ds.longitude.to_numpy(), tomo_ds.latitude.to_numpy())

        for var in variables:
            try:
                xr.align(tomo_ds['tomo'], variables[var].tomo, join='exact')
                tomo_ds[var] = variables[var].tomo
            except:
                from scipy.interpolate import griddata

                logger.info(f'Interpolating map to tomo grid')

                var_ds = variables[var]
                data_points = []

                for lon_i, lon in enumerate(var_ds.longitude):
                    for lat_i, lat in enumerate(var_ds.latitude):
                        val = var_ds.isel(latitude=lat_i, longitude=lon_i).tomo.item()

                        data_points.append((lon, lat, val))

                regridded = griddata(
                    list(zip(
                        [p[0] for p in data_points], [p[1] for p in data_points]
                    )),
                    np.array([p[2] for p in data_points]),
                    (X, Y),
                    method='nearest',
                    fill_value=np.nan
                )

                tomo_ds[var] = xr.DataArray(
                    data=regridded,
                    dims=['latitude', 'longitude'],
                    coords=dict(
                        latitude=tomo_ds.latitude,
                        longitude=tomo_ds.longitude
                    ),
                    name=var
                )

    @staticmethod
    def _get_elevation_step_size(data):
        tries = 5
        sample = [1]

        while tries >= 0:
            tries -= 1
            sample = data[random.randint(0, len(data) - 1)]

            sample = [d['elevation'] for d in data if
                      d['latitude'] == sample['latitude'] and d['longitude'] == sample['longitude']]
            sample.sort()

            sample = [sample[i+1] - sample[i] for i in range(len(sample) - 1)]

            if min(sample) == max(sample):
                break
            elif tries < 0:
                raise NexusProcessingException(
                    code=500,
                    reason=f'There appears to be non-uniform elevation slices in the subset: '
                           f'{sample}, {min(sample)}, {max(sample)}, {len(sample)}'
                )
            else:
                logger.warning(f'Possible bad elevation sampling. Trying again. '
                               f'{min(sample)}, {max(sample)}, {len(sample)}')

        return sample[0]


# @nexus_handler
class ElevationTomogramImpl(TomogramBaseClass):
    name = "Elevation-sliced Tomogram"
    path = "/tomogram/elevation"
    description = "Fetches a tomogram sliced by elevation"
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

    def calc(self, compute_options, **args):
        dataset, parameter, bounding_poly, elevation, margin = self.parse_args(compute_options)

        min_lat = bounding_poly.bounds[1]
        max_lat = bounding_poly.bounds[3]
        min_lon = bounding_poly.bounds[0]
        max_lon = bounding_poly.bounds[2]

        slices = dict(
            lat=slice(min_lat, max_lat),
            lon=slice(min_lon, max_lon),
            elevation=float(elevation)
        )

        data_in_bounds = self.do_subset(dataset, parameter, slices, margin)
        logger.info(f'Fetched {len(data_in_bounds):,} data points, organizing them for plotting')

        lats, lons, vals, ds = TomogramBaseClass.data_subset_to_ds(data_in_bounds)

        result = ElevationTomoResults(
            (vals, lats, lons, dict(ds=dataset, elevation=elevation, margin=margin)),
        )

        return result


@nexus_handler
class LongitudeTomogramImpl(TomogramBaseClass):
    name = "Tomogram Latitude profile view"
    path = "/tomogram/longitude"
    description = "Fetches a tomogram sliced by longitude"
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
        "longitude": {
            "name": "Longitude",
            "type": "float, comma-delimited floats(>1), colon-delimited floats(<=3), literal['*']",
            "description": "Longitude(s) at which to slice the tomogram. Required. Valid formats: Single float - Slice "
                           "at a singular longitude; comma-delimited floats - slice at a list of longitudes; "
                           "colon-delimited floats - slice using Python slicing notation (both ends are inclusive): "
                           "[<start>]:[<stop>]:[<step>] where omitting <start> and/or <stop> produce unbounded slices "
                           "and omitting <step> equates to an all-inclusive step size. A special value of \"*\" "
                           "equates to ::1. Values other than singular floats are incompatible with PNG rendering "
                           "(use ZIP to a ZIP archive of .pngs)"
        },
        "minLat": {
            "name": "Minimum Latitude",
            "type": "float",
            "description": "Minimum (Southern) Latitude. Required."
        },
        "maxLat": {
            "name": "Maximum Latitude",
            "type": "float",
            "description": "Maximum (Northern) Latitude. Required."
        },
        "minElevation": {
            "name": "Minimum Elevation",
            "type": "int",
            "description": "Minimum Elevation. Required."
        },
        "maxElevation": {
            "name": "Maximum Elevation",
            "type": "int",
            "description": "Maximum Elevation. Required."
        },
        "horizontalMargin": {
            "name": "Horizontal Margin",
            "type": "float",
            "description": "Margin +/- desired lat/lon slice to include in output. Default: 0.001m"
        },
        "peaks": {
            "name": "Calculate peaks",
            "type": "boolean",
            "description": "Calculate peaks along tomogram slice (currently uses simplest approach). "
                           "Ignored with \'elevPercentiles\'"
        },
        "canopy_ds": {
            "name": "Canopy height dataset name",
            "type": "string",
            "description": "Dataset containing canopy heights (RH98). This is used to trim out tomogram voxels that "
                           "return over the measured or predicted canopy"
        },
        "ground_ds": {
            "name": "Ground height dataset name",
            "type": "string",
            "description": "Dataset containing ground height (DEM). This is used to trim out tomogram voxels that "
                           "return below the measured or predicted ground"
        },
        "elevPercentiles": {
            "name": "Elevation percentiles",
            "type": "boolean",
            "description": "Display percentiles of cumulative returns over elevation. Requires both canopy_ds and "
                           "ground_ds be set."
        },
        "cmap": {
            "name": "Color Map",
            "type": "string",
            "description": f"Color map to use. Will default to viridis if not provided or an unsupported map is "
                           f"provided. Supported cmaps: {[k for k in sorted(mpl.colormaps.keys())]}"
        },
        "cbarMin": {
            "name": "Color bar min",
            "type": "float",
            "description": "Minimum value of the color bar. Only applies if elevPercentiles is False. Value must be "
                           "less than cbarMax. Invalid configuration will be replaced by default values (-30)"
        },
        "cbarMax": {
            "name": "Color bar max",
            "type": "float",
            "description": "Maximum value of the color bar. Only applies if elevPercentiles is False. Value must be "
                           " greater than cbarMin. Invalid configuration will be replaced by default value (-10)"
        },
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        TomogramBaseClass.__init__(self, tile_service_factory, **kwargs)

    def parse_args(self, compute_options):
        ds, parameter = super().parse_args(compute_options)

        def get_required_float(name):
            val = compute_options.get_float_arg(name, None)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        def get_required_int(name):
            val = compute_options.get_int_arg(name, None)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        longitude = compute_options.get_argument('longitude', None)
        output = compute_options.get_content_type().lower()

        if longitude is None:
            raise NexusProcessingException(reason='Missing required parameter: longitude', code=400)
        if re.match(FLOAT_PATTERN, longitude):
            longitude = float(longitude)

            if longitude < -180 or longitude > 180:
                raise NexusProcessingException(reason='Invalid longitude: out of range', code=400)

            longitude = [longitude]
        elif ',' in longitude:
            if output == 'png':
                raise NexusProcessingException(reason='Cannot use PNG output with multi-longitude slicing', code=400)

            try:
                longitude = list(sorted([float(l) for l in longitude.split(',')]))
            except ValueError:
                raise NexusProcessingException(reason='Invalid longitude: invalid format', code=400)

            if any([l < -180 or l > 180 for l in longitude]):
                raise NexusProcessingException(reason='Invalid longitude: out of range', code=400)
        elif ':' in longitude:
            if output == 'png':
                raise NexusProcessingException(reason='Cannot use PNG output with multi-longitude slicing', code=400)

            try:
                longitude = slice(*(float(p) if p else None for p in longitude.split(':')))
            except:
                raise NexusProcessingException(reason='Invalid longitude: invalid format', code=400)

            if longitude.start is not None and longitude.stop is not None and longitude.start >= longitude.stop:
                raise NexusProcessingException(reason='Invalid longitude: out of order', code=400)

            # Some helpers for a < b / a > b where a could be None and return false in that case
            def lt(a, b):
                return a is not None and a < b

            def gt(a, b):
                return a is not None and a > b

            if (lt(longitude.start, -180) or gt(longitude.start, 180) or
                    lt(longitude.stop, -180) or gt(longitude.stop, 180)):
                raise NexusProcessingException(reason='Invalid longitude: out of range', code=400)
        elif longitude == '*':
            if output == 'png':
                raise NexusProcessingException(reason='Cannot use PNG output with multi-longitude slicing', code=400)

            longitude = slice(None, None, None)
        else:
            raise NexusProcessingException(reason='Invalid longitude argument', code=400)

        min_lat = get_required_float('minLat')
        max_lat = get_required_float('maxLat')
        min_elevation = get_required_int('minElevation')
        max_elevation = get_required_int('maxElevation')

        horizontal_margin = compute_options.get_float_arg('horizontalMargin', 0.001)

        ch_ds = compute_options.get_argument('canopy_ds', None)
        g_ds = compute_options.get_argument('ground_ds', None)

        percentiles = compute_options.get_boolean_arg('elevPercentiles')

        if percentiles and (ch_ds is None or g_ds is None):
            raise NexusProcessingException(
                code=400,
                reason='\'elevPercentiles\' argument requires \'canopy_ds\' and \'ground_ds\' be set'
            )

        peaks = compute_options.get_boolean_arg('peaks') and not percentiles

        cmap = mpl.colormaps.get(
            compute_options.get_argument('cmap', 'viridis'),
            mpl.colormaps['viridis']
        )

        return (ds, parameter, longitude, min_lat, max_lat, min_elevation, max_elevation, horizontal_margin, peaks,
                ch_ds, g_ds, percentiles, cmap)

    def calc(self, compute_options, **args):
        (dataset, parameter, longitude, min_lat, max_lat, min_elevation, max_elevation, horizontal_margin, calc_peaks,
         ch_ds, g_ds, percentiles, cmap) = self.parse_args(compute_options)

        slices = dict(
            lat=slice(min_lat, max_lat),
            elevation=slice(min_elevation, max_elevation)
        )

        if isinstance(longitude, slice):
            slices['lon'] = longitude
        elif isinstance(longitude, list):
            slices['lon'] = slice(longitude[0] - horizontal_margin, longitude[-1] + horizontal_margin)
        else:
            slices['lon'] = slice(longitude - horizontal_margin, longitude + horizontal_margin)

        data_in_bounds = self.do_subset(dataset, parameter, slices, 0)
        z_step = TomogramBaseClass._get_elevation_step_size(data_in_bounds)

        r = np.arange(min_elevation, max_elevation + z_step, z_step)

        logger.info(f'Fetched {len(data_in_bounds):,} data points at this elevation')

        ds = TomogramBaseClass.data_subset_to_ds_with_elevation(
            TomogramBaseClass.bin_subset_elevations_to_range(data_in_bounds, r, z_step / 2)
        )[3]

        slices['elevation'] = slice(None, None)
        elev_vars = {}

        if ch_ds is not None:
            elev_vars['ch'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(ch_ds, parameter, slices, 0))[3]

        if g_ds is not None:
            elev_vars['gh'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(g_ds, parameter, slices, 0))[3]

        if elev_vars:
            TomogramBaseClass.add_variables_to_tomo(ds, **elev_vars)

        if isinstance(longitude, list):
            lon_slices = [ds.sel(longitude=l, method='nearest') for l in longitude]
            ds = xr.concat(lon_slices, dim='longitude').drop_duplicates('longitude')
        else:
            ds = ds.sel(longitude=longitude)

        if 'ch' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation <= ds.ch)
        if 'gh' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation >= ds.gh)

        if percentiles:
            tomo = ds['tomo'].cumsum(dim='elevation', skipna=True).where(ds['tomo'].notnull())
            ds['tomo'] = tomo / tomo.max(dim='elevation', skipna=True)
        else:
            ds['tomo'] = xr.apply_ufunc(lambda a: 10 * np.log10(a), ds.tomo)

        if calc_peaks:
            peaks = []

            for i in range(len(ds.tomo.longitude)):
                lon_peaks = []
                ds_sub = ds.isel(longitude=i)

                for j in range(len(ds.tomo.latitude)):
                    col = ds_sub.isel(latitude=j)

                    lat = col.latitude.item()

                    try:
                        idx = col.argmax(dim='elevation').tomo.item()

                        lon_peaks.append((lat, col.isel(elevation=idx).elevation.item()))

                    except ValueError:
                        lon_peaks.append((lat, np.nan))

                peaks.append((np.array([p[0] for p in lon_peaks]),
                              np.array([p[1] for p in lon_peaks])))
        else:
            peaks = None

        return ProfileTomoResults(
            results=(ds, peaks),
            s={'longitude': longitude},
            coords=dict(x=ds.latitude, y=ds.elevation),
            meta=dict(dataset=dataset),
            style='db' if not percentiles else 'percentile',
            cmap=cmap,
            computeOptions=compute_options
        )


@nexus_handler
class LatitudeTomogramImpl(TomogramBaseClass):
    name = "Tomogram Latitude profile view"
    path = "/tomogram/latitude"
    description = "Fetches a tomogram sliced by latitude"
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
        "latitude": {
            "name": "Latitude",
            "type": "float, comma-delimited floats(>1), colon-delimited floats(<=3), literal['*']",
            "description": "Latitude(s) at which to slice the tomogram. Required. Valid formats: Single float - Slice "
                           "at a singular latitude; comma-delimited floats - slice at a list of latitudes; "
                           "colon-delimited floats - slice using Python slicing notation (both ends are inclusive): "
                           "[<start>]:[<stop>]:[<step>] where omitting <start> and/or <stop> produce unbounded slices "
                           "and omitting <step> equates to an all-inclusive step size. A special value of \"*\" "
                           "equates to ::1. Values other than singular floats are incompatible with PNG rendering "
                           "(use ZIP to a ZIP archive of .pngs)"
        },
        "minLon": {
            "name": "Minimum Longitude",
            "type": "float",
            "description": "Minimum (Eastern) Longitude. Required."
        },
        "maxLon": {
            "name": "Maximum Longitude",
            "type": "float",
            "description": "Maximum (Western) Longitude. Required."
        },
        "minElevation": {
            "name": "Minimum Elevation",
            "type": "int",
            "description": "Minimum Elevation. Required."
        },
        "maxElevation": {
            "name": "Maximum Elevation",
            "type": "int",
            "description": "Maximum Elevation. Required."
        },
        "horizontalMargin": {
            "name": "Horizontal Margin",
            "type": "float",
            "description": "Margin +/- desired lat/lon slice to include in output. Default: 0.001m"
        },
        "peaks": {
            "name": "Calculate peaks",
            "type": "boolean",
            "description": "Calculate peaks along tomogram slice (currently uses simplest approach). "
                           "Ignored with \'elevPercentiles\'"
        },
        "canopy_ds": {
            "name": "Canopy height dataset name",
            "type": "string",
            "description": "Dataset containing canopy heights (RH98). This is used to trim out tomogram voxels that "
                           "return over the measured or predicted canopy"
        },
        "ground_ds": {
            "name": "Ground height dataset name",
            "type": "string",
            "description": "Dataset containing ground height (DEM). This is used to trim out tomogram voxels that "
                           "return below the measured or predicted ground"
        },
        "elevPercentiles": {
            "name": "Elevation percentiles",
            "type": "boolean",
            "description": "Display percentiles of cumulative returns over elevation. Requires both canopy_ds and "
                           "ground_ds be set."
        },
        "cmap": {
            "name": "Color Map",
            "type": "string",
            "description": f"Color map to use. Will default to viridis if not provided or an unsupported map is "
                           f"provided. Supported cmaps: {[k for k in sorted(mpl.colormaps.keys())]}"
        },
        "cbarMin": {
            "name": "Color bar min",
            "type": "float",
            "description": "Minimum value of the color bar. Only applies if elevPercentiles is False. Must be set with"
                           " and less than cbarMax. Invalid configuration will be replaced by default value (-30)"
        },
        "cbarMax": {
            "name": "Color bar max",
            "type": "float",
            "description": "Maximum value of the color bar. Only applies if elevPercentiles is False. Must be set with"
                           " and greater than cbarMin. Invalid configuration will be replaced by default value (-10)"
        },
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        TomogramBaseClass.__init__(self, tile_service_factory, **kwargs)

    def parse_args(self, compute_options):
        ds, parameter = super().parse_args(compute_options)

        def get_required_float(name):
            val = compute_options.get_float_arg(name, None)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        def get_required_int(name):
            val = compute_options.get_int_arg(name, None)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        latitude = compute_options.get_argument('latitude', None)
        output = compute_options.get_content_type().lower()

        if latitude is None:
            raise NexusProcessingException(reason='Missing required parameter: latitude', code=400)
        if re.match(FLOAT_PATTERN, latitude):
            latitude = float(latitude)

            if latitude < -180 or latitude > 180:
                raise NexusProcessingException(reason='Invalid latitude: out of range', code=400)

            latitude = [latitude]
        elif ',' in latitude:
            if output == 'png':
                raise NexusProcessingException(reason='Cannot use PNG output with multi-latitude slicing', code=400)

            try:
                latitude = list(sorted([float(l) for l in latitude.split(',')]))
            except ValueError:
                raise NexusProcessingException(reason='Invalid latitude: invalid format', code=400)

            if any([l < -180 or l > 180] for l in latitude):
                raise NexusProcessingException(reason='Invalid latitude: out of range', code=400)
        elif ':' in latitude:
            if output == 'png':
                raise NexusProcessingException(reason='Cannot use PNG output with multi-longitude slicing', code=400)

            try:
                latitude = slice(*(float(p) if p else None for p in latitude.split(':')))
            except:
                raise NexusProcessingException(reason='Invalid latitude: invalid format', code=400)

            if latitude.start is not None and latitude.stop is not None and latitude.start >= latitude.stop:
                raise NexusProcessingException(reason='Invalid latitude: out of order', code=400)

            # Some helpers for a < b / a > b where a could be None and return false in that case
            def lt(a, b):
                return a is not None and a < b

            def gt(a, b):
                return a is not None and a > b

            if (lt(latitude.start, -180) or gt(latitude.start, 180) or
                    lt(latitude.stop, -180) or gt(latitude.stop, 180)):
                raise NexusProcessingException(reason='Invalid latitude: out of range', code=400)
        elif latitude == '*':
            if output == 'png':
                raise NexusProcessingException(reason='Cannot use PNG output with multi-latitude slicing', code=400)

            latitude = slice(None, None, None)
        else:
            raise NexusProcessingException(reason='Invalid latitude argument', code=400)

        min_lon = get_required_float('minLon')
        max_lon = get_required_float('maxLon')
        min_elevation = get_required_int('minElevation')
        max_elevation = get_required_int('maxElevation')

        horizontal_margin = compute_options.get_float_arg('horizontalMargin', 0.001)

        ch_ds = compute_options.get_argument('canopy_ds', None)
        g_ds = compute_options.get_argument('ground_ds', None)

        percentiles = compute_options.get_boolean_arg('elevPercentiles')

        if percentiles and (ch_ds is None or g_ds is None):
            raise NexusProcessingException(
                code=400,
                reason='\'elevPercentiles\' argument requires \'canopy_ds\' and \'ground_ds\' be set'
            )

        peaks = compute_options.get_boolean_arg('peaks') and not percentiles

        cmap = mpl.colormaps.get(
            compute_options.get_argument('cmap', 'viridis'),
            mpl.colormaps['viridis']
        )

        return (ds, parameter, latitude, min_lon, max_lon, min_elevation, max_elevation,horizontal_margin, peaks, ch_ds,
                g_ds, percentiles, cmap)

    def calc(self, compute_options, **args):
        (dataset, parameter, latitude, min_lon, max_lon, min_elevation, max_elevation, horizontal_margin, calc_peaks,
         ch_ds, g_ds, percentiles, cmap) = self.parse_args(compute_options)

        slices = dict(
            lon=slice(min_lon, max_lon),
            elevation=slice(min_elevation, max_elevation)
        )

        if isinstance(latitude, slice):
            slices['lat'] = latitude
        elif isinstance(latitude, list):
            slices['lat'] = slice(latitude[0] - horizontal_margin, latitude[-1] + horizontal_margin)
        else:
            slices['lat'] = slice(latitude - horizontal_margin, latitude + horizontal_margin)

        data_in_bounds = self.do_subset(dataset, parameter, slices, 0)
        z_step = TomogramBaseClass._get_elevation_step_size(data_in_bounds)

        r = np.arange(min_elevation, max_elevation + z_step, z_step)

        logger.info(f'Fetched {len(data_in_bounds):,} data points at this elevation')

        ds = TomogramBaseClass.data_subset_to_ds_with_elevation(
            TomogramBaseClass.bin_subset_elevations_to_range(data_in_bounds, r, z_step / 2)
        )[3]

        slices['elevation'] = slice(None, None)
        elev_vars = {}

        if ch_ds is not None:
            elev_vars['ch'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(ch_ds, parameter, slices, 0))[3]

        if g_ds is not None:
            elev_vars['gh'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(g_ds, parameter, slices, 0))[3]

        if elev_vars:
            TomogramBaseClass.add_variables_to_tomo(ds, **elev_vars)

        if isinstance(latitude, list):
            lat_slices = [ds.sel(latitude=l, method='nearest') for l in latitude]
            ds = xr.concat(lat_slices, dim='latitude').drop_duplicates('latitude')
        else:
            ds = ds.sel(latitude=latitude)

        if 'ch' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation <= ds.ch)
        if 'gh' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation >= ds.gh)

        if percentiles:
            tomo = ds['tomo'].cumsum(dim='elevation', skipna=True).where(ds['tomo'].notnull())
            ds['tomo'] = tomo / tomo.max(dim='elevation', skipna=True)
        else:
            ds['tomo'] = xr.apply_ufunc(lambda a: 10 * np.log10(a), ds.tomo)

        if calc_peaks:
            peaks = []

            for i in range(len(ds.tomo.latitude)):
                lat_peaks = []
                ds_sub = ds.isel(latitude=i)

                for j in range(len(ds.tomo.longitude)):
                    col = ds_sub.isel(longitude=j)

                    lon = col.longitude.item()

                    try:
                        idx = col.argmax(dim='elevation').tomo.item()

                        lat_peaks.append((lon, col.isel(elevation=idx).elevation.item()))

                    except ValueError:
                        lat_peaks.append((lon, np.nan))

                peaks.append((np.array([p[0] for p in lat_peaks]),
                              np.array([p[1] for p in lat_peaks])))
        else:
            peaks = None

        return ProfileTomoResults(
            results=(ds, peaks),
            s={'latitude': latitude},
            coords=dict(x=ds.longitude, y=ds.elevation),
            meta=dict(dataset=dataset),
            style='db' if not percentiles else 'percentile',
            cmap=cmap,
            computeOptions=compute_options
        )


@nexus_handler
class CustomProfileTomogramImpl(TomogramBaseClass):
    name = "Tomogram profile view between two points"
    path = "/tomogram/profile"
    description = "Fetches a tomogram sliced by a line between two points"
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
        "p1": {
            "name": "Start Point",
            "type": "comma-delimited pair of floats",
            "description": "Point to start the slice line at: longitude,latitude, REQUIRED"
        },
        "p2": {
            "name": "End Point",
            "type": "comma-delimited pair of floats",
            "description": "Point to end the slice line at: longitude,latitude, REQUIRED"
        },
        "minElevation": {
            "name": "Minimum Elevation",
            "type": "int",
            "description": "Minimum Elevation. Required."
        },
        "maxElevation": {
            "name": "Maximum Elevation",
            "type": "int",
            "description": "Maximum Elevation. Required."
        },
        "horizontalMargin": {
            "name": "Horizontal Margin",
            "type": "float",
            "description": "Margin +/- desired lat/lon slice to include in output. Default: 0.001m"
        },
        "peaks": {
            "name": "Calculate peaks",
            "type": "boolean",
            "description": "Calculate peaks along tomogram slice (currently uses simplest approach). "
                           "Ignored with \'elevPercentiles\'"
        },
        "canopy_ds": {
            "name": "Canopy height dataset name",
            "type": "string",
            "description": "Dataset containing canopy heights (RH98). This is used to trim out tomogram voxels that "
                           "return over the measured or predicted canopy"
        },
        "ground_ds": {
            "name": "Ground height dataset name",
            "type": "string",
            "description": "Dataset containing ground height (DEM). This is used to trim out tomogram voxels that "
                           "return below the measured or predicted ground"
        },
        "elevPercentiles": {
            "name": "Elevation percentiles",
            "type": "boolean",
            "description": "Display percentiles of cumulative returns over elevation. Requires both canopy_ds and "
                           "ground_ds be set."
        },
        "cmap": {
            "name": "Color Map",
            "type": "string",
            "description": f"Color map to use. Will default to viridis if not provided or an unsupported map is "
                           f"provided. Supported cmaps: {[k for k in sorted(mpl.colormaps.keys())]}"
        },
        "cbarMin": {
            "name": "Color bar min",
            "type": "float",
            "description": "Minimum value of the color bar. Only applies if elevPercentiles is False. Must be set with"
                           " and less than cbarMax. Invalid configuration will be replaced by default value (-30)"
        },
        "cbarMax": {
            "name": "Color bar max",
            "type": "float",
            "description": "Maximum value of the color bar. Only applies if elevPercentiles is False. Must be set with"
                           " and greater than cbarMin. Invalid configuration will be replaced by default value (-10)"
        },
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        TomogramBaseClass.__init__(self, tile_service_factory, **kwargs)

    def parse_args(self, compute_options):
        ds, parameter = super().parse_args(compute_options)

        def get_required_int(name):
            val = compute_options.get_int_arg(name, None)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        def get_required_point(name):
            val = compute_options.get_argument(name, None)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            val = val.split(',')

            if len(val) != 2:
                raise NexusProcessingException(reason=f'Invalid parameter: {name}={val}. Incorrect number of fields, '
                                                      f'need 2', code=400)

            try:
                longitude = float(val[0])
            except ValueError:
                raise NexusProcessingException(reason='Longitude must be numeric', code=400)

            try:
                latitude = float(val[1])
            except ValueError:
                raise NexusProcessingException(reason='Latitude must be numeric', code=400)

            if longitude < -180 or longitude > 180 or isnan(longitude):
                raise NexusProcessingException(
                    reason='Longitude must be non-nan and between -180 and 180', code=400
                )

            if latitude < -90 or latitude > 90 or isnan(latitude):
                raise NexusProcessingException(
                    reason='Latitude must be non-nan and between -90 and 90', code=400
                )

            return Point(longitude, latitude)

        start_point = get_required_point('p1')
        end_point = get_required_point('p2')

        min_elevation = get_required_int('minElevation')
        max_elevation = get_required_int('maxElevation')

        if min_elevation >= max_elevation:
            raise NexusProcessingException(
                reason='Min elevation must be less than max elevation', code=400
            )

        horizontal_margin = compute_options.get_float_arg('horizontalMargin', 0.001)

        ch_ds = compute_options.get_argument('canopy_ds', None)
        g_ds = compute_options.get_argument('ground_ds', None)

        output = compute_options.get_content_type().lower()

        if output in ['csv', 'zip']:
            raise NexusProcessingException(reason=f'Output {output} unsupported', code=400)

        percentiles = compute_options.get_boolean_arg('elevPercentiles')

        if percentiles and (ch_ds is None or g_ds is None):
            raise NexusProcessingException(
                code=400,
                reason='\'elevPercentiles\' argument requires \'canopy_ds\' and \'ground_ds\' be set'
            )

        peaks = compute_options.get_boolean_arg('peaks') and not percentiles

        cmap = mpl.colormaps.get(
            compute_options.get_argument('cmap', 'viridis'),
            mpl.colormaps['viridis']
        )

        return (ds, parameter, start_point, end_point, min_elevation, max_elevation,horizontal_margin, peaks, ch_ds,
                g_ds, percentiles, cmap)

    def __subset_on_line(self, ds, start_point, end_point, min_elevation, max_elevation, parameter, horizontal_margin):
        tile_service = self._get_tile_service()

        tiles = tile_service.find_tiles_along_line(
            start_point, end_point,
            ds=ds,
            min_elevation=min_elevation,
            max_elevation=max_elevation,
            fetch_data=False
        )

        logger.info(f'Matched {len(tiles):,} tiles from Solr')

        if len(tiles) == 0:
            raise NoDataException(reason="No tiles matched the selected paramters")

        data = []

        bbox_miny = min(start_point.y, end_point.y) - horizontal_margin
        bbox_maxy = max(start_point.y, end_point.y) - horizontal_margin
        bbox_minx = min(start_point.x, end_point.x) + horizontal_margin
        bbox_maxx = max(start_point.x, end_point.x) + horizontal_margin

        for i in range(len(tiles) - 1, -1, -1):
            tile = tiles.pop(i)

            tile_id = tile.tile_id

            logger.info(f'Processing tile {tile_id} | {i=}')

            tile = tile_service.fetch_data_for_tiles(tile)[0]
            tile = tile_service.mask_tiles_to_bbox(bbox_miny, bbox_maxy, bbox_minx, bbox_maxx, [tile])

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
                    logger.warning(
                        f'No variable {parameter} found at point {nexus_point.index} for tile {tile.tile_id}')
                    data_val = np.nan

                data.append({
                    'latitude': nexus_point.latitude,
                    'longitude': nexus_point.longitude,
                    'elevation': nexus_point.depth,
                    'data': data_val
                })

        if len(data) == 0:
            raise NoDataException(reason='No data fit the selected parameters')

        return data

    def calc(self, compute_options, **args):
        (dataset, parameter, start_point, end_point, min_elevation, max_elevation, horizontal_margin, calc_peaks,
         ch_ds, g_ds, percentiles, cmap) = self.parse_args(compute_options)

        bbox_miny = min(start_point.y, end_point.y) - horizontal_margin
        bbox_maxy = max(start_point.y, end_point.y) - horizontal_margin
        bbox_minx = min(start_point.x, end_point.x) + horizontal_margin
        bbox_maxx = max(start_point.x, end_point.x) + horizontal_margin

        slices = dict(
            lat=slice(bbox_miny, bbox_maxy),
            lon=slice(bbox_minx, bbox_maxx),
            elevation=slice(min_elevation, max_elevation)
        )

        try:
            data_in_bounds = self.__subset_on_line(dataset, start_point, end_point, min_elevation, max_elevation,
                                                   parameter, horizontal_margin)
        except NexusTileServiceException as e:
            logger.warning('Selected dataset does not support subsetting along a line, will fall back to box subset')
            data_in_bounds = self.do_subset(dataset, parameter, slices, 0)
        except Exception as e:
            logger.error('An unexpected exception occurred', exc_info=e)
            raise NexusProcessingException(
                code=500,
                reason=str(e)
            )

        z_step = TomogramBaseClass._get_elevation_step_size(data_in_bounds)

        r = np.arange(min_elevation, max_elevation + z_step, z_step)

        logger.info(f'Fetched {len(data_in_bounds):,} data points at this elevation')

        ds = TomogramBaseClass.data_subset_to_ds_with_elevation(
            TomogramBaseClass.bin_subset_elevations_to_range(data_in_bounds, r, z_step / 2)
        )[3]

        slices['elevation'] = slice(None, None)
        elev_vars = {}

        if ch_ds is not None:
            elev_vars['ch'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(ch_ds, parameter, slices, 0))[3]

        if g_ds is not None:
            elev_vars['gh'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(g_ds, parameter, slices, 0))[3]

        if elev_vars:
            TomogramBaseClass.add_variables_to_tomo(ds, **elev_vars)

        if 'ch' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation <= ds.ch)
        if 'gh' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation >= ds.gh)

        if percentiles:
            tomo = ds['tomo'].cumsum(dim='elevation', skipna=True).where(ds['tomo'].notnull())
            ds['tomo'] = tomo / tomo.max(dim='elevation', skipna=True)
        else:
            ds['tomo'] = xr.apply_ufunc(lambda a: 10 * np.log10(a), ds.tomo)

        line = LineString([start_point, end_point])

        i_lat_1 = list(ds.latitude.values).index(ds.sel(latitude=start_point.y, method='nearest').latitude)
        i_lat_2 = list(ds.latitude.values).index(ds.sel(latitude=end_point.y, method='nearest').latitude)

        i_lon_1 = list(ds.longitude.values).index(ds.sel(longitude=start_point.x, method='nearest').longitude)
        i_lon_2 = list(ds.longitude.values).index(ds.sel(longitude=end_point.x, method='nearest').longitude)

        steps = ceil(sqrt((i_lat_2 - i_lat_1) ** 2 + (i_lon_2 - i_lon_1) ** 2))

        point_coords = []

        for p in np.linspace(0, 1, steps):
            point_coords.append(line.interpolate(p, True).coords[0])

        point_coords = [dict(longitude=p[0], latitude=p[1]) for p in point_coords]

        points = [ds.sel(p, method='nearest') for p in point_coords]

        ds = xr.concat(points, dim='distance')

        def dist(lat, lon):
            return abs(geodesic((start_point.y, start_point.x), (lat, lon)).m)

        dists = []

        for lat, lon in zip(ds.latitude.values, ds.longitude.values):
            dists.append(dist(lat, lon))

        ds = (ds.assign_coords(dict(distance=(['distance'], dists))).set_index(distance='distance').
              expand_dims(dim='slice').drop_duplicates('distance'))

        if calc_peaks:
            peaks = []

            for i in range(len(ds.tomo.slice)):
                slice_peaks = []
                ds_sub = ds.isel(slice=i)

                for j in range(len(ds.tomo.distance)):
                    col = ds_sub.isel(distance=j)

                    distance = col.distance.item()

                    try:
                        idx = col.argmax(dim='elevation').tomo.item()

                        slice_peaks.append((distance, col.isel(elevation=idx).elevation.item()))

                    except ValueError:
                        slice_peaks.append((distance, np.nan))

                peaks.append((np.array([p[0] for p in slice_peaks]),
                              np.array([p[1] for p in slice_peaks])))
        else:
            peaks = None

        return ProfileTomoResults(
            results=(ds, peaks),
            s={'start_point': start_point, 'end_point': end_point},
            coords=dict(x=ds.distance, y=ds.elevation),
            meta=dict(dataset=dataset),
            style='db' if not percentiles else 'percentile',
            cmap=cmap,
            computeOptions=compute_options
        )


class ElevationTomoResults(NexusResults):
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, **args):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **args)

    def toImage(self):
        data, lats, lons, attrs = self.results()

        lats = lats.tolist()
        lons = lons.tolist()

        logger.info('Building plot')

        plt.figure(figsize=(15,11))
        plt.imshow(np.squeeze(10*np.log10(data)), vmax=-10, vmin=-30)
        plt.colorbar(label=f'Tomogram, z={attrs["elevation"]} ± {attrs["margin"]} m (dB)')
        plt.title(f'{attrs["ds"]} tomogram @ {attrs["elevation"]} ± {attrs["margin"]} m elevation')
        plt.ylabel('Latitude')
        plt.xlabel('Longitude')

        xticks, xlabels = plt.xticks()
        xlabels = [f'{lons[int(t)]:.4f}' if int(t) in range(len(lons)) else '' for t in xticks]
        plt.xticks(xticks, xlabels, rotation=-90)

        yticks, ylabels = plt.yticks()
        ylabels = [f'{lats[int(t)]:.4f}' if int(t) in range(len(lats)) else '' for t in yticks]
        plt.yticks(yticks, ylabels, )

        buffer = BytesIO()

        logger.info('Writing plot to buffer')
        plt.savefig(buffer, format='png', facecolor='white')

        buffer.seek(0)
        plt.close()
        return buffer.read()


class ProfileTomoResults(NexusResults):
    def __init__(
            self,
            results=None,
            s=None,
            coords=None,
            meta=None,
            style='db',
            stats=None,
            computeOptions=None,
            status_code=200,
            **args
    ):
        NexusResults.__init__(self, results, meta, stats, None, status_code, **args)
        # Workaround to avoid error from non-standard parameters
        self.__computeOptions = computeOptions
        self.__coords = coords
        self.__slice = s
        self.__style = style
        self.__cmap = args.get('cmap', mpl.colormaps['viridis'])

    def toImage(self, i=0):
        ds, peaks = self.results()

        options = self.__computeOptions

        if 'longitude' in self.__slice:
            lon = self.__slice['longitude'][0]
            title_row = f'Longitude: {lon}'
            xlabel = 'Latitude'
            coord = 'longitude'
        elif 'latitude' in self.__slice:
            lat = self.__slice['latitude'][0]
            title_row = f'Latitude: {lat}'
            xlabel = 'Longitude'
            coord = 'latitude'
        else:
            start = self.__slice['start_point']
            end = self.__slice['end_point']

            start = (start.x, start.y)
            end = (end.x, end.y)

            title_row = f'Slice from: {start} to {end}'
            xlabel = 'Distance along slice [m]'
            coord = 'slice'

        ds = ds.isel({coord: i}).transpose(self.__coords['y'].name, self.__coords['x'].name)

        rows = ds.tomo.to_numpy()
        if peaks is not None:
            peaks = peaks[i]

        logger.info('Building plot')

        plt.figure(figsize=(11, 7))
        if self.__style == 'db':
            opt_min = options.get_float_arg('cbarMin', None)
            opt_max = options.get_float_arg('cbarMax', None)

            v = dict(vmin=-30, vmax=-10)

            if opt_min is not None:
                v['vmin'] = opt_min
            if opt_max is not None:
                v['vmax'] = opt_max

            if v['vmin'] >= v['vmax']:
                logger.warning('cbarMin >= cbarMax. Using defaults')
                v = dict(vmin=-30, vmax=-10)
        else:
            v = dict(vmin=0, vmax=1)
        plt.pcolormesh(self.__coords['x'], self.__coords['y'], rows, cmap=self.__cmap, **v)
        plt.colorbar()
        plt.title(f'{self.meta()["dataset"]} tomogram slice\n{title_row}')
        plt.xlabel(xlabel)
        plt.ylabel(f'Elevation w.r.t. {self.meta()["dataset"]} reference (m)')

        if peaks is not None:
            plt.plot(
                peaks[0], peaks[1], color='red', alpha=0.75
            )

            plt.legend(['Peak value'])

        plt.ticklabel_format(useOffset=False)

        buffer = BytesIO()

        logger.info('Writing plot to buffer')
        plt.savefig(buffer, format='png', facecolor='white')

        buffer.seek(0)
        plt.close()
        return buffer.read()

    def toNetCDF(self):
        ds, peaks = self.results()

        if 'start_point' in self.__slice:
            coords = dict(slice=ds['slice'], distance=ds['distance'])
            dims = ['slice', 'distance']
        else:
            coords = dict(latitude=ds['latitude'], longitude=ds['longitude'])
            dims = ['longitude', 'latitude']

        if peaks is not None:
            ds['peaks'] = xr.DataArray(
                np.array([p[1] for p in peaks]),
                coords=coords,
                dims=dims,
                name='peaks'
            )

        comp = dict(zlib=True, complevel=9)
        encoding = {vname: comp for vname in ds.data_vars}

        with TemporaryDirectory() as td:
            ds.to_netcdf(os.path.join(td, 'ds.nc'), encoding=encoding)

            with open(os.path.join(td, 'ds.nc'), 'rb') as fp:
                buf = BytesIO(fp.read())

        buf.seek(0)
        return buf.read()

    def __plot_to_images(self, td):
        ds, peaks = self.results()

        options = self.__computeOptions

        if 'latitude' in self.__slice:
            along = 'latitude'
            across = 'longitude'
            title_row = 'Latitude: {:3.4f}'
            xlabel = 'Longitude'
        else:
            along = 'longitude'
            across = 'latitude'
            title_row = 'Longitude: {:3.4f}'
            xlabel = 'Latitude'

        min_across = ds[across].min().item()
        max_across = ds[across].max().item()

        files = []

        for al_i in range(len(ds[along])):
            ds_sub = ds.isel({along: al_i})
            al = ds_sub[along].item()

            rows = ds_sub.tomo.to_numpy()

            peaks_sub = None

            if peaks is not None:
                peaks_sub = peaks[al_i]

            logger.info(f'Building plot for along slice {al_i} of {len(ds[along])}')

            plt.figure(figsize=(11, 7))
            if self.__style == 'db':
                opt_min = options.get_float_arg('cbarMin', None)
                opt_max = options.get_float_arg('cbarMax', None)

                v = dict(vmin=-30, vmax=-10)

                if opt_min is not None:
                    v['vmin'] = opt_min
                if opt_max is not None:
                    v['vmax'] = opt_max

                if v['vmin'] >= v['vmax']:
                    logger.warning('cbarMin >= cbarMax. Using defaults')
                    v = dict(vmin=-30, vmax=-10)
            else:
                v = dict(vmin=0, vmax=1)

            plt.pcolormesh(self.__coords['x'], self.__coords['y'], rows, cmap=self.__cmap, **v)
            plt.colorbar()
            plt.title(f'{self.meta()["dataset"]} tomogram slice\n{title_row.format(al)}')
            plt.xlabel(xlabel)
            plt.ylabel(f'Elevation w.r.t. {self.meta()["dataset"]} reference (m)')

            if peaks_sub is not None:
                plt.plot(
                    peaks_sub[0], peaks_sub[1], color='red', alpha=0.75
                )

                plt.legend(['Peak value'])

            plt.ticklabel_format(useOffset=False)

            filename = f'tomogram_{self.meta()["dataset"]}_{along}_{al:3.4f}_{across}_{min_across:3.4f}_to_{max_across:3.4f}.png'

            plt.savefig(
                os.path.join(
                    td, filename
                ), format='png', facecolor='white')

            files.append((filename, os.path.join(td, filename)))
            plt.close()

        return files

    def toZip(self):
        buffer = BytesIO()

        with TemporaryDirectory() as td:
            files = self.__plot_to_images(td)

            with zipfile.ZipFile(buffer, 'a', zipfile.ZIP_DEFLATED) as zf:
                for filename, file_path in files:
                    zf.write(file_path, filename)

        buffer.seek(0)
        return buffer.read()

    def toGif(self):
        buffer = BytesIO()

        with TemporaryDirectory() as td:
            files = self.__plot_to_images(td)

            with contextlib.ExitStack() as stack:
                logger.info(f'Combining frames into final GIF')

                imgs = (stack.enter_context(Image.open(f[1])) for f in files)
                img = next(imgs)
                img.save(
                    buffer,
                    format='GIF',
                    save_all=True,
                    append_images=imgs,
                    duration=([100] * (len(files) - 1)) + [800],
                    loop=0
                )

        buffer.seek(0)
        return buffer.read()
