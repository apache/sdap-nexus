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
from typing import Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

logger = logging.getLogger(__name__)


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

        tiles = tile_service.find_tiles_in_box(
            min_lat, max_lat, min_lon, max_lon,
            ds=ds,
            min_elevation=min_elevation,
            max_elevation=max_elevation,
            fetch_data=False
        )

        logger.info(f'Matched {len(tiles):,} tiles from Solr')

        if len(tiles) == 0:
            raise NoDataException(reason='No data was found within the selected parameters')

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
                    'elevation': nexus_point.depth,
                    'data': data_val
                })

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
            elevation = d['elevation']
            d_copy = dict(**d)

            a = np.argwhere(np.absolute(elevation_range - elevation) <= margin)

            if a.shape[0] > 0:
                d_copy['elevation'] = elevation_range[a[0][0]]
                binned_subset.append(d_copy)
            else:
                logger.warning(f'Could not bin point {d_copy}')

        return binned_subset

    @staticmethod
    def data_subset_to_ds_with_elevation(
            data_in_bounds: list
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, xr.Dataset]:
        lats = np.unique([d['latitude'] for d in data_in_bounds])
        lons = np.unique([d['longitude'] for d in data_in_bounds])
        elevations = np.unique([d['elevation'] for d in data_in_bounds])

        vals = np.empty((len(elevations), len(lats), len(lons)))

        data_dict = {(d['elevation'], d['latitude'], d['longitude']): d['data'] for d in data_in_bounds}

        for i, elev in enumerate(elevations):
            for j, lat in enumerate(lats):
                for k, lon in enumerate(lons):
                    vals[i, j, k] = data_dict.get((elev, lat, lon), np.nan)

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
            "type": "float",
            "description": "Longitude at which to slice the tomogram. Required."
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
        "stride": {
            "name": "Stride",
            "type": "integer",
            "description": "Stride between elevation slices. Default: 1"
        },
        "elevationMargin": {
            "name": "Elevation Margin",
            "type": "float",
            "description": "Margin +/- desired elevation to include in output. Default: 0.5m"
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

        longitude = get_required_float('longitude')
        min_lat = get_required_float('minLat')
        max_lat = get_required_float('maxLat')
        min_elevation = get_required_int('minElevation')
        max_elevation = get_required_int('maxElevation')

        elevation_margin = compute_options.get_float_arg('elevationMargin', 0.5)
        horizontal_margin = compute_options.get_float_arg('horizontalMargin', 0.001)
        stride = compute_options.get_int_arg('stride', 1)

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

        return (ds, parameter, longitude, min_lat, max_lat, min_elevation, max_elevation, elevation_margin,
                horizontal_margin, stride, peaks, ch_ds, g_ds, percentiles, cmap)

    def calc(self, compute_options, **args):
        (dataset, parameter, longitude, min_lat, max_lat, min_elevation, max_elevation, elevation_margin,
         horizontal_margin, stride, calc_peaks, ch_ds, g_ds, percentiles, cmap) = self.parse_args(compute_options)

        slices = dict(
            lat=slice(min_lat, max_lat),
            lon=slice(longitude - horizontal_margin, longitude + horizontal_margin),
            elevation=slice(min_elevation, max_elevation)
        )

        # TODO: Do stride/elevation margin need to be provided or can they be computed? This is important because
        #  getting them wrong will mess up the result. (?): Stride === elevation diff between adjacent layers;
        #  margin === stride / 2

        r = np.arange(min_elevation, max_elevation + stride, stride)

        data_in_bounds = self.do_subset(dataset, parameter, slices, elevation_margin)

        logger.info(f'Fetched {len(data_in_bounds):,} data points at this elevation')

        ds = TomogramBaseClass.data_subset_to_ds_with_elevation(
            TomogramBaseClass.bin_subset_elevations_to_range(data_in_bounds, r, elevation_margin)
        )[3]

        slices['elevation'] = slice(None, None)
        elev_vars = {}

        if ch_ds is not None:
            elev_vars['ch'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(ch_ds, parameter, slices, 0))[3]

        if g_ds is not None:
            elev_vars['gh'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(g_ds, parameter, slices, 0))[3]

        # if len(ds.longitude) > 1:
        #     ds = ds.sel(longitude=longitude, method='nearest')

        if elev_vars:
            TomogramBaseClass.add_variables_to_tomo(ds, **elev_vars)

        ds = ds.sel(longitude=longitude, method='nearest')

        if 'ch' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation <= ds.ch)
        if 'gh' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation >= ds.gh)

        lats = ds.latitude.to_numpy()

        if percentiles:
            tomo = ds['tomo'].cumsum(dim='elevation', skipna=True).where(ds['tomo'].notnull())
            ds['tomo'] = tomo / tomo.max(dim='elevation', skipna=True)
        else:
            ds['tomo'] = xr.apply_ufunc(lambda a: 10 * np.log10(a), ds.tomo)

        rows = ds.tomo.to_numpy()

        if calc_peaks:
            peaks = []

            for i in range(len(ds.tomo.latitude)):
                col = ds.isel(latitude=i)

                lat = col.latitude.item()

                try:
                    idx = col.argmax(dim='elevation').tomo.item()

                    peaks.append((lat, col.isel(elevation=idx).elevation.item()))

                except ValueError:
                    peaks.append((lat, np.nan))

            peaks = (np.array([p[0] for p in peaks]),
                     np.array([p[1] for p in peaks]))
        else:
            peaks = None

        return ProfileTomoResults(
            results=(rows, peaks),
            s={'longitude': longitude},
            coords=dict(x=lats, y=ds.elevation.to_numpy()),
            meta=dict(dataset=dataset),
            style='db' if not percentiles else 'percentile',
            cmap=cmap
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
            "type": "float",
            "description": "Latitude at which to slice the tomogram. Required."
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
        "stride": {
            "name": "Stride",
            "type": "integer",
            "description": "Stride between elevation slices. Default: 1"
        },
        "elevationMargin": {
            "name": "Elevation Margin",
            "type": "float",
            "description": "Margin +/- desired elevation to include in output. Default: 0.5m"
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

        latitude = get_required_float('latitude')
        min_lon = get_required_float('minLon')
        max_lon = get_required_float('maxLon')
        min_elevation = get_required_int('minElevation')
        max_elevation = get_required_int('maxElevation')

        elevation_margin = compute_options.get_float_arg('elevationMargin', 0.5)
        horizontal_margin = compute_options.get_float_arg('horizontalMargin', 0.001)
        stride = compute_options.get_int_arg('stride', 1)

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

        return (ds, parameter, latitude, min_lon, max_lon, min_elevation, max_elevation, elevation_margin,
                horizontal_margin, stride, peaks, ch_ds, g_ds, percentiles, cmap)

    def calc(self, compute_options, **args):
        (dataset, parameter, latitude, min_lon, max_lon, min_elevation, max_elevation, elevation_margin,
         horizontal_margin, stride, calc_peaks, ch_ds, g_ds, percentiles, cmap) = self.parse_args(compute_options)

        slices = dict(
            lon=slice(min_lon, max_lon),
            lat=slice(latitude - horizontal_margin, latitude + horizontal_margin),
            elevation=slice(min_elevation, max_elevation)
        )

        r = np.arange(min_elevation, max_elevation + stride, stride)

        data_in_bounds = self.do_subset(dataset, parameter, slices, elevation_margin)

        logger.info(f'Fetched {len(data_in_bounds):,} data points at this elevation')

        ds = TomogramBaseClass.data_subset_to_ds_with_elevation(
            TomogramBaseClass.bin_subset_elevations_to_range(data_in_bounds, r, elevation_margin)
        )[3]

        slices['elevation'] = slice(None, None)
        elev_vars = {}

        if ch_ds is not None:
            elev_vars['ch'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(ch_ds, parameter, slices, 0))[3]

        if g_ds is not None:
            elev_vars['gh'] = TomogramBaseClass.data_subset_to_ds(self.do_subset(g_ds, parameter, slices, 0))[3]

        # if len(ds.latitude) > 1:
        #     ds = ds.sel(latitude=latitude, method='nearest')

        if elev_vars:
            TomogramBaseClass.add_variables_to_tomo(ds, **elev_vars)

        ds = ds.sel(latitude=latitude, method='nearest')

        if 'ch' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation <= ds.ch)
        if 'gh' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation >= ds.gh)

        lons = ds.longitude.to_numpy()

        if percentiles:
            tomo = ds['tomo'].cumsum(dim='elevation', skipna=True).where(ds['tomo'].notnull())
            ds['tomo'] = tomo / tomo.max(dim='elevation', skipna=True)
        else:
            ds['tomo'] = xr.apply_ufunc(lambda a: 10 * np.log10(a), ds.tomo)

        rows = ds.tomo.to_numpy()

        if calc_peaks:
            peaks = []

            for i in range(len(ds.tomo.longitude)):
                col = ds.isel(longitude=i)

                lon = col.longitude.item()

                try:
                    idx = col.argmax(dim='elevation').tomo.item()

                    peaks.append((lon, col.isel(elevation=idx).elevation.item()))

                except ValueError:
                    peaks.append((lon, np.nan))

            peaks = (np.array([p[0] for p in peaks]),
                     np.array([p[1] for p in peaks]))
        else:
            peaks = None

        return ProfileTomoResults(
            results=(rows, peaks),
            s={'latitude': latitude},
            coords=dict(x=lons, y=ds.elevation.to_numpy()),
            meta=dict(dataset=dataset),
            style='db' if not percentiles else 'percentile',
            cmap=cmap
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
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **args)
        self.__coords = coords
        self.__slice = s
        self.__style = style
        self.__cmap = args.get('cmap', mpl.colormaps['viridis'])

    def toImage(self):
        rows, peaks = self.results()

        if 'longitude' in self.__slice:
            lon = self.__slice['longitude']
            title_row = f'Longitude: {lon}'
            xlabel = 'Latitude'
        else:
            lat = self.__slice['latitude']
            title_row = f'Latitude: {lat}'
            xlabel = 'Longitude'

        logger.info('Building plot')

        plt.figure(figsize=(11, 7))
        if self.__style == 'db':
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
