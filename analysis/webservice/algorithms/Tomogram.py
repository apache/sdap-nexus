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
from typing import Dict, Literal, Union, Tuple
from collections import namedtuple

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

            for e in elevation_range:
                if abs(elevation - e) < margin:
                    d_copy['elevation'] = e
                    break

            binned_subset.append(d_copy)

        return binned_subset

    @staticmethod
    def data_subset_to_ds_with_elevation(data_in_bounds: list) -> Tuple[np.ndarray, np.ndarray, np.ndarray, xr.Dataset]:
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


@nexus_handler
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
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        TomogramBaseClass.__init__(self, tile_service_factory, **kwargs)

    def parse_args(self, compute_options):
        ds, parameter = super().parse_args(compute_options)

        def get_required_float(name):
            val = compute_options.get_float_arg(name)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        def get_required_int(name):
            val = compute_options.get_int_arg(name)

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

        return (ds, parameter, longitude, min_lat, max_lat, min_elevation, max_elevation, elevation_margin,
                horizontal_margin, stride)

    def calc(self, compute_options, **args):
        (dataset, parameter, longitude, min_lat, max_lat, min_elevation, max_elevation, elevation_margin,
         horizontal_margin, stride) = self.parse_args(compute_options)

        slices = dict(
            lat=slice(min_lat, max_lat),
            lon=slice(longitude - horizontal_margin, longitude + horizontal_margin),
            elevation=slice(min_elevation, max_elevation)
        )

        r = np.arange(min_elevation, max_elevation, elevation_margin)

        data_in_bounds = self.do_subset(dataset, parameter, slices, elevation_margin)

        logger.info(f'Fetched {len(data_in_bounds):,} data points at this elevation')

        ds = TomogramBaseClass.data_subset_to_ds_with_elevation(
            TomogramBaseClass.bin_subset_elevations_to_range(data_in_bounds, r, elevation_margin)
        )[3]

        lats = ds.latitude.to_numpy()

        rows = 10*np.log10(np.array(ds.tomo.to_numpy()))

        return ProfileTomoResults(
            results=rows,
            s={'longitude': longitude},
            extent=[lats[0], lats[-1], min_elevation, max_elevation],
            meta=dict(dataset=dataset)
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
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        TomogramBaseClass.__init__(self, tile_service_factory, **kwargs)

    def parse_args(self, compute_options):
        ds, parameter = super().parse_args(compute_options)

        def get_required_float(name):
            val = compute_options.get_float_arg(name)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        def get_required_int(name):
            val = compute_options.get_int_arg(name)

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

        return (ds, parameter, latitude, min_lon, max_lon, min_elevation, max_elevation, elevation_margin,
                horizontal_margin, stride)

    def calc(self, compute_options, **args):
        (dataset, parameter, latitude, min_lon, max_lon, min_elevation, max_elevation, elevation_margin,
         horizontal_margin, stride) = self.parse_args(compute_options)

        slices = dict(
            lon=slice(min_lon, max_lon),
            lat=slice(latitude - horizontal_margin, latitude + horizontal_margin),
        )

        slice_dict = {}
        lons = []
        n_points = 0
        r = range(min_elevation, max_elevation, stride)

        logger.info(f'Fetching {len(r):,} elevation slices')

        for e in r:
            logger.info(f'Fetching elevation: {e}')

            slices['elevation'] = e

            data_in_bounds = self.do_subset(dataset, parameter, slices, elevation_margin)
            logger.info(f'Fetched {len(data_in_bounds):,} data points at this elevation')
            n_points += len(data_in_bounds)

            ds = TomogramBaseClass.data_subset_to_ds(data_in_bounds)[3]

            if ds.tomo.shape == (0, 0):
                continue

            slice_dict[e] = ds
            lons.extend(ds.latitude)

        logger.info('Data fetch complete, organizing data to rows')

        lons = np.unique(lons)
        n_lons = len(lons)
        rows = []

        for e in r:
            row = np.full((n_lons,), np.nan)

            if e not in slice_dict:
                rows.append(row)
                continue

            ds = slice_dict[e]

            try:
                ds = ds.sel(latitude=latitude)

                for lon, vox in zip(ds.longitude, ds.tomo):
                    lon, vox = lon.item(), vox.item()

                    lon_i = np.where(lons == lon)[0][0]

                    row[lon_i] = vox
            except:
                rows.append(row)
                continue

            rows.append(row)

        rows = 10*np.log10(np.array(rows))

        return ProfileTomoResults(
            results=rows,
            s={'latitude': latitude},
            extent=[lons[0], lons[-1], min_elevation, max_elevation],
            meta=dict(dataset=dataset)
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
        return buffer.read()


class ProfileTomoResults(NexusResults):
    def __init__(self, results=None, s=None, extent=None, meta=None, stats=None, computeOptions=None, status_code=200, **args):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **args)
        self.__extent = extent
        self.__slice = s

    def toImage(self):
        rows = self.results()

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
        plt.imshow(np.flipud(rows), extent=self.__extent, vmin=-30, vmax=-10, aspect='auto')
        plt.colorbar()
        plt.title(f'{self.meta()["dataset"]} tomogram slice\n{title_row}')
        plt.xlabel(xlabel)
        plt.ylabel('Height Relative to WGS84 ellipsoid (m)')

        buffer = BytesIO()

        logger.info('Writing plot to buffer')
        plt.savefig(buffer, format='png', facecolor='white')

        buffer.seek(0)
        return buffer.read()
