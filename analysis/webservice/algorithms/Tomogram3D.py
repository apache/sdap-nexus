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
import random
from io import BytesIO
from os.path import join
from tempfile import TemporaryDirectory
from urllib.parse import urlencode

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from PIL import Image
from matplotlib.colors import LinearSegmentedColormap, XKCD_COLORS
from xarray import Dataset, apply_ufunc

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

logger = logging.getLogger(__name__)
mpl.colormaps.register(
    LinearSegmentedColormap.from_list(
        'forest',
        [
            XKCD_COLORS['xkcd:dirt brown'],
            # (0.0, 1.0, 0.0),
            XKCD_COLORS['xkcd:dark forest green'],
            XKCD_COLORS['xkcd:forest green'],
            XKCD_COLORS['xkcd:forest'],
            XKCD_COLORS['xkcd:light forest green'],
            XKCD_COLORS['xkcd:leaf'],
        ])
)


@nexus_handler
class Tomogram3D(NexusCalcHandler):
    name = '3D-Rendered Tomogram Subsetter'
    path = '/tomogram/3d'
    description = '3D visualization of tomogram subset, either as a static image or animated, orbiting GIF'
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
        "minElevation": {
            "name": "Minimum Elevation",
            "type": "float",
            "description": "Minimum Elevation. Required."
        },
        "maxElevation": {
            "name": "Maximum Elevation",
            "type": "float",
            "description": "Maximum Elevation. Required."
        },
        "output": {
            "name": "Output format",
            "type": "string",
            "description": "Desired output format. Must be one of \"PNG\", \"GIF\", or \"CSV\". Required."
        },
        "orbit": {
            "name": "Orbit settings",
            "type": "comma-delimited pair of ints",
            "description": "If output==GIF, specifies the orbit to be used in the animation. Format: "
                           "elevation angle,orbit step. Default: 30, 10; Ranges: [-180,180],[1,90]"
        },
        "viewAngle": {
            "name": "Static view angle",
            "type": "comma-delimited pair of ints",
            "description": "If output==PNG, specifies the angle to be used for the render. Format: "
                           "azimuth,elevation angle. Default: 30,45; Ranges: [0,359],[-180,180]"
        },
        "frameDuration": {
            "name": "Frame duration",
            "type": "int",
            "description": "If output==GIF, specifies the duration of each frame in the animation in milliseconds. "
                           "Default: 100; Range: >=100"
        },
        "filter": {
            "name": "Voxel filter",
            "type": "comma-delimited pair of numbers",
            "description": "Pair of numbers min,max. If defined, will filter out all points in the tomogram whose value"
                           " does not satisfy min <= v <= max. Can be left unbounded, eg: 'filter=-15,' would filter "
                           "everything but points >= -15. Ignored with \'elevPercentiles\'"
        },
        "renderType": {
            "name": "Render Type",
            "type": "string",
            "description": "Type of render: Must be either \"scatter\" or \"peak\". Scatter will plot every voxel in "
                           "the tomogram, colored by value, peak will plot a surface of all peaks for each lat/lon "
                           "grid point, flat colored and shaded. Ignored with \'elevPercentiles\'. Default: scatter"
        },
        "hideBasemap": {
            "name": "Hide Basemap",
            "type": "boolean",
            "description": "If true, do not draw basemap beneath plot. This can be used to speed up render time a bit"
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
        "renderRH": {
            "name": "Render RH",
            "type": "int",
            "description": "With \'elevPercentiles\', this parameter is an integer between 0 and 100, which if set, "
                           "will filter out all data whose cumulative return values is greater than the parameter "
                           "value. It will also apply the color mapping to the voxel elevation rather than value, "
                           "rendering a surface of that RH value."
        },
        "cmap": {
            "name": "Color Map",
            "type": "string",
            "description": f"Color map to use. Will default to viridis if not provided or an unsupported map is "
                           f"provided. Supported cmaps: {[k for k in sorted(mpl.colormaps.keys())]}"
        },
    }
    singleton = True

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

        try:
            bounding_poly = compute_options.get_bounding_polygon()
        except:
            raise NexusProcessingException(reason='Missing required parameter: b', code=400)

        def get_required_float(name):
            val = compute_options.get_float_arg(name, None)

            if val is None:
                raise NexusProcessingException(reason=f'Missing required parameter: {name}', code=400)

            return val

        min_elevation = get_required_float('minElevation')
        max_elevation = get_required_float('maxElevation')

        output = compute_options.get_argument('output', None)

        if output is not None:
            output = output.upper()

        if output not in ['PNG', 'GIF', 'CSV', 'NETCDF']:
            raise NexusProcessingException(reason=f'Missing or invalid required parameter: output = {output}', code=400)

        orbit_params = compute_options.get_argument('orbit', '30,10')
        view_params = compute_options.get_argument('viewAngle', '30,45')

        frame_duration = compute_options.get_int_arg('frameDuration', 100)

        if output == 'GIF':
            try:
                orbit_params = orbit_params.split(',')
                assert len(orbit_params) == 2
                orbit_elev, orbit_step = tuple([int(p) for p in orbit_params])

                assert -180 <= orbit_elev <= 180
                assert 1 <= orbit_step <= 90
                assert frame_duration >= 100
            except:
                raise NexusProcessingException(
                    reason=f'Invalid orbit parameters: {orbit_params} & {frame_duration}',
                    code=400
                )

            view_azim, view_elev = None, None
        else:
            try:
                view_params = view_params.split(',')
                assert len(view_params) == 2
                view_azim, view_elev = tuple([int(p) for p in view_params])

                assert 0 <= view_azim <= 359
                assert -180 <= view_elev <= 180
            except:
                raise NexusProcessingException(reason=f'Invalid view angle string: {orbit_params}', code=400)

            orbit_elev, orbit_step = None, None

        ch_ds = compute_options.get_argument('canopy_ds', None)
        g_ds = compute_options.get_argument('ground_ds', None)

        percentiles = compute_options.get_boolean_arg('elevPercentiles')

        if percentiles and (ch_ds is None or g_ds is None):
            raise NexusProcessingException(
                code=400,
                reason='\'elevPercentiles\' argument requires \'canopy_ds\' and \'ground_ds\' be set'
            )

        render_rh = compute_options.get_int_arg('renderRH', default=None)

        if render_rh is not None and not 0 <= render_rh <= 100:
            raise NexusProcessingException(
                code=400,
                reason="'renderRH' must be between 0 and 100"
            )

        filter_arg = compute_options.get_argument('filter') if not percentiles else None

        if filter_arg is not None:
            try:
                filter_arg = filter_arg.split(',')
                assert len(filter_arg) == 2
                vmin, vmax = filter_arg

                if vmin != '':
                    vmin = float(vmin)
                else:
                    vmin = None

                if vmax != '':
                    vmax = float(vmax)
                else:
                    vmax = None

                filter_arg = (vmin, vmax)
            except:
                raise NexusProcessingException(
                    reason='Invalid filter arg, must be of format [number],[number]',
                    code=400
                )
        else:
            filter_arg = (None, None)

        render_type = compute_options.get_argument('renderType', 'scatter').lower() if not percentiles else 'scatter'

        if render_type not in ['scatter', 'peak']:
            raise NexusProcessingException(
                reason='renderType must be either scatter or peak',
                code=400
            )

        hide_basemap = compute_options.get_boolean_arg('hideBasemap')

        cmap = mpl.colormaps.get(
            compute_options.get_argument('cmap', 'viridis'),
            mpl.colormaps['viridis']
        )

        return (ds, parameter_s, bounding_poly, min_elevation, max_elevation,
                (orbit_elev, orbit_step, frame_duration, view_azim, view_elev), filter_arg, render_type, hide_basemap,
                ch_ds, g_ds, percentiles, output, render_rh, cmap)

    def do_subset(self, ds, parameter, bounds):
        tile_service = self._get_tile_service()

        min_lat = bounds['lat'].start
        max_lat = bounds['lat'].stop

        min_lon = bounds['lon'].start
        max_lon = bounds['lon'].stop

        min_elevation = bounds['elevation'].start
        max_elevation = bounds['elevation'].stop

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

        for i in range(len(tiles) - 1, -1, -1):
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
                    logger.warning(
                        f'No variable {parameter} found at point {nexus_point.index} for tile {tile.tile_id}')
                    data_val = np.nan

                data.append({
                    'latitude': nexus_point.latitude,
                    'longitude': nexus_point.longitude,
                    'elevation': nexus_point.depth,
                    'data': data_val
                })

        return data

    def calc(self, computeOptions, **args):
        (ds, parameter, bounding_poly, min_elevation, max_elevation, render_params, filter_arg, render_type,
         hide_basemap, ch_ds, g_ds, percentiles, output, render_rh, cmap) = (self.parse_args(computeOptions))

        min_lat = bounding_poly.bounds[1]
        max_lat = bounding_poly.bounds[3]
        min_lon = bounding_poly.bounds[0]
        max_lon = bounding_poly.bounds[2]

        bounds = dict(
            lat=slice(min_lat, max_lat),
            lon=slice(min_lon, max_lon),
            elevation=slice(min_elevation, max_elevation)
        )

        data = self.do_subset(ds, parameter, bounds)

        logger.info('Building dataframe')

        if ch_ds is None and g_ds is None and output != 'NETCDF':
            lats = np.array([p['latitude'] for p in data])
            lons = np.array([p['longitude'] for p in data])
            elevs = np.array([p['elevation'] for p in data])

            tomo = np.array([p['data'] for p in data])
            tomo = 10 * np.log10(tomo)

            df = pd.DataFrame(
                np.hstack((
                    lats[:, np.newaxis],
                    lons[:, np.newaxis],
                    elevs[:, np.newaxis],
                    tomo[:, np.newaxis]
                )),
                columns=[
                    'lat',
                    'lon',
                    'elevation',
                    'tomo_value'
                ]
            )
        else:
            df = self.__mask_by_elev_map(
                data, ch_ds, g_ds, bounds.copy(), parameter, percentiles, output, render_rh is not None
            )

        logger.info(f'Result repr:\n{df}')

        bounds = (bounds['lat'].start, bounds['lon'].start, bounds['lat'].stop, bounds['lon'].stop)

        if percentiles:
            if render_rh is None or output == 'NETCDF':
                style = 'percentile'
            else:
                df = df[df['tomo_value'] > 1 - (render_rh / 100)]
                style = 'percentile_elevation'
        else:
            style = 'db'

        return Tomogram3DResults(
            df,
            render_params,
            filter_arg,
            render_type,
            bounds=bounds,
            hide_basemap=hide_basemap,
            style=style,
            cmap=cmap
        )

    def __mask_by_elev_map(self, data_points, ch_ds, g_ds, bounds, parameter, percentiles, output, invert):
        from .Tomogram import TomogramBaseClass as Tomo

        tries = 5

        # Sample data_points to determine difference in elevation between slices
        while tries >= 0:
            # There's a bug where some samples yield non-uniform slices for what DEFINITELY was uniform data at ingest
            # Trying to get around it by just resampling for now, but this will definitely need further investigation
            tries -= 1
            sample = data_points[random.randint(0, len(data_points) - 1)]

            sample = [d['elevation'] for d in data_points if
                      d['latitude'] == sample['latitude'] and d['longitude'] == sample['longitude']]

            sample.sort()
            sample = [sample[i + 1] - sample[i] for i in range(len(sample) - 1)]

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

        stride = sample[0]

        # Bin and grid the subset data to an xarray Dataset
        elevations = np.arange(bounds['elevation'].start, bounds['elevation'].stop + stride, stride / 2)
        bounds['elevation'] = slice(None, None)  # Remove elevation from params as these are 2d datasets

        ds = Tomo.data_subset_to_ds_with_elevation(
            Tomo.bin_subset_elevations_to_range(
                data_points, elevations, stride / 2
            )
        )[3]

        elev_vars = {}

        # Subset the elevation maps
        if ch_ds is not None:
            elev_vars['ch'] = Tomo.data_subset_to_ds(self.do_subset(ch_ds, parameter, bounds))[3]

        if g_ds is not None:
            elev_vars['gh'] = Tomo.data_subset_to_ds(self.do_subset(g_ds, parameter, bounds))[3]

        # Add them to the Dataset, regridding if necessary
        if elev_vars:
            Tomo.add_variables_to_tomo(ds, **elev_vars)

        # Trim the data
        if 'ch' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation <= ds.ch)
        if 'gh' in ds.data_vars:
            ds['tomo'] = ds['tomo'].where(ds.tomo.elevation >= ds.gh)

        if percentiles:
            tomo = ds['tomo'].cumsum(dim='elevation', skipna=True).where(ds['tomo'].notnull())
            ds['tomo'] = tomo / tomo.max(dim='elevation', skipna=True)

            if invert:
                ds['tomo'] = 1 - ds['tomo']
        else:
            ds['tomo'] = apply_ufunc(lambda a: 10 * np.log10(a), ds.tomo)

        if output == 'NETCDF':
            return ds

        # Convert to DataFrame, move coords to columns, rename to be consistent with other approach and select cols
        filtered_df = ds.to_dataframe().reset_index(['elevation', 'latitude', 'longitude']).rename(
            columns=dict(
                latitude='lat', longitude='lon', tomo='tomo_value'
            )
        )[['lat', 'lon', 'elevation', 'tomo_value']]

        return filtered_df


class Tomogram3DResults(NexusResults):
    def __init__(self, results=None, render_params=None, filter_args=None, render_type='scatter', style='db',
                 bounds=None, meta=None, stats=None, computeOptions=None, status_code=200, **args):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **args)
        self.render_params = render_params
        self.bounds = bounds

        if filter_args is None:
            self.filter = (None, None)
        else:
            self.filter = filter_args

        self.render_type = render_type
        self.style = style
        self.hide_basemap = 'hide_basemap' in args and args['hide_basemap']
        self.cmap = args.get('cmap', mpl.colormaps['viridis'])

    def results(self):
        r: pd.DataFrame = NexusResults.results(self)
        r = Tomogram3DResults.min_max_filter(r, *self.filter)

        return r

    def result_dataset(self):
        ds = NexusResults.results(self)

        if not isinstance(ds, Dataset):
            raise NexusProcessingException(
                code=500,
                reason=f'Invalid result type {type(ds)}'
            )

        return ds

    def __common(self):
        xyz = self.results()[['lon', 'lat', 'elevation']].values

        fig = plt.figure(figsize=(10, 7))
        return xyz, (fig, fig.add_subplot(111, projection='3d'))

    @staticmethod
    def min_max_filter(df: pd.DataFrame, vmin=None, vmax=None):
        if vmin is not None or vmax is not None:
            n_points = len(df)

            if vmin is not None:
                df = df[df['tomo_value'] >= vmin]

            if vmax is not None:
                df = df[df['tomo_value'] <= vmax]

            logger.info(f'Filtered data from {n_points:,} to {len(df):,} points')

        return df

    def toImage(self):
        _, _, _, view_azim, view_elev = self.render_params

        results = self.results()
        xyz = results[['lon', 'lat', 'elevation']].values

        fig = plt.figure(figsize=(10, 7))
        ax = fig.add_subplot(111, projection='3d')

        ax.view_init(elev=view_elev, azim=view_azim)

        if not self.hide_basemap:
            min_lat, min_lon, max_lat, max_lon = self.bounds

            # Originally used mpl_toolkits.basemap.Basemap.aspect for this, but basemap installs an LGPL3 data package
            # so we can't have that in a release. Thankfully, manual computation ends up being quite simple
            aspect = (max_lat - min_lat) / (max_lon - min_lon)

            basemap_size = 512

            params = dict(
                bbox=f'{min_lon},{min_lat},{max_lon},{max_lat}',
                bboxSR=4326, imageSR=4326,
                size=f'{basemap_size},{int(aspect * basemap_size)}',
                dpi=2000,
                format='png32',
                transparent=True,
                f='image'
            )

            url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export'

            logger.info(f'Pulling basemap ({url}?{urlencode(params)})')

            try:
                elevations = results[['elevation']].values
                z_range = np.nanmax(elevations) - np.nanmin(elevations)
                z_coord = np.nanmin(elevations) - (0.1 * z_range)

                r = requests.get(url, params=params)
                r.raise_for_status()

                buf = BytesIO(r.content)

                img = Image.open(buf)
                img_data = np.flipud(np.array(img))

                lats = np.linspace(min_lat, max_lat, num=img.height)
                lons = np.linspace(min_lon, max_lon, num=img.width)

                X, Y = np.meshgrid(lons, lats)
                Z = np.full(X.shape, z_coord)

                logger.info('Plotting basemap')
                ax.plot_surface(X, Y, Z, rstride=1, cstride=1, facecolors=img_data / 255)
            except:
                logger.error('Failed to pull basemap, will not draw it')

        if self.render_type == 'scatter':
            logger.info('Plotting tomogram data')

            if self.style == 'db':
                v = dict(vmin=-30, vmax=-10)
                cbl = 'Tomogram (dB)'
            elif self.style == 'percentile':
                v = dict(vmin=0, vmax=1)
                cbl = 'Tomogram Cumulative value (%/100)'
            else:
                elevs = results[['elevation']].values
                v = dict(vmin=np.nanmin(elevs), vmax=np.nanmax(elevs))
                cbl = 'Voxel elevation'

            s = ax.scatter(
                xyz[:, 0], xyz[:, 1], xyz[:, 2],
                marker=',',
                c=results[['tomo_value']].values if self.style != 'percentile_elevation'
                else results[['elevation']].values,
                zdir='z',
                depthshade=True,
                cmap=self.cmap,
                **v
            )

            cbar = fig.colorbar(s, ax=ax)
            cbar.set_label(cbl)
        else:
            logger.info('Collecting data by lat/lon')
            lats = np.unique(results['lat'].values)
            lons = np.unique(results['lon'].values)

            data_dict = {}

            for r in results.itertuples(index=False):
                key = (r.lon, r.lat)

                if key not in data_dict:
                    data_dict[key] = ([r.elevation], [r.tomo_value])
                else:
                    data_dict[key][0].append(r.elevation)
                    data_dict[key][1].append(r.tomo_value)

            logger.info('Determining peaks')

            vals = np.empty((len(lats), len(lons)))

            for i, lat in enumerate(lats):
                for j, lon in enumerate(lons):
                    if (lon, lat) in data_dict:
                        elevs, tomo_vals = data_dict[(lon, lat)]

                        i_max = np.argmax(np.array(tomo_vals))
                        vals[i, j] = elevs[i_max]
                    else:
                        vals[i, j] = np.nan

            X2, Y2 = np.meshgrid(lons, lats)

            logger.info('Plotting peak surface')
            s = ax.plot_surface(
                X2,
                Y2,
                vals,
                rstride=1, cstride=1,
                color='xkcd:leaf'
            )

        ax.set_ylabel('Latitude')
        ax.set_xlabel('Longitude')
        ax.set_zlabel('Elevation w.r.t. dataset reference (m)')

        plt.tight_layout()

        buffer = BytesIO()

        logger.info('Writing plot to buffer')
        plt.savefig(buffer, format='png', facecolor='white')

        buffer.seek(0)
        plt.close(fig)
        return buffer.read()

    def toGif(self):
        orbit_elev, orbit_step, frame_duration, _, _ = self.render_params

        results = self.results()
        xyz = results[['lon', 'lat', 'elevation']].values

        fig = plt.figure(figsize=(10, 7))
        ax = fig.add_subplot(111, projection='3d')

        ax.view_init(elev=orbit_elev, azim=0)

        if not self.hide_basemap:
            min_lat, min_lon, max_lat, max_lon = self.bounds

            # Originally used mpl_toolkits.basemap.Basemap.aspect for this, but basemap installs an LGPL3 data package
            # so we can't have that in a release. Thankfully, manual computation ends up being quite simple
            aspect = (max_lat - min_lat) / (max_lon - min_lon)

            basemap_size = 512

            params = dict(
                bbox=f'{min_lon},{min_lat},{max_lon},{max_lat}',
                bboxSR=4326, imageSR=4326,
                size=f'{basemap_size},{int(aspect * basemap_size)}',
                dpi=2000,
                format='png32',
                transparent=True,
                f='image'
            )

            url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export'

            logger.info(f'Pulling basemap ({url}?{urlencode(params)})')

            try:
                elevations = results[['elevation']].values
                z_range = np.nanmax(elevations) - np.nanmin(elevations)
                z_coord = np.nanmin(elevations) - (0.1 * z_range)

                r = requests.get(url, params=params)
                r.raise_for_status()

                buf = BytesIO(r.content)

                img = Image.open(buf)
                img_data = np.flipud(np.array(img))

                lats = np.linspace(min_lat, max_lat, num=img.height)
                lons = np.linspace(min_lon, max_lon, num=img.width)

                X, Y = np.meshgrid(lons, lats)
                Z = np.full(X.shape, z_coord)

                logger.info('Plotting basemap')
                ax.plot_surface(X, Y, Z, rstride=1, cstride=1, facecolors=img_data / 255)
            except:
                logger.error('Failed to pull basemap, will not draw it')

        if self.render_type == 'scatter':
            logger.info('Plotting tomogram data')

            if self.style == 'db':
                v = dict(vmin=-30, vmax=-10)
                cbl = 'Tomogram (dB)'
            elif self.style == 'percentile':
                v = dict(vmin=0, vmax=1)
                cbl = 'Tomogram Cumulative value (%/100)'
            else:
                elevs = results[['elevation']].values
                v = dict(vmin=np.nanmin(elevs), vmax=np.nanmax(elevs))
                cbl = 'Voxel elevation'

            s = ax.scatter(
                xyz[:, 0], xyz[:, 1], xyz[:, 2],
                marker=',',
                c=results[['tomo_value']].values if self.style != 'percentile_elevation'
                else results[['elevation']].values,
                zdir='z',
                depthshade=True,
                cmap=self.cmap,
                **v
            )

            cbar = fig.colorbar(s, ax=ax)
            cbar.set_label(cbl)
        else:
            logger.info('Collecting data by lat/lon')
            lats = np.unique(results['lat'].values)
            lons = np.unique(results['lon'].values)

            data_dict = {}

            for r in results.itertuples(index=False):
                key = (r.lon, r.lat)

                if key not in data_dict:
                    data_dict[key] = ([r.elevation], [r.tomo_value])
                else:
                    data_dict[key][0].append(r.elevation)
                    data_dict[key][1].append(r.tomo_value)

            logger.info('Determining peaks')

            vals = np.empty((len(lats), len(lons)))

            for i, lat in enumerate(lats):
                for j, lon in enumerate(lons):
                    if (lon, lat) in data_dict:
                        elevs, tomo_vals = data_dict[(lon, lat)]

                        i_max = np.argmax(np.array(tomo_vals))
                        vals[i, j] = elevs[i_max]
                    else:
                        vals[i, j] = np.nan

            X2, Y2 = np.meshgrid(lons, lats)

            logger.info('Plotting peak surface')
            ax.plot_surface(
                X2,
                Y2,
                vals,
                rstride=1, cstride=1,
                color='xkcd:leaf'
            )

        ax.set_ylabel('Latitude')
        ax.set_xlabel('Longitude')
        ax.set_zlabel('Elevation w.r.t. dataset reference (m)')

        plt.tight_layout()

        buffer = BytesIO()

        with TemporaryDirectory() as td:
            for azim in range(0, 360, orbit_step):
                logger.info(f'Saving frame for azimuth = {azim}')

                ax.view_init(azim=azim)

                plt.savefig(join(td, f'fr_{azim}.png'))

            with contextlib.ExitStack() as stack:
                logger.info(f'Combining frames into final GIF')

                imgs = (stack.enter_context(Image.open(join(td, f'fr_{a}.png'))) for a in range(0, 360, orbit_step))
                img = next(imgs)
                img.save(buffer, format='GIF', append_images=imgs, save_all=True, duration=frame_duration, loop=0)

        buffer.seek(0)
        plt.close(fig)
        return buffer.read()

    def toCSV(self):
        df = self.results()[['lon', 'lat', 'elevation', 'tomo_value']]

        buf = BytesIO()

        df.to_csv(buf, header=False, index=False)

        buf.seek(0)
        return buf.read()

    def toNetCDF(self):
        ds = self.result_dataset()

        comp = dict(zlib=True, complevel=9)
        encoding = {vname: comp for vname in ds.data_vars}

        with TemporaryDirectory() as td:
            ds.to_netcdf(join(td, 'ds.nc'), encoding=encoding)

            with open(join(td, 'ds.nc'), 'rb') as fp:
                buf = BytesIO(fp.read())

        buf.seek(0)
        return buf.read()
