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
from io import BytesIO
from os.path import join
from tempfile import TemporaryDirectory

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

logger = logging.getLogger(__name__)


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
            "description": "Desired output format. Must be either \"PNG\" or \"GIF\". Required."
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

        if output not in ['PNG', 'GIF']:
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

        return ds, parameter_s, bounding_poly, min_elevation, max_elevation, (orbit_elev, orbit_step, frame_duration,
                                                                              view_azim, view_elev)

    def calc(self, computeOptions, **args):
        (ds, parameter, bounding_poly, min_elevation, max_elevation, render_params) = self.parse_args(computeOptions)

        min_lat = bounding_poly.bounds[1]
        max_lat = bounding_poly.bounds[3]
        min_lon = bounding_poly.bounds[0]
        max_lon = bounding_poly.bounds[2]

        tile_service = self._get_tile_service()

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

        cmap = mpl.colormaps['viridis']
        normalizer = mpl.colors.Normalize(vmin=-30, vmax=-10)

        def c(v):
            return cmap(normalizer(v))

        logger.info('Building dataframe')

        lats = np.array([p['latitude'] for p in data])
        lons = np.array([p['longitude'] for p in data])
        elevs = np.array([p['elevation'] for p in data])

        tomo = np.array([p['data'] for p in data])
        tomo = 10 * np.log10(tomo)

        tomo_rgb = np.array([list(c(v))[0:3] for v in tomo]) * 256

        df = pd.DataFrame(
            np.hstack((lats[:, np.newaxis], lons[:, np.newaxis], elevs[:, np.newaxis], tomo_rgb, tomo[:, np.newaxis])),
            columns=['lat', 'lon', 'elevation', 'red', 'green', 'blue', 'tomo_value']
        )

        logger.info(f'DataFrame:\n{df}')

        return Tomogram3DResults(df, render_params)


class Tomogram3DResults(NexusResults):
    def __init__(self, results=None, render_params=None, meta=None, stats=None, computeOptions=None, status_code=200, **args):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **args)
        self.render_params = render_params

    def results(self):
        r: pd.DataFrame = NexusResults.results(self)
        return r

    def __common(self):
        xyz = self.results()[['lon', 'lat', 'elevation']].values

        plt.figure(figsize=(10,10))
        return xyz, plt.axes(projection='3d')

    def toImage(self):
        _, _, _, view_azim, view_elev = self.render_params

        xyz, ax = self.__common()

        ax.view_init(elev=view_elev, azim=view_azim)

        logger.info('Plotting data')

        ax.scatter(
            xyz[:, 0], xyz[:, 1], xyz[:, 2],
            marker='D',
            facecolors=self.results()[['red', 'green', 'blue']].values.astype(np.uint8) / 255,
            zdir='z',
            depthshade=True
        )

        buffer = BytesIO()

        logger.info('Writing plot to buffer')
        plt.savefig(buffer, format='png', facecolor='white')

        buffer.seek(0)
        return buffer.read()

    def toGif(self):
        orbit_elev, orbit_step, frame_duration, _, _ = self.render_params

        xyz, ax = self.__common()

        ax.view_init(elev=orbit_elev, azim=0)

        logger.info('Plotting data')

        ax.scatter(
            xyz[:, 0], xyz[:, 1], xyz[:, 2],
            marker='D',
            facecolors=self.results()[['red', 'green', 'blue']].values.astype(np.uint8) / 255,
            zdir='z',
            depthshade=True
        )

        buffer = BytesIO()

        with TemporaryDirectory() as td:
            for azim in range(0, 360, orbit_step):
                logger.info(f'Saving frame for azimuth = {azim}')

                ax.view_init(azim=azim)

                # plt.draw()
                # plt.pause(0.001)
                plt.savefig(join(td, f'fr_{azim}.png'))

            with contextlib.ExitStack() as stack:
                logger.info(f'Combining frames into final GIF')

                imgs = (stack.enter_context(Image.open(join(td, f'fr_{a}.png'))) for a in range(0, 360, orbit_step))
                img = next(imgs)
                img.save(buffer, format='GIF', append_images=imgs, save_all=True, duration=frame_duration, loop=0)

        buffer.seek(0)
        return buffer.read()
