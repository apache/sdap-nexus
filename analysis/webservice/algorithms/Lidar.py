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

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException
from datetime import datetime
from pytz import timezone
from itertools import zip_longest, chain
from functools import partial
import json
import pandas as pd
from tempfile import NamedTemporaryFile
import zipfile


logger = logging.getLogger(__name__)


# NOTE: Current implementation expects the data vars to be in separate collections named
# <root_name>_<var_name>


EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
NP_EPOCH = np.datetime64('1970-01-01T00:00:00')
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

@nexus_handler
class LidarVegetation(NexusCalcHandler):
    name = "LIDAR Vegetation Data"
    path = "/stv/lidar"
    description = "Gets vegetation stats (canopy height, mean height, canopy cover)"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation. Required"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required."
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "latSlice": {
            "name": "Latitude slice",
            "type": "string",
            "description": "Comma separated values: latitude to slice on[,min lon of slice,max lon of slice]"
        },
        "lonSlice": {
            "name": "Longitude slice",
            "type": "string",
            "description": "Comma separated values: longitude to slice on[,min lat of slice,max lat of slice]"
        },
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        NexusCalcHandler.__init__(self, tile_service_factory, desired_projection='swath')

    def parse_arguments(self, request):
        # Parse input arguments

        try:
            ds = request.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        try:
            start_time = request.get_start_datetime()
            start_time = int((start_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
            end_time = int((end_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        if start_time > end_time:
            raise NexusProcessingException(
                reason="The starting time must be before the ending time. Received startTime: %s, endTime: %s" % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(reason="Parameter 'b' is required. 'b' must be comma-delimited float "
                                                  "formatted as Minimum (Western) Longitude, Minimum (Southern) "
                                                  "Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude.")

        return ds, start_time, end_time, bounding_polygon

    def calc(self, computeOptions, **args):
        dataset, start_time, end_time, bounding_polygon = self.parse_arguments(computeOptions)

        tile_service = self._get_tile_service()

        ds_zg   = f'{dataset}_ZG'
        ds_rh50 = f'{dataset}_RH050'
        ds_rh98 = f'{dataset}_RH098'
        ds_cc   = f'{dataset}_CC'

        get_tiles = partial(
            tile_service.find_tiles_in_polygon,
            bounding_polygon=bounding_polygon,
            start_time=start_time,
            end_time=end_time
        )

        tiles_zg = get_tiles(ds=ds_zg)
        tiles_rh50 = get_tiles(ds=ds_rh50)
        tiles_rh98 = get_tiles(ds=ds_rh98)
        tiles_cc = get_tiles(ds=ds_cc)

        logger.info(f'Matched tile counts by variable: ZG={len(tiles_zg):,}, RH050={len(tiles_rh50):,}, '
                    f'RH098={len(tiles_rh98):,}, CC={len(tiles_cc):,}')

        points_zg, points_50, points_98, points_cc = [], [], [], []

        for tile_zg, tile_50, tile_98, tile_cc in zip_longest(tiles_zg, tiles_rh50, tiles_rh98, tiles_cc):
            if tile_zg:
                logger.info(f'Processing ground height tile {tile_zg.tile_id}')

            if tile_50:
                logger.info(f'Processing mean vegetation height tile {tile_50.tile_id}')

            if tile_98:
                logger.info(f'Processing canopy height tile {tile_98.tile_id}')

            if tile_cc:
                logger.info(f'Processing canopy coverage tile {tile_cc.tile_id}')

            for np_zg, np_50, np_98, np_cc in zip_longest(
                    tile_zg.nexus_point_generator(),
                    tile_50.nexus_point_generator(),
                    tile_98.nexus_point_generator(),
                    tile_cc.nexus_point_generator()
            ):
                p_zg = dict(
                    latitude=np_zg.latitude,
                    longitude=np_zg.longitude,
                    time=np_zg.time,
                    data=np_zg.data_vals
                )

                if isinstance(p_zg['data'], list):
                    p_zg['data'] = p_zg['data'][0]

                points_zg.append(p_zg)

                p_50 = dict(
                    latitude=np_50.latitude,
                    longitude=np_50.longitude,
                    time=np_50.time,
                    data=np_50.data_vals
                )

                if isinstance(p_50['data'], list):
                    p_50['data'] = p_50['data'][0]

                points_50.append(p_50)

                p_98 = dict(
                    latitude=np_98.latitude,
                    longitude=np_98.longitude,
                    time=np_98.time,
                    data=np_98.data_vals
                )

                if isinstance(p_98['data'], list):
                    p_98['data'] = p_98['data'][0]

                points_98.append(p_98)

                p_cc = dict(
                    latitude=np_cc.latitude,
                    longitude=np_cc.longitude,
                    time=np_cc.time,
                    data=np_cc.data_vals
                )

                if isinstance(p_cc['data'], list):
                    p_cc['data'] = p_cc['data'][0]

                points_cc.append(p_cc)

        lats = np.unique([p['latitude'] for p in chain(points_zg, points_50, points_98, points_cc)])
        lons = np.unique([p['longitude'] for p in chain(points_zg, points_50, points_98, points_cc)])
        times = np.unique([datetime.utcfromtimestamp(p['time']) for p in chain(points_zg, points_50, points_98, points_cc)])

        vals_4d = np.full((len(times), len(lats), len(lons), 4), np.nan)

        data_dict = {}

        for zg, rh50, rh98, cc in zip_longest(points_zg, points_50, points_98, points_cc):
            if zg is not None:
                key = (datetime.utcfromtimestamp(zg['time']), zg['latitude'], zg['longitude'])

                if key not in data_dict:
                    data_dict[key] = [zg['data'], None, None, None]
                else:
                    data_dict[key][0] = zg['data']

            if rh50 is not None:
                key = (datetime.utcfromtimestamp(rh50['time']), rh50['latitude'], rh50['longitude'])

                if key not in data_dict:
                    data_dict[key] = [None, rh50['data'], None, None]
                else:
                    data_dict[key][1] = rh50['data']

            if rh98 is not None:
                key = (datetime.utcfromtimestamp(rh98['time']), rh98['latitude'], rh98['longitude'])

                if key not in data_dict:
                    data_dict[key] = [None, None, rh98['data'], None]
                else:
                    data_dict[key][2] = rh98['data']

            if cc is not None:
                key = (datetime.utcfromtimestamp(cc['time']), cc['latitude'], cc['longitude'])

                if key not in data_dict:
                    data_dict[key] = [None, None, None, cc['data']]
                else:
                    data_dict[key][3] = cc['data'] / 10000

        for i, t in enumerate(times):
            for j, lat in enumerate(lats):
                for k, lon in enumerate(lons):
                    vals_4d[i, j, k, :] = data_dict.get((t, lat, lon), [np.nan] * 4)

        ds = xr.DataArray(
            data=vals_4d,
            dims=['time', 'lat', 'lon', 'var'],
            coords=dict(
                time=(['time'], times),
                lat=(['lat'], lats),
                lon=(['lon'], lons),
                var=(['var'], ['ground_height', 'mean_veg_height', 'canopy_height', 'canopy_coverage'])
            )
        )

        ds = ds.to_dataset('var')

        return LidarResults(
            results=ds,
            meta=dict(
                ground_height_dataset=ds_zg,
                vegetation_mean_height_dataset=ds_rh50,
                canopy_height_dataset=ds_rh98,
                canopy_coverage_dataset=ds_cc,
                start_time=times[0].strftime(ISO_8601),
                end_time=times[-1].strftime(ISO_8601),
                b=f'{lons[0]},{lats[0]},{lons[-1]},{lats[-1]}'
            )
        )


class LidarResults(NexusResults):
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, **kwargs):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **kwargs)

    def meta(self):
        m = NexusResults.meta(self)

        del m['shortName']
        del m['bounds']

        return m

    def points_list(self):
        points = []

        ds = self.results()

        logger.info('Generating non-NaN points list')

        for i, t in enumerate(ds.time):
            for j, lat in enumerate(ds.lat):
                for k, lon in enumerate(ds.lon):
                    ds_point = ds.isel(time=i, lat=j, lon=k)

                    zg = ds_point['ground_height'].item()
                    rh50 = ds_point['mean_veg_height'].item()
                    rh98 = ds_point['canopy_height'].item()
                    cc = ds_point['canopy_coverage'].item()

                    if all([np.isnan(v) for v in [zg, rh50, rh98, cc]]):
                        continue

                    point_ts = int((t.data - NP_EPOCH) / np.timedelta64(1, 's'))

                    points.append(dict(
                        latitude=lat.item(),
                        longitude=lon.item(),
                        time=point_ts,
                        time_iso=datetime.utcfromtimestamp(point_ts).strftime(ISO_8601),
                        ground_height=zg,
                        mean_vegetation_height=rh50,
                        canopy_height=rh98,
                        canopy_coverage=cc
                    ))

        return points

    def toImage(self):
        ds = self.results()
        meta = self.meta()

        min_lon, min_lat, max_lon, max_lat = [float(c) for c in meta['b'].split(',')]

        lat_len = max_lat - min_lat
        lon_len = max_lon - min_lon

        if lat_len >= lon_len:
            diff = lat_len - lon_len

            min_lon -= (diff / 2)
            max_lon += (diff / 2)
        else:
            diff = lon_len - lat_len

            min_lat -= (diff / 2)
            max_lat += (diff / 2)

        extent = [min_lon, max_lon, min_lat, max_lat]

        fig, ((rh50_ax, rh98_ax), (zg_ax, cc_ax)) = plt.subplots(2, 2, figsize=(10, 10))

        rh50_im = rh50_ax.imshow(np.squeeze(ds['mean_veg_height']).data, extent=extent, aspect='equal', cmap='viridis')
        rh98_im = rh98_ax.imshow(np.squeeze(ds['canopy_height']).data, extent=extent, aspect='equal', cmap='viridis')
        zg_im = zg_ax.imshow(np.squeeze(ds['ground_height']).data, extent=extent, aspect='equal', cmap='viridis')
        cc_im = cc_ax.imshow(np.squeeze(ds['canopy_coverage']).data, extent=extent, aspect='equal', cmap='viridis')

        rh50_cb = plt.colorbar(rh50_im, ax=rh50_ax, label='Height above terrain [m]')
        rh98_cb = plt.colorbar(rh98_im, ax=rh98_ax, label='Height above terrain [m]')
        zg_cb = plt.colorbar(zg_im, ax=zg_ax, label='Height above ellipsoid [m]')
        cc_cb = plt.colorbar(cc_im, ax=cc_ax, label='Coverage [%]')

        rh50_ax.set_title('Mean Vegetation Height')
        rh98_ax.set_title('Canopy Height')
        zg_ax.set_title('Terrain Height')
        cc_ax.set_title('Canopy Coverage')

        plt.tight_layout()

        buffer = BytesIO()

        plt.savefig(buffer, format='png', facecolor='white')
        buffer.seek(0)
        return buffer.read()

    def toJson(self):
        points = self.points_list()

        logger.info('Dumping to JSON string')

        return json.dumps(dict(
            meta=self.meta(),
            data=points
        ), indent=4)

    def toNetCDF(self):
        ds: xr.Dataset = self.results()
        meta = self.meta()

        ds.attrs.update(meta)

        with NamedTemporaryFile(suffix='.nc', mode='w') as fp:
            comp = {"zlib": True, "complevel": 9}
            encoding = {vname: comp for vname in ds.data_vars}

            ds.to_netcdf(fp.name, encoding=encoding)

            fp.flush()

            with open(fp.name, 'rb') as rfp:
                buf = BytesIO(rfp.read())

        buf.seek(0)
        return buf.read()

    def toCSV(self):
        df = pd.DataFrame(self.points_list())

        buffer = BytesIO()

        df.to_csv(buffer, index=False)

        buffer.seek(0)
        return buffer.read()

    def toZip(self):
        csv_results = self.toCSV()

        buffer = BytesIO()

        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip:
            zip.writestr('lidar_subset.csv', csv_results)

        buffer.seek(0)
        return buffer.read()

