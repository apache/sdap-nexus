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
from mpl_toolkits.axes_grid1 import make_axes_locatable


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

        lat_slice = request.get_argument('latSlice', None)
        lon_slice = request.get_argument('lonSlice', None)

        if lat_slice is not None:
            parts = lat_slice.split(',')

            if len(parts) not in [1, 3]:
                raise NexusProcessingException(
                    reason='latSlice must consist of either one number (lat to slice on), or 3 numbers separated by '
                           'commas (lat to slice on, min lon of slice, max lon of slice)', code=400
                )

            try:
                if len(parts) == 1:
                    lat_slice = (float(parts[0]), None, None)
                else:
                    lat_slice = tuple([float(p) for p in parts])
            except ValueError:
                raise NexusProcessingException(
                    reason='Invalid numerical component provided. latSlice must consist of either one number (lat to '
                           'slice on), or 3 numbers separated by commas (lon to slice on, min lat of slice, max lat of '
                           'slice)', code=400
                )

        if lon_slice is not None:
            parts = lon_slice.split(',')

            if len(parts) not in [1, 3]:
                raise NexusProcessingException(
                    reason='lonSlice must consist of either one number (lon to slice on), or 3 numbers separated by '
                           'commas (lon to slice on, min lat of slice, max lat of slice)', code=400
                )

            try:
                if len(parts) == 1:
                    lon_slice = (float(parts[0]), None, None)
                else:
                    lon_slice = tuple([float(p) for p in parts])
            except ValueError:
                raise NexusProcessingException(
                    reason='Invalid numerical component provided. lonSlice must consist of either one number (lon to '
                           'slice on), or 3 numbers separated by commas (lon to slice on, min lat of slice, max lat of '
                           'slice)', code=400
                )

        return ds, start_time, end_time, bounding_polygon, lon_slice, lat_slice

    def calc(self, computeOptions, **args):
        dataset, start_time, end_time, bounding_polygon, lon_slice, lat_slice = self.parse_arguments(computeOptions)

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
                npg_zg = tile_zg.nexus_point_generator()
            else:
                npg_zg = []

            if tile_50:
                logger.info(f'Processing mean vegetation height tile {tile_50.tile_id}')
                npg_50 = tile_50.nexus_point_generator()
            else:
                npg_50 = []

            if tile_98:
                logger.info(f'Processing canopy height tile {tile_98.tile_id}')
                npg_98 = tile_98.nexus_point_generator()
            else:
                npg_98 = []

            if tile_cc:
                logger.info(f'Processing canopy coverage tile {tile_cc.tile_id}')
                npg_cc = tile_cc.nexus_point_generator()
            else:
                npg_cc = []

            for np_zg, np_50, np_98, np_cc in zip_longest(
                    npg_zg,
                    npg_50,
                    npg_98,
                    npg_cc
            ):
                if npg_zg:
                    p_zg = dict(
                        latitude=np_zg.latitude,
                        longitude=np_zg.longitude,
                        time=np_zg.time,
                        data=np_zg.data_vals
                    )

                    if isinstance(p_zg['data'], list):
                        p_zg['data'] = p_zg['data'][0]

                    points_zg.append(p_zg)

                if npg_50:
                    p_50 = dict(
                        latitude=np_50.latitude,
                        longitude=np_50.longitude,
                        time=np_50.time,
                        data=np_50.data_vals
                    )

                    if isinstance(p_50['data'], list):
                        p_50['data'] = p_50['data'][0]

                    points_50.append(p_50)

                if npg_98:
                    p_98 = dict(
                        latitude=np_98.latitude,
                        longitude=np_98.longitude,
                        time=np_98.time,
                        data=np_98.data_vals
                    )

                    if isinstance(p_98['data'], list):
                        p_98['data'] = p_98['data'][0]

                    points_98.append(p_98)

                if npg_cc:
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
        ).to_dataset('var')

        slice_lat, slice_lon = None, None
        slice_min_lat, slice_max_lat = None, None
        slice_min_lon, slice_max_lon = None, None

        if lat_slice is not None:
            slice_lat, slice_min_lon, slice_max_lon = lat_slice

            if slice_min_lon is None:
                slice_min_lon = ds.lon.min().item()

            if slice_max_lon is None:
                slice_max_lon = ds.lon.max().item()

            if len(ds['time']) > 1:
                slice_ds = ds.mean(dim='time', skipna=True)
            else:
                slice_ds = ds

            lat_slice = slice_ds.sel(lat=slice_lat, method='nearest').sel(lon=slice(slice_min_lon, slice_max_lon))

        if lon_slice is not None:
            slice_lon, slice_min_lat, slice_max_lat = lon_slice

            if slice_min_lat is None:
                slice_min_lat = ds.lat.min().item()

            if slice_max_lat is None:
                slice_max_lat = ds.lat.max().item()

            if len(ds['time']) > 1:
                slice_ds = ds.mean(dim='time', skipna=True)
            else:
                slice_ds = ds

            lon_slice = slice_ds.sel(lon=slice_lon, method='nearest').sel(lat=slice(slice_min_lat, slice_max_lat))

        slices = (lat_slice, lon_slice)

        result_meta = dict(
                ground_height_dataset=ds_zg,
                vegetation_mean_height_dataset=ds_rh50,
                canopy_height_dataset=ds_rh98,
                canopy_coverage_dataset=ds_cc,
                start_time=times[0].strftime(ISO_8601),
                end_time=times[-1].strftime(ISO_8601),
                b=f'{lons[0]},{lats[0]},{lons[-1]},{lats[-1]}'
            )

        if lat_slice is not None:
            result_meta['slice_lat'] = (slice_lat, slice_min_lon, slice_max_lon)

        if lon_slice is not None:
            result_meta['slice_lon'] = (slice_lon, slice_min_lat, slice_max_lat)

        return LidarResults(
            results=(ds, slices),
            meta=result_meta
        )


class LidarResults(NexusResults):
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, **kwargs):
        NexusResults.__init__(self, results, meta, stats, computeOptions, status_code, **kwargs)

    def meta(self):
        m = NexusResults.meta(self)

        del m['shortName']
        del m['bounds']

        return m

    def results(self, reduce_time=False):
        ds, (lat_slice, lon_slice) = NexusResults.results(self)

        if reduce_time and len(ds['time']) > 1:
            ds = ds.mean(dim='time', skipna=True)

        return ds, (lat_slice, lon_slice)

    def points_list(self):
        points = []
        slice_points = {}

        ds, (lat_slice, lon_slice) = self.results()

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

        if lat_slice is not None:
            if len(lat_slice['time']) > 1:
                lat_slice = lat_slice.mean(dim='time', skipna=True)
            else:
                lat_slice = lat_slice.squeeze(dim='time')

            pts = [dict(
                longitude=lat_slice.isel(lon=l).lon.item(),
                ground_height=lat_slice.ground_height.isel(lon=l).item(),
                mean_vegetation_height=lat_slice.mean_veg_height.isel(lon=l).item(),
                canopy_height=lat_slice.canopy_height.isel(lon=l).item(),
                canopy_coverage=lat_slice.canopy_coverage.isel(lon=l).item()
            ) for l in range(len(ds['lon']))]

            slice_points['latitude'] = dict(latitude=lat_slice.lat.item(), slice=pts)

        if lon_slice is not None:
            if len(lon_slice['time']) > 1:
                lon_slice = lon_slice.mean(dim='time', skipna=True)
            else:
                lon_slice = lon_slice.squeeze(dim='time')

            pts = [dict(
                latitude=lon_slice.isel(lat=l).lat.item(),
                ground_height=lon_slice.ground_height.isel(lat=l).item(),
                mean_vegetation_height=lon_slice.mean_veg_height.isel(lat=l).item(),
                canopy_height=lon_slice.canopy_height.isel(lat=l).item(),
                canopy_coverage=lon_slice.canopy_coverage.isel(lat=l).item()
            ) for l in range(len(ds['lat']))]

            slice_points['longitude'] = dict(longitude=lat_slice.lon.item(), slice=pts)

        return points, slice_points

    @staticmethod
    def slice_point_list(ds, s, coord):
        if len(ds['time']) > 1:
            s = s.mean(dim='time', skipna=True)
        else:
            s = s.squeeze(dim='time')

        if coord == 'latitude':
            pts = [dict(
                s=s.isel(lon=l).lon.item(),
                ground_height=s.ground_height.isel(lon=l).item(),
                mean_vegetation_height=s.mean_veg_height.isel(lon=l).item(),
                canopy_height=s.canopy_height.isel(lon=l).item(),
                canopy_coverage=s.canopy_coverage.isel(lon=l).item()
            ) for l in range(len(s['lon']))]

            return s.lat.item(), pts
        else:
            pts = [dict(
                s=s.isel(lat=l).lat.item(),
                ground_height=s.ground_height.isel(lat=l).item(),
                mean_vegetation_height=s.mean_veg_height.isel(lat=l).item(),
                canopy_height=s.canopy_height.isel(lat=l).item(),
                canopy_coverage=s.canopy_coverage.isel(lat=l).item()
            ) for l in range(len(s['lat']))]

            return s.lon.item(), pts

    def toImage(self):
        ds, (lat_slice, lon_slice) = self.results()
        meta = self.meta()

        slice_lat, slice_lon = meta.get('slice_lat'), meta.get('slice_lon')

        n_rows = 2
        
        if lat_slice is not None:
            n_rows += 2
        if lon_slice is not None:
            n_rows += 2

        min_lon, min_lat, max_lon, max_lat = (
            ds.lon.min().item(),
            ds.lat.min().item(),
            ds.lon.max().item(),
            ds.lat.max().item(),
        )

        extent = [min_lon, max_lon, min_lat, max_lat]

        fig = plt.figure(
            figsize=(8, 10 + (max(0, n_rows - 2) * 4)), constrained_layout=True
        )

        gs = fig.add_gridspec(n_rows, 2)

        rh50_ax = fig.add_subplot(gs[0, 0])
        rh98_ax = fig.add_subplot(gs[0, 1])
        zg_ax = fig.add_subplot(gs[1, 0])
        cc_ax = fig.add_subplot(gs[1, 1])

        rh50_im = rh50_ax.imshow(np.flipud(np.squeeze(ds['mean_veg_height'])), extent=extent, aspect='equal', cmap='viridis')
        rh98_im = rh98_ax.imshow(np.flipud(np.squeeze(ds['canopy_height'])), extent=extent, aspect='equal', cmap='viridis')
        zg_im = zg_ax.imshow(np.flipud(np.squeeze(ds['ground_height'])), extent=extent, aspect='equal', cmap='viridis')
        cc_im = cc_ax.imshow(np.flipud(np.squeeze(ds['canopy_coverage'])) * 100, extent=extent, aspect='equal', cmap='viridis', vmin=0, vmax=100)

        if slice_lat is not None:
            rh50_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')
            rh98_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')
            zg_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')
            cc_ax.plot([slice_lat[1], slice_lat[2]], [slice_lat[0], slice_lat[0]], 'r--')

        if slice_lon is not None:
            rh50_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')
            rh98_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')
            zg_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')
            cc_ax.plot([slice_lon[0], slice_lon[0]], [slice_lon[1], slice_lon[2]], 'r--')

        divider_50 = make_axes_locatable(rh50_ax)
        divider_98 = make_axes_locatable(rh98_ax)
        divider_zg = make_axes_locatable(zg_ax)
        divider_cc = make_axes_locatable(cc_ax)

        cax_50 = divider_50.append_axes("right", size="5%", pad=0.05)
        cax_98 = divider_98.append_axes("right", size="5%", pad=0.05)
        cax_zg = divider_zg.append_axes("right", size="5%", pad=0.05)
        cax_cc = divider_cc.append_axes("right", size="5%", pad=0.05)

        rh50_cb = plt.colorbar(rh50_im, cax=cax_50, label='Height above terrain [m]', use_gridspec=True)
        rh98_cb = plt.colorbar(rh98_im, cax=cax_98, label='Height above terrain [m]', use_gridspec=True)
        zg_cb = plt.colorbar(zg_im, cax=cax_zg, label='Height above ellipsoid [m]', use_gridspec=True)
        cc_cb = plt.colorbar(cc_im, cax=cax_cc, label='Coverage [%]', use_gridspec=True)

        rh50_ax.set_title('Mean Vegetation Height')
        rh98_ax.set_title('Canopy Height')
        zg_ax.set_title('Terrain Height')
        cc_ax.set_title('Canopy Coverage')

        row = 2

        for s, coord in zip([lat_slice, lon_slice], ['latitude', 'longitude']):
            if s is None:
                continue

            slice_ax = fig.add_subplot(gs[row, :])

            slice_point, pts = LidarResults.slice_point_list(ds, s, coord)

            x_lim = [min_lon, max_lon] if coord == 'latitude' else [min_lat, max_lat]

            x_pts = [p['s'] for p in pts]
            rh50_pts = np.array([p['mean_vegetation_height'] for p in pts])
            rh98_pts = np.array([p['canopy_height'] for p in pts])
            zg_pts = np.array([p['ground_height'] for p in pts])
            cc_pts = np.array([p['canopy_coverage'] for p in pts]) * 100

            slice_ax.plot(
                x_pts, rh98_pts + zg_pts,
                x_pts, rh50_pts + zg_pts,
                x_pts, zg_pts,
            )

            slice_ax.set_title(f'Slice at {coord}={slice_point}\nHeights w.r.t. to reference ellipsoid (m)')
            slice_ax.ticklabel_format(useOffset=False)
            slice_ax.set_xlim(x_lim)

            slice_ax.legend([
                'Canopy Height',
                'Mean Vegetation Height',
                'Ground Height',
            ])

            cc_slice_ax = fig.add_subplot(gs[row + 1, :])

            cc_slice_ax.plot(x_pts, cc_pts)
            cc_slice_ax.ticklabel_format(useOffset=False)
            cc_slice_ax.set_ylim([0, 100])
            cc_slice_ax.set_xlim(x_lim)

            cc_slice_ax.set_title(f'Slice at {coord}={slice_point}\nCanopy coverage (%)')

            cc_slice_ax.legend(['Canopy Coverage'])

            row += 2

        buffer = BytesIO()

        plt.savefig(buffer, format='png', facecolor='white')
        buffer.seek(0)

        plt.close(fig)
        return buffer.read()

    def toJson(self):
        points, slice_points = self.points_list()

        logger.info('Dumping to JSON string')

        return json.dumps(dict(
            meta=self.meta(),
            data=points,
            slices=slice_points
        ), indent=4)

    def toNetCDF(self):
        ds, (lat_slice, lon_slice) = self.results()
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

