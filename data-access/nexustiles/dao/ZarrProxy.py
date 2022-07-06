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

import boto3

import xarray as xr
import s3fs
import numpy as np

from dask.diagnostics import ProgressBar

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
h.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger.addHandler(h)

class NexusDataTile(object):
    __data = None
    tile_id = None

    def __init__(self, data, _tile_id):   #change to data (dataset subset w/ temporal range), tid, coords
        import re

        if self.__data is None:
            self.__data = data

        if self.tile_id is None:
            self.tile_id = _tile_id

        if not re.search("^MUR_[0-9-T :.+]*_[0-9-T :.+]*_[0-9.-]*_[0-9.-]*_[0-9.-]*_[0-9.-]*", self.tile_id):
            raise ValueError("Bad tile id")

        self.__lat, self.__lon, self.__time, self.__vdata, self.__meta, self.__mv = self._get_data()

    def get_raw_data_array(self):
        return self.__data

    def get_lat_lon_time_data_meta(self):
        return self.__lat, self.__lon, self.__time, self.__vdata, self.__meta, self.__mv

    def as_model_tile(self):
        from nexustiles.model.nexusmodel import Tile, TileVariable

        tile = Tile()

        tile.latitudes = self.__lat
        tile.longitudes = self.__lon
        tile.times = self.__time
        tile.data = self.__vdata
        tile.is_multi = self.__mv
        tile.meta_data = self.__meta
        tile.tile_id = self.tile_id

        tile.dataset = self.__meta['main']['id']
        tile.dataset_id = self.__meta['main']['uuid']

        try:
            tile.granule = self.__data['main']['granules']
        except:
            pass

        variables = []

        for var in self.__data.data_vars:
            try:
                standard_name = self.__meta[var]['standard_name']
            except:
                standard_name = None

            variables.append(TileVariable(var, standard_name))

        tile.variables = variables

        return tile

    def _get_data(self):
        isMultiVar = False

        metadata = {'main': self.__data.attrs, 'analysed_sst': self.__data.analysed_sst.attrs,
                    'lat': self.__data.lat.attrs, 'lon': self.__data.lon.attrs, 'time': self.__data.time.attrs}

        tile_type = 'grid_tile'

        if tile_type == 'grid_tile': #for now, assume gridded
            latitude_data = np.ma.masked_invalid(self.__data.lat)
            longitude_data = np.ma.masked_invalid(self.__data.lon)

            with ProgressBar():
              grid_tile_data = np.ma.masked_invalid(self.__data.analysed_sst)  # POC for MUR data
        else:
            raise NotImplementedError("Only supports grid_tile")

        if len(grid_tile_data.shape) == 2:
            grid_tile_data = grid_tile_data[np.newaxis, :]

        return latitude_data, longitude_data, self.__data.time.values, grid_tile_data, metadata, isMultiVar


class ZarrProxy(object):
    def __init__(self, config):
        self.config = config
        self.__s3_bucket_name = config.get("s3", "bucket")
        self.__s3_region = config.get("s3", "region")
        self.__s3_public = config.getboolean("s3", "public", fallback=False)
        self.__s3_profile = config.get("s3", "profile", fallback=None)
        self.__s3 = boto3.resource('s3')
        self.__nexus_tile = None

        logger.info('Opening Zarr proxy')

        if self.__s3_public:
            store = f"https://{self.__s3_bucket_name}.s3.{self.__s3_region}.amazonaws.com/{self.config.get('s3', 'key')}"
        else:
            s3path = f"s3://{self.__s3_bucket_name}/{self.config.get('s3', 'key')}/"
            s3 = s3fs.S3FileSystem(self.__s3_public, profile=self.__s3_profile)
            store = s3fs.S3Map(root=s3path, s3=s3, check=False)

        zarr_data = xr.open_zarr(store=store, consolidated=True, mask_and_scale=False)
        zarr_data.analysed_sst.attrs['_FillValue'] = -32768
        zarr_data = xr.decode_cf(zarr_data, mask_and_scale=True)

        logger.info('Successfully opened Zarr proxy')

        self.__zarr_data = zarr_data

    #Interpreting tile id's as: MUR_<start_time>_<end_tim>_<lat_min>_<lat_max>_<lon_min>_<lon_max>
    def fetch_nexus_tiles(self, *tile_ids):
        import re

        if not isinstance(tile_ids[0], str):
            tile_ids = [str(tile.tile_id) for tile in tile_ids]

        res = []

        for tid in tile_ids:
            c = re.split("_", tid)

            parts = {
                'id': tid,
                'start_time': c[1],
                'end_time': c[2],
                'min_lat': float(c[3]),
                'max_lat': float(c[4]),
                'min_lon': float(c[5]),
                'max_lon': float(c[6])
            }

            tz_regex = "\\+00:00$"

            if re.search(tz_regex, parts['start_time']):
                parts['start_time'] = re.split(tz_regex, parts['start_time'])[0]

            if re.search(tz_regex, parts['end_time']):
                parts['end_time'] = re.split(tz_regex, parts['end_time'])[0]

            logger.debug(f"getting {parts['id']}")

            times = slice(parts['start_time'], parts['end_time'])
            lats = slice(parts['min_lat'], parts['max_lat'])
            lons = slice(parts['min_lon'], parts['max_lon'])

            nexus_tile = NexusDataTile(self.__zarr_data.sel(time=times, lat=lats, lon=lons), parts['id'])
            res.append(nexus_tile)

        return res