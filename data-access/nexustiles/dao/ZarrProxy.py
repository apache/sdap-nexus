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

import boto3
import numpy as np
import s3fs
import xarray as xr
from dask.diagnostics import ProgressBar
from webservice.webmodel import NexusProcessingException

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
h.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger.addHandler(h)

class NexusDataTile(object):
    __data = None
    tile_id = None

    def __init__(self, data, _tile_id, var_name):   #change to data (dataset subset w/ temporal range), tid, coords
        import re

        if self.__data is None:
            self.__data = data

        if self.tile_id is None:
            self.tile_id = _tile_id

        if not re.search("^.*_[0-9-T :.+]*_[0-9-T :.+]*_[0-9.-]*_[0-9.-]*_[0-9.-]*_[0-9.-]*$", self.tile_id):
            raise NexusProcessingException(reason="Bad tile id", code=500)

        self.__vars = var_name

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

        tile.dataset = self.__meta['main']['id'] if 'id' in self.__meta['main'] else None
        tile.dataset_id = self.__meta['main']['uuid'] if 'uuid' in self.__meta['main'] else None
        tile.granule = self.__meta['main']['granules'] if 'granules' in self.__meta['main'] else None

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

        metadata = {'main': self.__data.attrs, 'lat': self.__data.lat.attrs,
                    'lon': self.__data.lon.attrs, 'time': self.__data.time.attrs}

        for var in self.__vars:
            metadata[var['name_s']] = self.__data[var['name_s']].attrs

        tile_type = 'grid_tile'

        if not isMultiVar:
            self.__vars = self.__vars[0]

        if tile_type == 'grid_tile': #for now, assume gridded
            latitude_data = np.ma.masked_invalid(self.__data.lat)
            longitude_data = np.ma.masked_invalid(self.__data.lon)

            with ProgressBar():
              grid_tile_data = np.ma.masked_invalid(self.__data[self.__vars['name_s']])
        else:
            raise NotImplementedError("Only supports grid_tile")

        if len(grid_tile_data.shape) == 2:
            grid_tile_data = grid_tile_data[np.newaxis, :]

        return latitude_data, longitude_data, self.__data.time.values, grid_tile_data, metadata, isMultiVar

class ZarrProxy(object):
    mock_solr_for_testing = True

    def __init__(self, config, test_fs = None, open_direct=False, **kwargs):
        from .SolrProxy import SolrProxy

        import io, configparser

        self.config = config
        self.__s3_bucket_name = config.get("s3", "bucket")
        self.__s3_region = config.get("s3", "region")
        self.__s3_public = config.getboolean("s3", "public", fallback=False)
        self.__s3_profile = config.get("s3", "profile", fallback=None)
        self.__s3 = boto3.resource('s3')
        self.__nexus_tile = None

        solr_config_txt = f"""
        [solr]
        host={config.get("solr", "host", fallback='http://localhost:8983')}
        core=nexusdatasets
        """

        buf = io.StringIO(solr_config_txt)
        solr_config = configparser.ConfigParser()
        solr_config.read_file(buf)

        if not ZarrProxy.mock_solr_for_testing:
            self._metadata_store = SolrProxy(solr_config)
        else:
            import mock

            mock_solr = mock.MagicMock()
            mock_solr.do_query_raw = ZarrProxy.mock_query

            self._metadata_store = mock_solr


        if open_direct:
            logger.info('Opening Zarr proxy')

            if self.__s3_public:
                store = f"https://{self.__s3_bucket_name}.s3.{self.__s3_region}.amazonaws.com/{self.config.get('s3', 'key')}"
            else:
                s3path = f"s3://{self.__s3_bucket_name}/{self.config.get('s3', 'key')}/"
                s3 = s3fs.S3FileSystem(self.__s3_public, profile=self.__s3_profile) if test_fs is None else test_fs
                store = s3fs.S3Map(root=s3path, s3=s3, check=False)

            zarr_data = xr.open_zarr(store=store, consolidated=True, mask_and_scale=False)
            zarr_data.analysed_sst.attrs['_FillValue'] = -32768
            zarr_data = xr.decode_cf(zarr_data, mask_and_scale=True)

            self.__variables = [{"name_s": "analysed_sst", "fill_d": -32768}]

            logger.info('Successfully opened Zarr proxy')

            self.__zarr_data = zarr_data

    def _get_ds_info(self, ds):
        store = self._metadata_store

        query_response = store.do_query_raw((f'id:{ds}'))

        if not query_response['responseHeader']['status'] == 0:
            raise NexusProcessingException(reason="bad solr response")

        if not query_response['response']['numFound'] == 1:
            raise  NexusProcessingException(
                reason=f"wrong number of datasets returned from solr: {query_response['response']['numFound']}",
                code=400 if query_response['response']['numFound'] == 0 else 500
            )

        ds_info = query_response['response']['docs'][0]

        return ds_info['variables'], ds_info['public_b'], ds_info['s3_uri_s']

    def open_dataset(self, ds, test_fs = None):
        variables, public, path = self._get_ds_info(ds)

        logger.info('Opening Zarr proxy')

        if public:
            store = path
        else:
            s3path = path
            s3 = s3fs.S3FileSystem(public, profile=self.__s3_profile) if test_fs is None else test_fs
            store = s3fs.S3Map(root=s3path, s3=s3, check=False)

        zarr_data = xr.open_zarr(store=store, consolidated=True, mask_and_scale=False)

        for variable in variables:
            zarr_data[variable['name_s']].attrs['_FillValue'] = variable['fill_d']

        zarr_data = xr.decode_cf(zarr_data, mask_and_scale=True)

        logger.info('Successfully opened Zarr proxy')

        self.__zarr_data = zarr_data
        self.__variables = variables

    #Interpreting tile id's as: MUR_<start_time>_<end_tim>_<lat_min>_<lat_max>_<lon_min>_<lon_max>
    def fetch_nexus_tiles(self, *tile_ids):
        import re

        if not isinstance(tile_ids[0], str):
            tile_ids = [str(tile.tile_id) for tile in tile_ids]

        res = []

        for tid in tile_ids:
            parts = ZarrProxy.parse_tile_id_to_bounds(tid)

            tz_regex = "\\+00:00$"

            if re.search(tz_regex, parts['start_time']):
                parts['start_time'] = re.split(tz_regex, parts['start_time'])[0]

            if re.search(tz_regex, parts['end_time']):
                parts['end_time'] = re.split(tz_regex, parts['end_time'])[0]

            logger.debug(f"getting {parts['id']}")

            times = slice(parts['start_time'], parts['end_time'])
            lats = slice(parts['min_lat'], parts['max_lat'])
            lons = slice(parts['min_lon'], parts['max_lon'])

            nexus_tile = NexusDataTile(self.__zarr_data.sel(time=times, lat=lats, lon=lons), parts['id'], self.__variables)
            res.append(nexus_tile)

        return res

    @staticmethod
    def parse_tile_id_to_bounds(tile_id):
        import re

        c = re.split("_", tile_id)

        parts = {
            'id': tile_id,
            'start_time': c[-6],
            'end_time': c[-5],
            'min_lat': float(c[-4]),
            'max_lat': float(c[-3]),
            'min_lon': float(c[-2]),
            'max_lon': float(c[-1])
        }

        return parts

    @staticmethod
    def mock_query(ds):
        import json

        if ds == "id:MUR25-JPL-L4-GLOB-v04.2":
            return json.load(open("/Users/rileykk/repo/incubator-sdap-nexus/data-access/tests/mock_mur_meta.json"))
        elif ds == "id:OISSS_L4_multimission_7day_v1":
            return json.load(open("/Users/rileykk/repo/incubator-sdap-nexus/data-access/tests/mock_oisss_meta.json"))
        elif ds == "id:JPL-L4-MRVA-CHLA-GLOB-v3.0":
            return json.load(open("/Users/rileykk/repo/incubator-sdap-nexus/data-access/tests/mock_chla_meta.json"))
        else:
            raise ValueError("unsupported dataset")

