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
import sys
from datetime import datetime
from urllib.parse import urlparse

import numpy as np
import numpy.ma as ma
import rioxarray
import xarray as xr
from nexustiles.AbstractTileService import AbstractTileService
from nexustiles.exception import NexusTileServiceException
from nexustiles.model.nexusmodel import Tile, BBox, TileVariable
from pytz import timezone
from shapely.geometry import MultiPolygon, box
from yarl import URL
from rioxarray.exceptions import MissingCRS
from nexustiles.backends.cog import SolrProxy

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger(__name__)


class CoGBackend(AbstractTileService):
    def __init__(self, dataset_name, bands, solr_config, config=None):
        AbstractTileService.__init__(self, dataset_name)
        self.__config = config if config is not None else {}

        logger.info(f'Opening CoG backend at for dataset {self._name}')

        self.__bands = bands

        self.__longitude = 'longitude'
        self.__latitude = 'latitude'
        self.__time = 'time'

        # self.__depth = config['coords'].get('depth')

        self.__solr: SolrProxy = SolrProxy(solr_config)

    def get_dataseries_list(self, simple=False):
        ds = dict(
            shortName=self._name,
            title=self._name,
            type='Cloud Optimized GeoTIFF'
        )

        if not simple:
            min_date, max_date = self.__solr.date_range_for_dataset(self._name)

            ds['start'] = (min_date - EPOCH).total_seconds()
            ds['end'] = (max_date - EPOCH).total_seconds()
            ds['iso_start'] = min_date.strftime(ISO_8601)
            ds['iso_end'] = max_date.strftime(ISO_8601)

        return [ds]

    def find_tile_by_id(self, tile_id, **kwargs):
        return [tile_id]

    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):
        return tile_ids

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, dataset, start_time, end_time,
                               metrics_callback=None, **kwargs):
        return self.__solr.find_days_in_range_asc(
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            dataset,
            start_time,
            end_time,
            **kwargs
        )

    def find_tile_by_polygon_and_most_recent_day_of_year(self, bounding_polygon, ds, day_of_year, **kwargs):
        """
        Given a bounding polygon, dataset, and day of year, find tiles in that dataset with the same bounding
        polygon and the closest day of year.

        For example:
            given a polygon minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; and day of year=32
            search for first tile in MY_DS with identical bbox and day_of_year <= 32 (sorted by day_of_year desc)

        Valid matches:
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 32
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 30

        Invalid matches:
            minx=1, miny=0, maxx=2, maxy=1; dataset=MY_DS; day of year = 32
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_OTHER_DS; day of year = 32
            minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 30 if minx=0, miny=0, maxx=1, maxy=1; dataset=MY_DS; day of year = 32 also exists

        :param bounding_polygon: The exact bounding polygon of tiles to search for
        :param ds: The dataset name being searched
        :param day_of_year: Tile day of year to search for, tile nearest to this day (without going over) will be returned
        :return: List of one tile from ds with bounding_polygon on or before day_of_year or raise NexusTileServiceException if no tile found
        """

        raise NotImplementedError()

    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return self.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon, dataset, time, time, **kwargs)

    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, dataset, time, **kwargs):
        return self.find_tiles_in_polygon(bounding_polygon, dataset, time, time, **kwargs)

    def find_tiles_in_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1, **kwargs):
        tiffs = self.__solr.find_tiffs_in_bounds(
            ds,
            start_time,
            end_time,
            {
                'min_lat': min_lat,
                'max_lat': max_lat,
                'min_lon': min_lon,
                'max_lon': max_lon
            }
        )

        params = {
                'min_lat': min_lat,
                'max_lat': max_lat,
                'min_lon': min_lon,
                'max_lon': max_lon
            }

        if 'depth' in kwargs:
            params['depth'] = kwargs['depth']
        elif 'min_depth' in kwargs or 'max_depth' in kwargs:
            params['min_depth'] = kwargs.get('min_depth')
            params['max_depth'] = kwargs.get('max_depth')

        return[CoGBackend.__to_url(
            self._name,
            tiff['path_s'],
            **params) for tiff in tiffs]

    def find_tiles_in_polygon(self, bounding_polygon, ds=None, start_time=None, end_time=None, **kwargs):
        # Find tiles that fall within the polygon in the Solr index
        tiffs = self.__solr.find_tiffs_in_bounds(
            ds,
            start_time,
            end_time,
            bounding_polygon
        )

        bounds = bounding_polygon.bounds

        params = {
            'min_lat': bounds[1],
            'max_lat': bounds[3],
            'min_lon': bounds[0],
            'max_lon': bounds[2]
        }

        if 'depth' in kwargs:
            params['depth'] = kwargs['depth']
        elif 'min_depth' in kwargs or 'max_depth' in kwargs:
            params['min_depth'] = kwargs.get('min_depth')
            params['max_depth'] = kwargs.get('max_depth')

        return [CoGBackend.__to_url(
            self._name,
            tiff['path_s'],
            **params) for tiff in tiffs]

    def find_tiles_by_metadata(self, metadata, ds=None, start_time=0, end_time=-1, **kwargs):
        """
        Return list of tiles whose metadata matches the specified metadata, start_time, end_time.
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of tiles
        """
        raise NotImplementedError()

    def find_tiles_by_exact_bounds(self, bounds, ds, start_time, end_time, **kwargs):
        """
        The method will return tiles with the exact given bounds within the time range. It differs from
        find_tiles_in_polygon in that only tiles with exactly the given bounds will be returned as opposed to
        doing a polygon intersection with the given bounds.

        :param bounds: (minx, miny, maxx, maxy) bounds to search for
        :param ds: Dataset name to search
        :param start_time: Start time to search (seconds since epoch)
        :param end_time: End time to search (seconds since epoch)
        :param kwargs: fetch_data: True/False = whether or not to retrieve tile data
        :return:
        """
        min_lon = bounds[0]
        min_lat = bounds[1]
        max_lon = bounds[2]
        max_lat = bounds[3]

        return self.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs)

    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        # Due to the precise nature of gridded Zarr's subsetting, it doesn't make sense to have a boundary region like
        # this
        raise NotImplementedError()

    def get_min_max_time_by_granule(self, ds, granule_name):
        raise NotImplementedError()

    def get_dataset_overall_stats(self, ds):
        raise NotImplementedError()

    def get_stats_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        raise NotImplementedError()

    def get_bounding_box(self, tile_ids):
        """
        Retrieve a bounding box that encompasses all of the tiles represented by the given tile ids.
        :param tile_ids: List of tile ids
        :return: shapely.geometry.Polygon that represents the smallest bounding box that encompasses all of the tiles
        """

        raise NotImplementedError()

    # def __get_ds_min_max_date(self):
    #     min_date = self.__ds[self.__time].min().to_numpy()
    #     max_date = self.__ds[self.__time].max().to_numpy()
    #
    #     if np.issubdtype(min_date.dtype, np.datetime64):
    #         min_date = ((min_date - np.datetime64(EPOCH)) / 1e9).astype(int).item()
    #
    #     if np.issubdtype(max_date.dtype, np.datetime64):
    #         max_date = ((max_date - np.datetime64(EPOCH)) / 1e9).astype(int).item()
    #
    #     return min_date, max_date

    def get_min_time(self, tile_ids, ds=None):
        """
        Get the minimum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        paths = [URL(t).query['path'] for t in tile_ids]

        min_time = self.__solr.find_min_date_from_tiles(paths, self._name)
        return int((min_time - EPOCH).total_seconds())

    def get_max_time(self, tile_ids, ds=None):
        """
        Get the maximum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        paths = [URL(t).query['path'] for t in tile_ids]

        max_time = self.__solr.find_max_date_from_tiles(paths, self._name)
        return int((max_time - EPOCH).total_seconds())

    def get_distinct_bounding_boxes_in_polygon(self, bounding_polygon, ds, start_time, end_time):
        """
        Get a list of distinct tile bounding boxes from all tiles within the given polygon and time range.
        :param bounding_polygon: The bounding polygon of tiles to search for
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of distinct bounding boxes (as shapely polygons) for tiles in the search polygon
        """
        raise NotImplementedError()

    def get_tile_count(self, ds, bounding_polygon=None, start_time=0, end_time=-1, metadata=None, **kwargs):
        """
        Return number of tiles that match search criteria.
        :param ds: The dataset name to search
        :param bounding_polygon: The polygon to search for tiles
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :return: number of tiles that match search criteria
        """
        return len(self.__solr.find_tiffs_in_bounds(
            ds,
            start_time,
            end_time,
            bounds=bounding_polygon
        ))

    def fetch_data_for_tiles(self, *tiles):
        for tile in tiles:
            self.__fetch_data_for_tile(tile)

        return tiles

    @staticmethod
    def __open_granule_at_url(url, time: np.datetime64, bands, **kwargs):
        url = urlparse(url)

        if url.scheme in ['file', '']:
            tiff = rioxarray.open_rasterio(url.path, mask_and_scale=True)
        else:
            raise NotImplementedError(f'Support not yet added for tiffs with {url.scheme} URLs')

        try:
            tiff.rio.reproject(dst_crs='EPSG:4326', nodata=np.nan)
        except MissingCRS:
            tiff.rio.write_crs('EPSG:4326').rio.reproject(dst_crs='EPSG:4326', nodata=np.nan)

        rename = dict(x='longitude', y='latitude')

        drop = set(tiff.data_vars)

        for band in bands:
            band_num = bands[band]

            key = f'band_{band_num}'

            rename[key] = band
            drop.discard(key)

        drop.discard('spatial_ref')

        tiff = tiff.rename(rename).drop_vars(drop, errors='ignore')

        tiff = tiff.expand_dims({"time": 1}).assign_coords({"time": [time]})

        return tiff

    def __fetch_data_for_tile(self, tile: Tile):
        bbox: BBox = tile.bbox

        min_lat = None
        min_lon = None
        max_lat = None
        max_lon = None

        if tile.min_time:
            min_time = tile.min_time
        else:
            min_time = None

        if tile.max_time:
            max_time = tile.max_time
        else:
            max_time = None

        # if min_time:
        #     min_time = datetime.utcfromtimestamp(min_time)
        #
        # if max_time:
        #     max_time = datetime.utcfromtimestamp(max_time)

        if bbox:
            min_lat = bbox.min_lat
            min_lon = bbox.min_lon
            max_lat = bbox.max_lat
            max_lon = bbox.max_lon

        granule = tile.granule

        ds: xr.Dataset = CoGBackend.__open_granule_at_url(granule, np.datetime64(min_time.isoformat()), self.__bands)
        variables = list(ds.data_vars)

        sel_g = {
            self.__latitude: slice(min_lat, max_lat),
            self.__longitude: slice(min_lon, max_lon),
        }

        sel_t = {}

        if min_time is None and max_time is None:
            sel_t = None
            method = None
        elif min_time == max_time:
            sel_t[self.__time] = [min_time]  # List, otherwise self.__time dim will be dropped
            method = 'nearest'
        else:
            sel_t[self.__time] = slice(min_time, max_time)
            method = None

        tile.variables = [
            TileVariable(v, v) for v in variables
        ]

        matched = self.__ds.sel(sel_g)

        if sel_t is not None:
            matched = matched.sel(sel_t, method=method)

        tile.latitudes = ma.masked_invalid(matched[self.__latitude].to_numpy())
        tile.longitudes = ma.masked_invalid(matched[self.__longitude].to_numpy())

        times = matched[self.__time].to_numpy()

        if np.issubdtype(times.dtype, np.datetime64):
            times = ((times - np.datetime64(EPOCH)) / 1e9).astype(int)

        tile.times = ma.masked_invalid(times)

        var_data = [matched[var].to_numpy() for var in variables]

        if len(variables) > 1:
            tile.data = ma.masked_invalid(var_data)
            tile.is_multi = True
        else:
            tile.data = ma.masked_invalid(var_data[0])
            tile.is_multi = False

    def _metadata_store_docs_to_tiles(self, *store_docs):
        return [CoGBackend.__nts_url_to_tile(d) for d in store_docs]

    @staticmethod
    def __nts_url_to_tile(nts_url):
        tile = Tile()

        url = URL(nts_url)

        tile.tile_id = nts_url

        try:
            min_lat = float(url.query['min_lat'])
            min_lon = float(url.query['min_lon'])
            max_lat = float(url.query['max_lat'])
            max_lon = float(url.query['max_lon'])

            tile.bbox = BBox(min_lat, max_lat, min_lon, max_lon)
        except KeyError:
            pass

        tile.dataset = url.path
        tile.dataset_id = url.path
        tile.granule = url.query['path']

        try:
            # tile.min_time = int(url.query['min_time'])
            tile.min_time = datetime.utcfromtimestamp(int(url.query['min_time']))
        except KeyError:
            pass

        try:
            # tile.max_time = int(url.query['max_time'])
            tile.max_time = datetime.utcfromtimestamp(int(url.query['max_time']))
        except KeyError:
            pass

        tile.meta_data = {}

        return tile

    @staticmethod
    def __to_url(dataset, tiff, **kwargs):
        if 'dataset' in kwargs:
            del kwargs['dataset']

        if 'ds' in kwargs:
            del kwargs['ds']

        if 'path' in kwargs:
            del kwargs['path']

        params = {}

        # If any params are numpy dtypes, extract them to base python types
        for kw in kwargs:
            v = kwargs[kw]

            if v is None:
                continue

            if isinstance(v, np.generic):
                v = v.item()

            params[kw] = v

        params['path'] = tiff

        return str(URL.build(
            scheme='cog',
            host='',
            path=dataset,
            query=params
        ))
