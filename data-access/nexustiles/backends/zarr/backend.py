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
import s3fs
import xarray as xr
from nexustiles.AbstractTileService import AbstractTileService
from nexustiles.exception import NexusTileServiceException
from nexustiles.model.nexusmodel import Tile, BBox, TileVariable
from pytz import timezone
from shapely.geometry import MultiPolygon, box
from yarl import URL

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger(__name__)


class ZarrBackend(AbstractTileService):
    def __init__(self, dataset_name, path, config=None):
        AbstractTileService.__init__(self, dataset_name)
        config = config if config is not None else {}
        self.__config = config

        logger.info(f'Opening zarr backend at {path} for dataset {self._name}')

        url = urlparse(path)

        self.__url = path

        self.__store_type = url.scheme
        self.__host = url.netloc
        self.__path = url.path

        if 'variable' in config:
            data_vars = config['variable']
        elif 'variables' in config:
            data_vars = config['variables']
        else:
            raise KeyError('Data variables not provided in config')

        if isinstance(data_vars, str):
            self.__variables = [data_vars]
        elif isinstance(data_vars, list):
            self.__variables = data_vars
        else:
            raise TypeError(f'Improper type for variables config: {type(data_vars)}')

        self.__variables = [v.strip('\"\'') for v in self.__variables]

        self.__longitude = config['coords']['longitude']
        self.__latitude = config['coords']['latitude']
        self.__time = config['coords']['time']

        self.__depth = config['coords'].get('depth')

        if self.__store_type in ['', 'file']:
            store = self.__path
        elif self.__store_type == 's3':
            try:
                aws_cfg = self.__config['aws']

                if aws_cfg['public']:
                    # region = aws_cfg.get('region', 'us-west-2')
                    # store = f'https://{self.__host}.s3.{region}.amazonaws.com{self.__path}'
                    s3 = s3fs.S3FileSystem(True)
                    store = s3fs.S3Map(root=path, s3=s3, check=False)
                else:
                    s3 = s3fs.S3FileSystem(False, key=aws_cfg['accessKeyID'], secret=aws_cfg['secretAccessKey'])
                    store = s3fs.S3Map(root=path, s3=s3, check=False)
            except Exception as e:
                logger.error(f'Failed to open zarr dataset at {self.__path}, ignoring it. Cause: {e}')
                raise NexusTileServiceException(f'Cannot open S3 dataset ({e})')
        else:
            raise ValueError(self.__store_type)

        try:
            self.__ds: xr.Dataset = xr.open_zarr(store, consolidated=True)
        except Exception as e:
            logger.error(f'Failed to open zarr dataset at {self.__path}, ignoring it. Cause: {e}')
            raise NexusTileServiceException(f'Cannot open dataset ({e})')

        lats = self.__ds[self.__latitude].to_numpy()
        delta = lats[1] - lats[0]

        if delta < 0:
            logger.warning(f'Latitude coordinate for {self._name} is in descending order. Flipping it to ascending')
            self.__ds = self.__ds.isel({self.__latitude: slice(None, None, -1)})

    def get_dataseries_list(self, simple=False):
        ds = {
            "shortName": self._name,
            "title": self._name,
            "type": "zarr"
        }

        if not simple:
            min_date = self.get_min_time([])
            max_date = self.get_max_time([])
            ds['start'] = min_date
            ds['end'] = max_date
            ds['iso_start'] = datetime.utcfromtimestamp(min_date).strftime(ISO_8601)
            ds['iso_end'] = datetime.utcfromtimestamp(max_date).strftime(ISO_8601)

            ds['metadata'] = dict(self.__ds.attrs)

        return [ds]

    def find_tile_by_id(self, tile_id, **kwargs):
        return [tile_id]

    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):
        return tile_ids

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, dataset, start_time, end_time,
                               metrics_callback=None, **kwargs):
        start = datetime.now()

        if not isinstance(start_time, datetime):
            start_time = datetime.utcfromtimestamp(start_time)

        if not isinstance(end_time, datetime):
            end_time = datetime.utcfromtimestamp(end_time)

        sel = {
            self.__latitude: slice(min_lat, max_lat),
            self.__longitude: slice(min_lon, max_lon),
            self.__time: slice(start_time, end_time)
        }

        times = self.__ds.sel(sel)[self.__time].to_numpy()

        if np.issubdtype(times.dtype, np.datetime64):
            times = (times - np.datetime64(EPOCH)).astype('timedelta64[s]').astype(int)

        times = sorted(times.tolist())

        if metrics_callback:
            metrics_callback(backend=(datetime.now() - start).total_seconds())

        return times

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

        times = self.__ds[self.__time].to_numpy()

        to_doy = lambda dt: datetime.utcfromtimestamp(int(dt)).timetuple().tm_yday

        vfunc = np.vectorize(to_doy)
        days_of_year = vfunc(times.astype(datetime) / 1e9)

        try:
            time = times[np.where(days_of_year <= day_of_year)[0][-1]].astype(datetime) / 1e9
        except IndexError:
            raise NexusTileServiceException(reason='No tiles matched')

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        return self.find_tiles_in_box(
            min_lat, max_lat, min_lon, max_lon, ds, time, time
        )

    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return self.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon, dataset, time, time, **kwargs)

    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, dataset, time, **kwargs):
        return self.find_tiles_in_polygon(bounding_polygon, dataset, time, time, **kwargs)

    def find_tiles_in_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1, **kwargs):
        if type(start_time) is datetime:
            start_time = (start_time - EPOCH).total_seconds()
        if type(end_time) is datetime:
            end_time = (end_time - EPOCH).total_seconds()

        params = {
            'min_lat': min_lat,
            'max_lat': max_lat,
            'min_lon': min_lon,
            'max_lon': max_lon
        }

        times = None

        if 0 <= start_time <= end_time:
            if kwargs.get('distinct', True):
                times_asc = self.find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time)
                times = [(t, t) for t in times_asc]
            else:
                times = [(start_time, end_time)]

        if 'depth' in kwargs:
            params['depth'] = kwargs['depth']
        elif 'min_depth' in kwargs or 'max_depth' in kwargs:
            params['min_depth'] = kwargs.get('min_depth')
            params['max_depth'] = kwargs.get('max_depth')

        if times:
            return [ZarrBackend.__to_url(self._name, min_time=t[0], max_time=t[1], **params) for t in times]
        else:
            return [ZarrBackend.__to_url(self._name, **params)]

    def find_tiles_in_polygon(self, bounding_polygon, ds=None, start_time=None, end_time=None, **kwargs):
        # Find tiles that fall within the polygon in the Solr index
        bounds = bounding_polygon.bounds

        min_lon = bounds[0]
        min_lat = bounds[1]
        max_lon = bounds[2]
        max_lat = bounds[3]

        return self.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs)

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
        return []

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

        bounds = [
            (
                float(URL(u).query['min_lon']),
                float(URL(u).query['min_lat']),
                float(URL(u).query['max_lon']),
                float(URL(u).query['max_lat'])
            )
            for u in tile_ids
        ]

        poly = MultiPolygon([box(*b) for b in bounds])

        return box(*poly.bounds)

    def __get_ds_min_max_date(self):
        min_date = self.__ds[self.__time].min().to_numpy()
        max_date = self.__ds[self.__time].max().to_numpy()

        if np.issubdtype(min_date.dtype, np.datetime64):
            min_date = (min_date - np.datetime64(EPOCH)).astype('timedelta64[s]').astype(int).item()

        if np.issubdtype(max_date.dtype, np.datetime64):
            max_date = (max_date - np.datetime64(EPOCH)).astype('timedelta64[s]').astype(int).item()

        return min_date, max_date

    def get_min_time(self, tile_ids, ds=None):
        """
        Get the minimum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        times = list(filter(lambda x: x is not None, [int(URL(tid).query['min_time']) for tid in tile_ids]))

        if len(times) == 0:
            min_date, max_date = self.__get_ds_min_max_date()
            return min_date
        else:
            return min(times)

    def get_max_time(self, tile_ids, ds=None):
        """
        Get the maximum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        times = list(filter(lambda x: x is not None, [int(URL(tid).query['max_time']) for tid in tile_ids]))

        if len(tile_ids) == 0:
            min_date, max_date = self.__get_ds_min_max_date()
            return max_date
        else:
            return max(times)

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
        raise NotImplementedError()

    def fetch_data_for_tiles(self, *tiles):
        for tile in tiles:
            self.__fetch_data_for_tile(tile)

        return tiles

    def __fetch_data_for_tile(self, tile: Tile):
        bbox: BBox = tile.bbox

        min_lat = None
        min_lon = None
        max_lat = None
        max_lon = None

        min_time = tile.min_time
        max_time = tile.max_time

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
            TileVariable(v, v) for v in self.__variables
        ]

        matched = self.__ds.sel(sel_g)

        if sel_t is not None:
            matched = matched.sel(sel_t, method=method)

        tile.latitudes = ma.masked_invalid(matched[self.__latitude].to_numpy())
        tile.longitudes = ma.masked_invalid(matched[self.__longitude].to_numpy())

        times = matched[self.__time].to_numpy()

        if np.issubdtype(times.dtype, np.datetime64):
            times = (times - np.datetime64(EPOCH)).astype('timedelta64[s]').astype(int)

        tile.times = ma.masked_invalid(times)

        var_data = [matched[var].to_numpy() for var in self.__variables]

        if len(self.__variables) > 1:
            tile.data = ma.masked_invalid(var_data)
            tile.is_multi = True
        else:
            tile.data = ma.masked_invalid(var_data[0])
            tile.is_multi = False

    def _metadata_store_docs_to_tiles(self, *store_docs):
        return [ZarrBackend.__nts_url_to_tile(d) for d in store_docs]

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
    def __to_url(dataset, **kwargs):
        if 'dataset' in kwargs:
            del kwargs['dataset']

        if 'ds' in kwargs:
            del kwargs['ds']

        params = {}

        # If any params are numpy dtypes, extract them to base python types
        for kw in kwargs:
            v = kwargs[kw]

            if v is None:
                continue

            if isinstance(v, np.generic):
                v = v.item()

            params[kw] = v

        return str(URL.build(
            scheme='nts',
            host='',
            path=dataset,
            query=params
        ))


