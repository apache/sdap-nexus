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

import configparser
import logging
import sys
import json
from abc import ABC, abstractmethod
from datetime import datetime
from functools import reduce

import numpy as np
import numpy.ma as ma
import pkg_resources
from pytz import timezone, UTC
from shapely.geometry import MultiPolygon, box

from .dao import CassandraProxy
from .dao import DynamoProxy
from .dao import S3Proxy
from .dao import SolrProxy
from .dao import ElasticsearchProxy

from nexustiles.model.nexusmodel import Tile, BBox, TileStats, TileVariable
from nexustiles.nexustiles import NexusTileServiceException

class AbstractTileService(ABC):
    @abstractmethod
    def get_dataseries_list(self, simple=False):
        raise NotImplementedError()

    @abstractmethod
    def find_tile_by_id(self, tile_id, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, dataset, start_time, end_time,
                               metrics_callback=None, **kwargs):
        raise NotImplementedError()

    @abstractmethod
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

    @abstractmethod
    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, dataset, time, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def find_tiles_in_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall in the given box in the Solr index
        raise NotImplementedError()

    @abstractmethod
    def find_tiles_in_polygon(self, bounding_polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall within the polygon in the Solr index
        raise NotImplementedError()

    @abstractmethod
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

    @abstractmethod
    def get_tiles_by_metadata(self, metadata, ds=None, start_time=0, end_time=-1, **kwargs):
        """
        Return list of tiles that matches the specified metadata, start_time, end_time with tile data outside of time
        range properly masked out.
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of tiles
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_tiles_bounded_by_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1,
                                 **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_tiles_bounded_by_polygon(self, polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_min_max_time_by_granule(self, ds, granule_name):
        raise NotImplementedError()

    @abstractmethod
    def get_dataset_overall_stats(self, ds):
        raise NotImplementedError()

    @abstractmethod
    def get_tiles_bounded_by_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_tiles_bounded_by_polygon_at_time(self, polygon, dataset, time, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_stats_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_bounding_box(self, tile_ids):
        """
        Retrieve a bounding box that encompasses all of the tiles represented by the given tile ids.
        :param tile_ids: List of tile ids
        :return: shapely.geometry.Polygon that represents the smallest bounding box that encompasses all of the tiles
        """
        raise NotImplementedError()

    @abstractmethod
    def get_min_time(self, tile_ids, ds=None):
        """
        Get the minimum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        raise NotImplementedError()

    @abstractmethod
    def get_max_time(self, tile_ids, ds=None):
        """
        Get the maximum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        raise NotImplementedError()

    @abstractmethod
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

    def mask_tiles_to_bbox(self, min_lat, max_lat, min_lon, max_lon, tiles):
        for tile in tiles:
            tile.latitudes = ma.masked_outside(tile.latitudes, min_lat, max_lat)
            tile.longitudes = ma.masked_outside(tile.longitudes, min_lon, max_lon)

            # Or together the masks of the individual arrays to create the new mask
            data_mask = ma.getmaskarray(tile.times)[:, np.newaxis, np.newaxis] \
                        | ma.getmaskarray(tile.latitudes)[np.newaxis, :, np.newaxis] \
                        | ma.getmaskarray(tile.longitudes)[np.newaxis, np.newaxis, :]

            # If this is multi-var, need to mask each variable separately.
            if tile.is_multi:
                # Combine space/time mask with existing mask on data
                data_mask = reduce(np.logical_or, [tile.data[0].mask, data_mask])

                num_vars = len(tile.data)
                multi_data_mask = np.repeat(data_mask[np.newaxis, ...], num_vars, axis=0)
                tile.data = ma.masked_where(multi_data_mask, tile.data)
            else:
                tile.data = ma.masked_where(data_mask, tile.data)

        tiles[:] = [tile for tile in tiles if not tile.data.mask.all()]

        return tiles

    def mask_tiles_to_bbox_and_time(self, min_lat, max_lat, min_lon, max_lon, start_time, end_time, tiles):
        for tile in tiles:
            tile.times = ma.masked_outside(tile.times, start_time, end_time)
            tile.latitudes = ma.masked_outside(tile.latitudes, min_lat, max_lat)
            tile.longitudes = ma.masked_outside(tile.longitudes, min_lon, max_lon)

            # Or together the masks of the individual arrays to create the new mask
            data_mask = ma.getmaskarray(tile.times)[:, np.newaxis, np.newaxis] \
                        | ma.getmaskarray(tile.latitudes)[np.newaxis, :, np.newaxis] \
                        | ma.getmaskarray(tile.longitudes)[np.newaxis, np.newaxis, :]

            tile.data = ma.masked_where(data_mask, tile.data)

        tiles[:] = [tile for tile in tiles if not tile.data.mask.all()]

        return tiles

    def mask_tiles_to_polygon(self, bounding_polygon, tiles):

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        return self.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, tiles)

    def mask_tiles_to_polygon_and_time(self, bounding_polygon, start_time, end_time, tiles):
        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds

        return self.mask_tiles_to_bbox_and_time(min_lat, max_lat, min_lon, max_lon, start_time, end_time, tiles)

    def mask_tiles_to_time_range(self, start_time, end_time, tiles):
        """
        Masks data in tiles to specified time range.
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :param tiles: List of tiles
        :return: A list tiles with data masked to specified time range
        """
        if 0 <= start_time <= end_time:
            for tile in tiles:
                tile.times = ma.masked_outside(tile.times, start_time, end_time)

                # Or together the masks of the individual arrays to create the new mask
                data_mask = ma.getmaskarray(tile.times)[:, np.newaxis, np.newaxis] \
                            | ma.getmaskarray(tile.latitudes)[np.newaxis, :, np.newaxis] \
                            | ma.getmaskarray(tile.longitudes)[np.newaxis, np.newaxis, :]

                # If this is multi-var, need to mask each variable separately.
                if tile.is_multi:
                    # Combine space/time mask with existing mask on data
                    data_mask = reduce(np.logical_or, [tile.data[0].mask, data_mask])

                    num_vars = len(tile.data)
                    multi_data_mask = np.repeat(data_mask[np.newaxis, ...], num_vars, axis=0)
                    tile.data = ma.masked_where(multi_data_mask, tile.data)
                else:
                    tile.data = ma.masked_where(data_mask, tile.data)

            tiles[:] = [tile for tile in tiles if not tile.data.mask.all()]

        return tiles

    @abstractmethod
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

    @abstractmethod
    def fetch_data_for_tiles(self, *tiles):
        raise NotImplementedError()

    @abstractmethod
    def open_dataset(self, dataset):
        raise NotImplementedError()

    @abstractmethod
    def _metadata_store_docs_to_tiles(self, *store_docs):
        raise NotImplementedError()

