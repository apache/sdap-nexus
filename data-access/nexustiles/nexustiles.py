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

import ConfigParser
import logging
import sys
from datetime import datetime
from functools import wraps

import numpy as np
import numpy.ma as ma
import pkg_resources
from pytz import timezone, UTC
from shapely.geometry import MultiPolygon, box

import dao.CassandraProxy
import dao.DynamoProxy
import dao.S3Proxy
import dao.SolrProxy
from model.nexusmodel import Tile, BBox, TileStats

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger("testing")


def tile_data(default_fetch=True):
    def tile_data_decorator(func):
        @wraps(func)
        def fetch_data_for_func(*args, **kwargs):
            solr_start = datetime.now()
            solr_docs = func(*args, **kwargs)
            solr_duration = (datetime.now() - solr_start).total_seconds()
            tiles = args[0]._solr_docs_to_tiles(*solr_docs)

            cassandra_duration = 0
            if ('fetch_data' in kwargs and kwargs['fetch_data']) or ('fetch_data' not in kwargs and default_fetch):
                if len(tiles) > 0:
                    cassandra_start = datetime.now()
                    args[0].fetch_data_for_tiles(*tiles)
                    cassandra_duration += (datetime.now() - cassandra_start).total_seconds()

            if 'metrics_callback' in kwargs and kwargs['metrics_callback'] is not None:
                try:
                    kwargs['metrics_callback'](cassandra=cassandra_duration,
                                               solr=solr_duration,
                                               num_tiles=len(tiles))
                except Exception as e:
                    logger.error("Metrics callback '{}'raised an exception. Will continue anyway. " +
                                 "The exception was: {}".format(kwargs['metrics_callback'], e))
            return tiles

        return fetch_data_for_func

    return tile_data_decorator


class NexusTileServiceException(Exception):
    pass


class NexusTileService(object):
    def __init__(self, skipDatastore=False, skipMetadatastore=False, config=None):
        self._datastore = None
        self._metadatastore = None

        self._config = ConfigParser.RawConfigParser()
        self._config.read(NexusTileService._get_config_files('config/datastores.ini'))

        if config:
            self.override_config(config)

        if not skipDatastore:
            datastore = self._config.get("datastore", "store")
            if datastore == "cassandra":
                self._datastore = dao.CassandraProxy.CassandraProxy(self._config)
            elif datastore == "s3":
                self._datastore = dao.S3Proxy.S3Proxy(self._config)
            elif datastore == "dynamo":
                self._datastore = dao.DynamoProxy.DynamoProxy(self._config)
            else:
                raise ValueError("Error reading datastore from config file")

        if not skipMetadatastore:
            self._metadatastore = dao.SolrProxy.SolrProxy(self._config)

    def override_config(self, config):
        for section in config.sections():
            if self._config.has_section(section):  # only override preexisting section, ignores the other
                for option in config.options(section):
                    if config.get(section, option) is not None:
                        self._config.set(section, option, config.get(section, option))

    def get_dataseries_list(self, simple=False):
        if simple:
            return self._metadatastore.get_data_series_list_simple()
        else:
            return self._metadatastore.get_data_series_list()

    @tile_data()
    def find_tile_by_id(self, tile_id, **kwargs):
        return self._metadatastore.find_tile_by_id(tile_id)

    @tile_data()
    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):
        return self._metadatastore.find_tiles_by_id(tile_ids, ds=ds, **kwargs)

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, dataset, start_time, end_time,
                               metrics_callback=None, **kwargs):
        start = datetime.now()
        result = self._metadatastore.find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, dataset, start_time,
                                                            end_time,
                                                            **kwargs)
        duration = (datetime.now() - start).total_seconds()
        if metrics_callback:
            metrics_callback(solr=duration)
        return result

    @tile_data()
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
        try:
            tile = self._metadatastore.find_tile_by_polygon_and_most_recent_day_of_year(bounding_polygon, ds,
                                                                                        day_of_year)
        except IndexError:
            raise NexusTileServiceException("No tile found."), None, sys.exc_info()[2]

        return tile

    @tile_data()
    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return self._metadatastore.find_all_tiles_in_box_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time,
                                                                 rows=5000,
                                                                 **kwargs)

    @tile_data()
    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, dataset, time, **kwargs):
        return self._metadatastore.find_all_tiles_in_polygon_at_time(bounding_polygon, dataset, time, rows=5000,
                                                                     **kwargs)

    @tile_data()
    def find_tiles_in_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall in the given box in the Solr index
        if type(start_time) is datetime:
            start_time = (start_time - EPOCH).total_seconds()
        if type(end_time) is datetime:
            end_time = (end_time - EPOCH).total_seconds()
        return self._metadatastore.find_all_tiles_in_box_sorttimeasc(min_lat, max_lat, min_lon, max_lon, ds, start_time,
                                                                     end_time, **kwargs)

    @tile_data()
    def find_tiles_in_polygon(self, bounding_polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall within the polygon in the Solr index
        if 'sort' in kwargs.keys():
            tiles = self._metadatastore.find_all_tiles_in_polygon(bounding_polygon, ds, start_time, end_time, **kwargs)
        else:
            tiles = self._metadatastore.find_all_tiles_in_polygon_sorttimeasc(bounding_polygon, ds, start_time,
                                                                              end_time,
                                                                              **kwargs)
        return tiles

    @tile_data()
    def find_tiles_by_metadata(self, metadata, ds=None, start_time=0, end_time=-1, **kwargs):
        """
        Return list of tiles whose metadata matches the specified metadata, start_time, end_time.
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of tiles
        """
        tiles = self._metadatastore.find_all_tiles_by_metadata(metadata, ds, start_time, end_time, **kwargs)

        return tiles

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
        tiles = self.find_tiles_by_metadata(metadata, ds, start_time, end_time, **kwargs)
        tiles = self.mask_tiles_to_time_range(start_time, end_time, tiles)

        return tiles

    @tile_data()
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
        tiles = self._metadatastore.find_tiles_by_exact_bounds(bounds[0], bounds[1], bounds[2], bounds[3], ds,
                                                               start_time,
                                                               end_time)
        return tiles

    @tile_data()
    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return self._metadatastore.find_all_boundary_tiles_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time,
                                                                   rows=5000,
                                                                   **kwargs)

    def get_tiles_bounded_by_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1,
                                 **kwargs):
        tiles = self.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs)
        tiles = self.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, tiles)
        if 0 <= start_time <= end_time:
            tiles = self.mask_tiles_to_time_range(start_time, end_time, tiles)

        return tiles

    def get_tiles_bounded_by_polygon(self, polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        tiles = self.find_tiles_in_polygon(polygon, ds, start_time, end_time,
                                           **kwargs)
        tiles = self.mask_tiles_to_polygon(polygon, tiles)
        if 0 <= start_time <= end_time:
            tiles = self.mask_tiles_to_time_range(start_time, end_time, tiles)

        return tiles

    def get_min_max_time_by_granule(self, ds, granule_name):
        start_time, end_time = self._solr.find_min_max_date_from_granule(ds, granule_name)

        return start_time, end_time

    def get_dataset_overall_stats(self, ds):
        return self._solr.get_data_series_stats(ds)

    def get_tiles_bounded_by_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        tiles = self.find_all_tiles_in_box_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs)
        tiles = self.mask_tiles_to_bbox_and_time(min_lat, max_lat, min_lon, max_lon, time, time, tiles)

        return tiles

    def get_tiles_bounded_by_polygon_at_time(self, polygon, dataset, time, **kwargs):
        tiles = self.find_all_tiles_in_polygon_at_time(polygon, dataset, time, **kwargs)
        tiles = self.mask_tiles_to_polygon_and_time(polygon, time, time, tiles)

        return tiles

    def get_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        tiles = self.find_all_boundary_tiles_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs)
        tiles = self.mask_tiles_to_bbox_and_time(min_lat, max_lat, min_lon, max_lon, time, time, tiles)

        return tiles

    def get_stats_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        tiles = self._metadatastore.find_all_tiles_within_box_at_time(min_lat, max_lat, min_lon, max_lon, dataset, time,
                                                                      **kwargs)

        return tiles

    def get_bounding_box(self, tile_ids):
        """
        Retrieve a bounding box that encompasses all of the tiles represented by the given tile ids.
        :param tile_ids: List of tile ids
        :return: shapely.geometry.Polygon that represents the smallest bounding box that encompasses all of the tiles
        """
        tiles = self.find_tiles_by_id(tile_ids, fl=['tile_min_lat', 'tile_max_lat', 'tile_min_lon', 'tile_max_lon'],
                                      fetch_data=False, rows=len(tile_ids))
        polys = []
        for tile in tiles:
            polys.append(box(tile.bbox.min_lon, tile.bbox.min_lat, tile.bbox.max_lon, tile.bbox.max_lat))
        return box(*MultiPolygon(polys).bounds)

    def get_min_time(self, tile_ids, ds=None):
        """
        Get the minimum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        min_time = self._metadatastore.find_min_date_from_tiles(tile_ids, ds=ds)
        return long((min_time - EPOCH).total_seconds())

    def get_max_time(self, tile_ids, ds=None):
        """
        Get the maximum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        max_time = self._metadatastore.find_max_date_from_tiles(tile_ids, ds=ds)
        return long((max_time - EPOCH).total_seconds())

    def get_distinct_bounding_boxes_in_polygon(self, bounding_polygon, ds, start_time, end_time):
        """
        Get a list of distinct tile bounding boxes from all tiles within the given polygon and time range.
        :param bounding_polygon: The bounding polygon of tiles to search for
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of distinct bounding boxes (as shapely polygons) for tiles in the search polygon
        """
        bounds = self._metadatastore.find_distinct_bounding_boxes_in_polygon(bounding_polygon, ds, start_time, end_time)
        return [box(*b) for b in bounds]

    def mask_tiles_to_bbox(self, min_lat, max_lat, min_lon, max_lon, tiles):

        for tile in tiles:
            tile.latitudes = ma.masked_outside(tile.latitudes, min_lat, max_lat)
            tile.longitudes = ma.masked_outside(tile.longitudes, min_lon, max_lon)

            # Or together the masks of the individual arrays to create the new mask
            data_mask = ma.getmaskarray(tile.times)[:, np.newaxis, np.newaxis] \
                        | ma.getmaskarray(tile.latitudes)[np.newaxis, :, np.newaxis] \
                        | ma.getmaskarray(tile.longitudes)[np.newaxis, np.newaxis, :]

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

                tile.data = ma.masked_where(data_mask, tile.data)

            tiles[:] = [tile for tile in tiles if not tile.data.mask.all()]

        return tiles

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
        return self._metadatastore.get_tile_count(ds, bounding_polygon, start_time, end_time, metadata, **kwargs)

    def fetch_data_for_tiles(self, *tiles):

        nexus_tile_ids = set([tile.tile_id for tile in tiles])
        matched_tile_data = self._datastore.fetch_nexus_tiles(*nexus_tile_ids)

        tile_data_by_id = {str(a_tile_data.tile_id): a_tile_data for a_tile_data in matched_tile_data}

        missing_data = nexus_tile_ids.difference(tile_data_by_id.keys())
        if len(missing_data) > 0:
            raise StandardError("Missing data for tile_id(s) %s." % missing_data)

        for a_tile in tiles:
            lats, lons, times, data, meta = tile_data_by_id[a_tile.tile_id].get_lat_lon_time_data_meta()

            a_tile.latitudes = lats
            a_tile.longitudes = lons
            a_tile.times = times
            a_tile.data = data
            a_tile.meta_data = meta

            del (tile_data_by_id[a_tile.tile_id])

        return tiles

    def _solr_docs_to_tiles(self, *solr_docs):

        tiles = []
        for solr_doc in solr_docs:
            tile = Tile()
            try:
                tile.tile_id = solr_doc['id']
            except KeyError:
                pass

            try:
                tile.bbox = BBox(
                    solr_doc['tile_min_lat'], solr_doc['tile_max_lat'],
                    solr_doc['tile_min_lon'], solr_doc['tile_max_lon'])
            except KeyError:
                pass

            try:
                tile.dataset = solr_doc['dataset_s']
            except KeyError:
                pass

            try:
                tile.dataset_id = solr_doc['dataset_id_s']
            except KeyError:
                pass

            try:
                tile.granule = solr_doc['granule_s']
            except KeyError:
                pass

            try:
                tile.min_time = datetime.strptime(solr_doc['tile_min_time_dt'], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=UTC)
            except KeyError:
                pass

            try:
                tile.max_time = datetime.strptime(solr_doc['tile_max_time_dt'], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=UTC)
            except KeyError:
                pass

            try:
                tile.section_spec = solr_doc['sectionSpec_s']
            except KeyError:
                pass

            try:
                tile.tile_stats = TileStats(
                    solr_doc['tile_min_val_d'], solr_doc['tile_max_val_d'],
                    solr_doc['tile_avg_val_d'], solr_doc['tile_count_i']
                )
            except KeyError:
                pass

            tiles.append(tile)

        return tiles

    def pingSolr(self):
        status = self._metadatastore.ping()
        if status and status["status"] == "OK":
            return True
        else:
            return False

    @staticmethod
    def _get_config_files(filename):
        log = logging.getLogger(__name__)
        candidates = []
        extensions = ['.default', '']
        for extension in extensions:
            try:
                candidate = pkg_resources.resource_filename(__name__, filename + extension)
                log.info('use config file {}'.format(filename + extension))
                candidates.append(candidate)
            except KeyError as ke:
                log.warning('configuration file {} not found'.format(filename + extension))

        return candidates
