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
import json
import logging
import sys
import threading
from datetime import datetime
from functools import reduce, wraps
from time import sleep
from typing import Dict, Union

import numpy as np
import numpy.ma as ma
import pkg_resources
import pysolr
from pytz import timezone, UTC
from shapely.geometry import box
from webservice.webmodel import DatasetNotFoundException, NexusProcessingException

from .AbstractTileService import AbstractTileService
from .backends.nexusproto.backend import NexusprotoTileService
from .backends.zarr.backend import ZarrBackend
from .model.nexusmodel import Tile, BBox, TileStats, TileVariable

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger("nexus-tile-svc")


def tile_data(default_fetch=True):
    def tile_data_decorator(func):
        @wraps(func)
        def fetch_data_for_func(*args, **kwargs):
            metadatastore_start = datetime.now()
            metadatastore_docs = func(*args, **kwargs)
            metadatastore_duration = (datetime.now() - metadatastore_start).total_seconds()
            tiles = args[0]._metadata_store_docs_to_tiles(*metadatastore_docs)

            cassandra_duration = 0
            if ('fetch_data' in kwargs and kwargs['fetch_data']) or ('fetch_data' not in kwargs and default_fetch):
                if len(tiles) > 0:
                    cassandra_start = datetime.now()
                    args[0].fetch_data_for_tiles(*tiles)
                    cassandra_duration += (datetime.now() - cassandra_start).total_seconds()

            if 'metrics_callback' in kwargs and kwargs['metrics_callback'] is not None:
                try:
                    kwargs['metrics_callback'](cassandra=cassandra_duration,
                                               metadatastore=metadatastore_duration,
                                               num_tiles=len(tiles))
                except Exception as e:
                    logger.error("Metrics callback '{}'raised an exception. Will continue anyway. " +
                                 "The exception was: {}".format(kwargs['metrics_callback'], e))
            return tiles

        return fetch_data_for_func

    return tile_data_decorator


SOLR_LOCK = threading.Lock()
DS_LOCK = threading.Lock()
thread_local = threading.local()


class NexusTileService:
    backends: Dict[Union[None, str], Dict[str, Union[AbstractTileService, bool]]] = {}

    ds_config = None

    __update_thread = None

    @staticmethod
    def __update_datasets():
        while True:
            with DS_LOCK:
                NexusTileService._update_datasets()
            sleep(3600)

    def __init__(self, config=None):
        self._config = configparser.RawConfigParser()
        self._config.read(NexusTileService._get_config_files('config/datasets.ini'))

        self._alg_config = config

        if config:
            self.override_config(config)

        if not NexusTileService.backends:
            NexusTileService.ds_config = configparser.RawConfigParser()
            NexusTileService.ds_config.read(NexusTileService._get_config_files('config/datasets.ini'))

            default_backend = {"backend": NexusprotoTileService(False, False, config), 'up': True}

            NexusTileService.backends[None] = default_backend
            NexusTileService.backends['__nexusproto__'] = default_backend

        if not NexusTileService.__update_thread:
            NexusTileService.__update_thread = threading.Thread(
                target=NexusTileService.__update_datasets,
                name='dataset_update',
                daemon=False
            )

            logger.info('Starting dataset refresh thread')

            NexusTileService.__update_thread.start()

    @staticmethod
    def __get_backend(dataset_s) -> AbstractTileService:
        with DS_LOCK:
            if dataset_s not in NexusTileService.backends:
                raise DatasetNotFoundException(reason=f'Dataset {dataset_s} is not currently loaded/ingested')

            b = NexusTileService.backends[dataset_s]

            if not b['up']:
                success = b['backend'].try_connect()

                if not success:
                    raise NexusProcessingException(reason=f'Dataset {dataset_s} is currently unavailable')
                else:
                    NexusTileService.backends[dataset_s]['up'] = True

            return b['backend']

    @staticmethod
    def _update_datasets():
        solr_url = NexusTileService.ds_config.get("solr", "host")
        solr_core = NexusTileService.ds_config.get("solr", "core")
        solr_kwargs = {}

        update_logger = logging.getLogger("nexus-tile-svc.backends")

        if NexusTileService.ds_config.has_option("solr", "time_out"):
            solr_kwargs["timeout"] = NexusTileService.ds_config.get("solr", "time_out")

        with SOLR_LOCK:
            solrcon = getattr(thread_local, 'solrcon', None)
            if solrcon is None:
                solr_url = '%s/solr/%s' % (solr_url, solr_core)
                solrcon = pysolr.Solr(solr_url, **solr_kwargs)
                thread_local.solrcon = solrcon

            solrcon = solrcon

            update_logger.info('Executing update query to check for new datasets')

            present_datasets = {None, '__nexusproto__'}
            next_cursor_mark = '*'

            while True:
                response = solrcon.search('*:*', cursorMark=next_cursor_mark, sort='id asc')

                try:
                    response_cursor_mark = response.nextCursorMark
                except AttributeError:
                    break

                if response_cursor_mark == next_cursor_mark:
                    break
                else:
                    next_cursor_mark = response_cursor_mark

                for dataset in response.docs:
                    d_id = dataset['dataset_s']
                    store_type = dataset.get('store_type_s', 'nexusproto')

                    present_datasets.add(d_id)

                    if d_id in NexusTileService.backends:
                        continue
                        # is_up = NexusTileService.backends[d_id]['backend'].try_connect()

                    if store_type == 'nexus_proto' or store_type == 'nexusproto':
                        update_logger.info(f"Detected new nexusproto dataset {d_id}, using default nexusproto backend")
                        NexusTileService.backends[d_id] = NexusTileService.backends[None]
                    elif store_type == 'zarr':
                        update_logger.info(f"Detected new zarr dataset {d_id}, opening new zarr backend")

                        ds_config = json.loads(dataset['config'][0])
                        NexusTileService.backends[d_id] = {
                            'backend': ZarrBackend(ds_config),
                            'up': True
                        }
                    else:
                        logger.warning(f'Unsupported backend {store_type} for dataset {d_id}')

        removed_datasets = set(NexusTileService.backends.keys()).difference(present_datasets)

        if len(removed_datasets) > 0:
            logger.info(f'{len(removed_datasets)} marked for removal')

        for dataset in removed_datasets:
            logger.info(f"Removing dataset {dataset}")
            del NexusTileService.backends[dataset]

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
        return NexusTileService.__get_backend('__nexusproto__').find_tile_by_id(tile_id)

    @tile_data()
    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):
        return NexusTileService.__get_backend('__nexusproto__').find_tiles_by_id(tile_ids, ds=ds, **kwargs)

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, dataset, start_time, end_time,
                               metrics_callback=None, **kwargs):
        return NexusTileService.__get_backend(dataset).find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon,
                                                                              dataset, start_time, end_time,
                                                                              metrics_callback, **kwargs)

    @tile_data()
    def find_tile_by_polygon_and_most_recent_day_of_year(self, bounding_polygon, ds, day_of_year, **kwargs):
        return NexusTileService.__get_backend(ds).find_tile_by_polygon_and_most_recent_day_of_year(
            bounding_polygon, ds, day_of_year, **kwargs
        )

    @tile_data()
    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return NexusTileService.__get_backend(dataset).find_all_tiles_in_box_at_time(
            min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs
        )

    @tile_data()
    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, dataset, time, **kwargs):
        return NexusTileService.__get_backend(dataset).find_all_tiles_in_polygon_at_time(
            bounding_polygon, dataset, time, **kwargs
        )

    @tile_data()
    def find_tiles_in_box(self, min_lat, max_lat, min_lon, max_lon, ds=None, start_time=0, end_time=-1, **kwargs):
        # Find tiles that fall in the given box in the Solr index
        if type(start_time) is datetime:
            start_time = (start_time - EPOCH).total_seconds()
        if type(end_time) is datetime:
            end_time = (end_time - EPOCH).total_seconds()

        return NexusTileService.__get_backend(ds).find_tiles_in_box(
            min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs
        )

    @tile_data()
    def find_tiles_in_polygon(self, bounding_polygon, ds=None, start_time=0, end_time=-1, **kwargs):
        return NexusTileService.__get_backend(ds).find_tiles_in_polygon(
            bounding_polygon, ds, start_time, end_time, **kwargs
        )

    @tile_data()
    def find_tiles_by_metadata(self, metadata, ds=None, start_time=0, end_time=-1, **kwargs):
        return NexusTileService.__get_backend(ds).find_tiles_by_metadata(
            metadata, ds, start_time, end_time, **kwargs
        )

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
        return NexusTileService.__get_backend(ds).find_tiles_by_exact_bounds(
            bounds, ds, start_time, end_time, **kwargs
        )

    @tile_data()
    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs):
        return NexusTileService.__get_backend(dataset).find_all_boundary_tiles_at_time(
            min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs
        )

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
        return NexusTileService.__get_backend(ds).get_min_max_time_by_granule(
            ds, granule_name
        )

    def get_dataset_overall_stats(self, ds):
        return NexusTileService.__get_backend(ds).get_dataset_overall_stats(ds)

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
        return NexusTileService.get_stats_within_box_at_time(
            min_lat, max_lat, min_lon, max_lon, dataset, time, **kwargs
        )

    def get_bounding_box(self, tile_ids, ds=None):
        """
        Retrieve a bounding box that encompasses all of the tiles represented by the given tile ids.
        :param tile_ids: List of tile ids
        :return: shapely.geometry.Polygon that represents the smallest bounding box that encompasses all of the tiles
        """
        return NexusTileService.__get_backend(ds).get_bounding_box(tile_ids, ds)

    def get_min_time(self, tile_ids, ds=None):
        """
        Get the minimum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        return NexusTileService.__get_backend(ds).get_min_time(tile_ids, ds)

    def get_max_time(self, tile_ids, ds=None):
        """
        Get the maximum tile date from the list of tile ids
        :param tile_ids: List of tile ids
        :param ds: Filter by a specific dataset. Defaults to None (queries all datasets)
        :return: long time in seconds since epoch
        """
        return int(NexusTileService.__get_backend(ds).get_max_time(tile_ids))

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

    def fetch_data_for_tiles(self, *tiles, dataset=None):
        return NexusTileService.__get_backend(dataset).fetch_data_for_tiles(*tiles)

    def _metadata_store_docs_to_tiles(self, *store_docs):
        tiles = []
        for store_doc in store_docs:
            tile = Tile()
            try:
                tile.tile_id = store_doc['id']
            except KeyError:
                pass

            try:
                min_lat = store_doc['tile_min_lat']
                min_lon = store_doc['tile_min_lon']
                max_lat = store_doc['tile_max_lat']
                max_lon = store_doc['tile_max_lon']

                if isinstance(min_lat, list):
                    min_lat = min_lat[0]
                if isinstance(min_lon, list):
                    min_lon = min_lon[0]
                if isinstance(max_lat, list):
                    max_lat = max_lat[0]
                if isinstance(max_lon, list):
                    max_lon = max_lon[0]

                tile.bbox = BBox(min_lat, max_lat, min_lon, max_lon)
            except KeyError:
                pass

            try:
                tile.dataset = store_doc['dataset_s']
            except KeyError:
                pass

            try:
                tile.dataset_id = store_doc['dataset_id_s']
            except KeyError:
                pass

            try:
                tile.granule = store_doc['granule_s']
            except KeyError:
                pass

            try:
                tile.min_time = datetime.strptime(store_doc['tile_min_time_dt'], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=UTC)
            except KeyError:
                pass

            try:
                tile.max_time = datetime.strptime(store_doc['tile_max_time_dt'], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=UTC)
            except KeyError:
                pass

            try:
                tile.section_spec = store_doc['sectionSpec_s']
            except KeyError:
                pass

            try:
                tile.tile_stats = TileStats(
                    store_doc['tile_min_val_d'], store_doc['tile_max_val_d'],
                    store_doc['tile_avg_val_d'], store_doc['tile_count_i']
                )
            except KeyError:
                pass

            try:
                # Ensure backwards compatibility by working with old
                # tile_var_name_s and tile_standard_name_s fields to

                # will be overwritten if tile_var_name_ss is present
                # as well.
                if '[' in store_doc['tile_var_name_s']:
                    var_names = json.loads(store_doc['tile_var_name_s'])
                else:
                    var_names = [store_doc['tile_var_name_s']]

                standard_name = store_doc.get(
                        'tile_standard_name_s',
                        json.dumps([None] * len(var_names))
                )
                if '[' in standard_name:
                    standard_names = json.loads(standard_name)
                else:
                    standard_names = [standard_name]

                tile.variables = []
                for var_name, standard_name in zip(var_names, standard_names):
                    tile.variables.append(TileVariable(
                        variable_name=var_name,
                        standard_name=standard_name
                    ))
            except KeyError:
                pass

            if 'tile_var_name_ss' in store_doc:
                tile.variables = []
                for var_name in store_doc['tile_var_name_ss']:
                    standard_name_key = f'{var_name}.tile_standard_name_s'
                    standard_name = store_doc.get(standard_name_key)
                    tile.variables.append(TileVariable(
                        variable_name=var_name,
                        standard_name=standard_name
                    ))

            tiles.append(tile)

        return tiles

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
