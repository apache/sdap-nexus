# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import configparser
import datetime
import logging
import pkg_resources
import time

from nexustiles.dao.ElasticsearchProxy import ElasticsearchProxy
from shapely.geometry import box


class TestQuery(unittest.TestCase):
    def setUp(self):
        config = configparser.ConfigParser()
        config.read_file(open("config/datastores.ini"))
        self.proxy = ElasticsearchProxy(config)
        logging.basicConfig(level=logging.INFO)

        self.query_data = configparser.ConfigParser()
        self.query_data.read_file(open("config/elasticsearch_query_data.ini"))

    def test_get_tile_count(self):
        bounding_polygon = box(self.query_data.getfloat('get_tile_count', 'min_lon'),
                               self.query_data.getfloat('get_tile_count', 'min_lat'),
                               self.query_data.getfloat('get_tile_count', 'max_lon'),
                               self.query_data.getfloat('get_tile_count', 'max_lat'))

        metadata_keys = self.query_data.get('get_tile_count', 'metadata_keys').split(',')
        metadata_values = self.query_data.get('get_tile_count', 'metadata_values').split(',')
        metadata = []
        for index, key in enumerate(metadata_keys):
            metadata.append(key + ':' + metadata_values[index])
        
        result = self.proxy.get_tile_count(self.query_data.get('get_tile_count', 'dataset_name'),
                                           bounding_polygon,
                                           self.query_data.getint('get_tile_count', 'start_time'),
                                           self.query_data.getint('get_tile_count', 'end_time'),
                                           metadata)

        self.assertIsInstance(result, int)
        self.assertIsNot(result, 0)
        # print('RESULT FROM get_tile_count = ' + str(result))
    
    def test_find_all_tiles_by_metadata(self):
        metadata_keys = self.query_data.get('find_all_tiles_by_metadata', 'metadata_keys').split(',')
        metadata_values = self.query_data.get('find_all_tiles_by_metadata', 'metadata_values').split(',')
        metadata = []
        for index, key in enumerate(metadata_keys):
            metadata.append(key + ':' + metadata_values[index])
        
        result = self.proxy.find_all_tiles_by_metadata(metadata,
                                                       self.query_data.get('find_all_tiles_by_metadata', 'dataset_name'),
                                                       self.query_data.getint('find_all_tiles_by_metadata', 'start_time'),
                                                       self.query_data.getint('find_all_tiles_by_metadata', "end_time"))
        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM find_all_tiles_by_metadata (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_distinct_bounding_boxes_in_polygon(self):
        bounding_polygon = box(self.query_data.getfloat('find_distinct_bounding_boxes_in_polygon', 'min_lon'),
                               self.query_data.getfloat('find_distinct_bounding_boxes_in_polygon', 'min_lat'),
                               self.query_data.getfloat('find_distinct_bounding_boxes_in_polygon', 'max_lon'),
                               self.query_data.getfloat('find_distinct_bounding_boxes_in_polygon', 'max_lat'))

        result = self.proxy.find_distinct_bounding_boxes_in_polygon(
            bounding_polygon,
            self.query_data.get('find_distinct_bounding_boxes_in_polygon', 'dataset_name'),
            self.query_data.getint('find_distinct_bounding_boxes_in_polygon', 'start_time'),
            self.query_data.getint('find_distinct_bounding_boxes_in_polygon', 'end_time'))
        
        self.assertIsNot(len(result), 0)
        self.assertIsInstance(result, list)

        # print('RESULT FROM find_distinct_bounding_boxes_in_polygon (LENGTH = ' + str(len(result)) + ') -> ' + str(result))              

    def test_find_tile_by_id(self):
        result = self.proxy.find_tile_by_id(self.query_data.get('find_tile_by_id', 'tile_id'))
        self.assertIs(len(result), 1)
        # print('RESULT FROM find_tile_by_id = ' + str(result))

    def test_find_tiles_by_id(self):
        tile_ids = [tile_id for tile_id in self.query_data.get('find_tiles_by_id', 'tile_ids').split(',')]
        result = self.proxy.find_tiles_by_id(tile_ids,
                                             self.query_data.get('find_tiles_by_id', 'dataset_name'))
        self.assertIs(len(result), len(tile_ids)) 
        # print('RESULT FROM find_tiles_by_id = ' + str(result[:10]))
    
    def test_find_min_date_from_tiles(self):
        tile_ids = [tile_id for tile_id in self.query_data.get('find_min_date_from_tiles', 'tile_ids').split(',')]
        result = self.proxy.find_min_date_from_tiles(tile_ids, self.query_data.get('find_min_date_from_tiles', 'dataset_name'))
        self.assertRegex(str(result), r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}')    
        # print('RESULT FROM find_min_date_from_tiles = ' + str(result))

    def test_find_max_date_from_tiles(self):
        tile_ids = [tile_id for tile_id in self.query_data.get('find_max_date_from_tiles', 'tile_ids').split(',')]
        result = self.proxy.find_max_date_from_tiles(tile_ids, self.query_data.get('find_max_date_from_tiles', 'dataset_name'))
        self.assertRegex(str(result), r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}')
        # print('RESULT FROM find_max_date_from_tiles = ' + str(result))

    def test_find_min_max_date_from_granule(self):
        result = self.proxy.find_min_max_date_from_granule(self.query_data.get('find_min_max_date_from_granule', 'dataset_name'),
                                                           self.query_data.get('find_min_max_date_from_granule', 'granule_name'))
        self.assertIs(len(result), 2)
        self.assertIsInstance(result[0], datetime.datetime)
        self.assertIsInstance(result[1], datetime.datetime)
        
        # print('RESULT FROM find_min_max_date_from_granule = ' + str(result)) 

    def test_get_data_series_list(self):
        result = self.proxy.get_data_series_list()

        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM get_data_series_list = ' + str(result[:10]))
    
    def test_get_data_series_list_simple(self):
        result = self.proxy.get_data_series_list_simple()
        
        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM get_data_series_list_simple = ' + str(result[:10]))

    def test_get_data_series_stats(self):
        result = self.proxy.get_data_series_stats(self.query_data.get('get_data_series_stats', 'dataset_name'))
        self.assertIsInstance(result, dict)
        self.assertIs(len(result), 5)
        
        result['available_dates'] = len(result['available_dates'])
        # print('RESULT FROM get_data_series_stats (length of available dates) = ' + str(result))

    def test_find_days_in_range_asc(self):
        result = self.proxy.find_days_in_range_asc(self.query_data.getfloat('find_days_in_range_asc', 'min_lat'),
                                                   self.query_data.getfloat('find_days_in_range_asc', 'max_lat'),
                                                   self.query_data.getfloat('find_days_in_range_asc', 'min_lon'),
                                                   self.query_data.getfloat('find_days_in_range_asc', 'max_lon'),
                                                   self.query_data.get('find_days_in_range_asc', 'dataset_name'),
                                                   self.query_data.getint('find_days_in_range_asc', 'start_time'),
                                                   self.query_data.getint('find_days_in_range_asc', 'end_time'))
        self.assertIsNot(len(result), 0)
        self.assertIsInstance(result[0], float)
        # print('RESULT FROM find_days_in_range_asc = ' + str(result[:10]))

    def test_find_all_tiles_in_box_sorttimeasc(self):
        result = self.proxy.find_all_tiles_in_box_sorttimeasc(
            self.query_data.getfloat('find_all_tiles_in_box_sorttimeasc', 'min_lat'),
            self.query_data.getfloat('find_all_tiles_in_box_sorttimeasc', 'max_lat'),
            self.query_data.getfloat('find_all_tiles_in_box_sorttimeasc', 'min_lon'),
            self.query_data.getfloat('find_all_tiles_in_box_sorttimeasc', 'max_lon'),
            self.query_data.get('find_all_tiles_in_box_sorttimeasc', 'dataset_name'),
            self.query_data.getint('find_all_tiles_in_box_sorttimeasc', 'start_time'),
            self.query_data.getint('find_all_tiles_in_box_sorttimeasc', 'end_time'))
        
        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM find_all_tiles_in_box_sorttimeasc (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:20]))

    def test_find_all_tiles_in_polygon_sorttimeasc(self):
        bounding_polygon = box(self.query_data.getfloat('find_all_tiles_in_polygon_sorttimeasc', 'min_lon'),
                               self.query_data.getfloat('find_all_tiles_in_polygon_sorttimeasc', 'min_lat'),
                               self.query_data.getfloat('find_all_tiles_in_polygon_sorttimeasc', 'max_lon'),
                               self.query_data.getfloat('find_all_tiles_in_polygon_sorttimeasc', 'max_lat'))

        result = self.proxy.find_all_tiles_in_polygon_sorttimeasc(
            bounding_polygon,
            self.query_data.get('find_all_tiles_in_polygon_sorttimeasc', 'dataset_name'),
            self.query_data.getint('find_all_tiles_in_polygon_sorttimeasc', 'start_time'),
            self.query_data.getint('find_all_tiles_in_polygon_sorttimeasc', 'end_time'))
        self.assertIsNotNone(result)

        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError
        
        # print('FULL RESULT FROM find_all_tiles_in_polygon_sorttimeasc = ' + str(result))
        # print('RESULT FROM find_all_tiles_in_polygon_sorttimeasc (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_all_tiles_in_polygon(self):
        bounding_polygon = box(self.query_data.getfloat('find_all_tiles_in_polygon', 'min_lon'),
                               self.query_data.getfloat('find_all_tiles_in_polygon', 'min_lat'),
                               self.query_data.getfloat('find_all_tiles_in_polygon', 'max_lon'),
                               self.query_data.getfloat('find_all_tiles_in_polygon', 'max_lat'))

        result = self.proxy.find_all_tiles_in_polygon(bounding_polygon,
                                                      self.query_data.get('find_all_tiles_in_polygon', 'dataset_name'),
                                                      self.query_data.getint('find_all_tiles_in_polygon', 'start_time'),
                                                      self.query_data.getint('find_all_tiles_in_polygon', 'end_time'))

        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM find_all_tiles_in_polygon (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_tiles_by_exact_bounds(self):
        result = self.proxy.find_tiles_by_exact_bounds(
            self.query_data.getfloat('find_tiles_by_exact_bounds', 'min_lon'),
            self.query_data.getfloat('find_tiles_by_exact_bounds', 'min_lat'),
            self.query_data.getfloat('find_tiles_by_exact_bounds', 'max_lon'),
            self.query_data.getfloat('find_tiles_by_exact_bounds', 'max_lat'),
            self.query_data.get('find_tiles_by_exact_bounds', 'dataset_name'),
            self.query_data.getint('find_tiles_by_exact_bounds', 'start_time'),
            self.query_data.getint('find_tiles_by_exact_bounds', 'end_time'))
        
        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM find_tiles_by_exact_bounds (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_all_tiles_in_box_at_time(self):
        result = self.proxy.find_all_tiles_in_box_at_time(
            self.query_data.getfloat('find_all_tiles_in_box_at_time', 'min_lat'),
            self.query_data.getfloat('find_all_tiles_in_box_at_time', 'max_lat'),
            self.query_data.getfloat('find_all_tiles_in_box_at_time', 'min_lon'),
            self.query_data.getfloat('find_all_tiles_in_box_at_time', 'max_lon'),
            self.query_data.get('find_all_tiles_in_box_at_time', 'dataset_name'),
            self.query_data.getint('find_all_tiles_in_box_at_time', 'search_time'))

        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM find_all_tiles_in_box_at_time (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_all_tiles_in_polygon_at_time(self):
        bounding_polygon = box(self.query_data.getfloat('find_all_tiles_in_polygon_at_time', 'min_lon'),
                               self.query_data.getfloat('find_all_tiles_in_polygon_at_time', 'min_lat'),
                               self.query_data.getfloat('find_all_tiles_in_polygon_at_time', 'max_lon'),
                               self.query_data.getfloat('find_all_tiles_in_polygon_at_time', 'max_lat'))

        result = self.proxy.find_all_tiles_in_polygon_at_time(
            bounding_polygon,
            self.query_data.get('find_all_tiles_in_polygon_at_time', 'dataset_name'),
            self.query_data.getint('find_all_tiles_in_polygon_at_time', 'search_time'))
       
        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise AssertionError

        # print('RESULT FROM find_all_tiles_in_polygon_at_time (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_all_tiles_within_box_at_time(self):
        result = self.proxy.find_all_tiles_within_box_at_time(
            self.query_data.getfloat('find_all_tiles_within_box_at_time', 'min_lat'),
            self.query_data.getfloat('find_all_tiles_within_box_at_time', 'max_lat'),
            self.query_data.getfloat('find_all_tiles_within_box_at_time', 'min_lon'),
            self.query_data.getfloat('find_all_tiles_within_box_at_time', 'max_lon'),
            self.query_data.get('find_all_tiles_within_box_at_time', 'dataset_name'),
            self.query_data.getint('find_all_tiles_within_box_at_time', 'search_time'))
        
        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise assertionerror

        # print('RESULT FROM find_all_tiles_within_box_at_time (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_all_boundary_tiles_at_time(self):
        result = self.proxy.find_all_boundary_tiles_at_time(
            self.query_data.getfloat('find_all_boundary_tiles_at_time', 'min_lat'),
            self.query_data.getfloat('find_all_boundary_tiles_at_time', 'max_lat'),
            self.query_data.getfloat('find_all_boundary_tiles_at_time', 'min_lon'),
            self.query_data.getfloat('find_all_boundary_tiles_at_time', 'max_lon'),
            self.query_data.get('find_all_boundary_tiles_at_time', 'dataset_name'),
            self.query_data.getint('find_all_boundary_tiles_at_time', 'search_time'))
        
        try:
            self.assertIsInstance(result[0], dict)
        except IndexError:
            raise assertionerror

        # print('RESULT FROM find_all_boundary_tiles_at_time (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

    def test_find_tile_by_polygon_and_most_recent_day_of_year(self):
        bounding_polygon = box(self.query_data.getfloat('find_tile_by_polygon_and_most_recent_day_of_year', 'min_lon'),
                               self.query_data.getfloat('find_tile_by_polygon_and_most_recent_day_of_year', 'min_lat'),
                               self.query_data.getfloat('find_tile_by_polygon_and_most_recent_day_of_year', 'max_lon'),
                               self.query_data.getfloat('find_tile_by_polygon_and_most_recent_day_of_year', 'max_lat'))
        result = self.proxy.find_tile_by_polygon_and_most_recent_day_of_year(
            bounding_polygon,
            self.query_data.get('find_tile_by_polygon_and_most_recent_day_of_year', 'dataset_name'),
            self.query_data.getint('find_tile_by_polygon_and_most_recent_day_of_year', 'day_of_year'))
        
        self.assertIs(len(result), 1)
        self.assertIsInstance(result, list)
        # print('RESULT FROM find_tile_by_polygon_and_most_recent_day_of_year (LENGTH = ' + str(len(result)) + ') -> ' + str(result[:10]))

