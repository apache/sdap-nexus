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


import json
import unittest
import urllib
#from multiprocessing.pool import ThreadPool
from unittest import skip

import numpy as np
from mock import Mock
from nexustiles.model.nexusmodel import Tile, BBox
from nexustiles.nexustiles import NexusTileService
from tornado.testing import AsyncHTTPTestCase, bind_unused_port
import tornado.web

from webservice.algorithms import StandardDeviationSearch
from webservice.nexus_tornado.request.handlers.NexusRequestHandler import NexusRequestHandler


class HttpParametersTest(AsyncHTTPTestCase):
    def get_app(self):
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path
        algorithm = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl
        thread_pool = tornado.concurrent.futures.ThreadPoolExecutor(1)
        #thread_pool = ThreadPool(processes=1)
        return tornado.web.Application(
            [(path, NexusRequestHandler, dict(clazz=algorithm, thread_pool=thread_pool))],
            default_host=bind_unused_port()
        )

    def test_no_ds_400(self):
        response = self.fetch(StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("'ds' argument is required", body['error'])

    def test_no_longitude_400(self):
        params = {
            "ds": "dataset"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("'longitude' argument is required", body['error'])

    def test_no_latitude_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("'latitude' argument is required", body['error'])

    def test_no_day_or_date_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("At least one of 'day' or 'date' arguments are required but not both.", body['error'])

    def test_no_day_not_int_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "day": "yayday"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("At least one of 'day' or 'date' arguments are required but not both.", body['error'])

    def test_day_and_date_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "day": "35",
            "date": "1992-01-01T00:00:00Z"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("At least one of 'day' or 'date' arguments are required but not both.", body['error'])

    def test_no_allInTile_200(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "day": "35"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(200, response.code)

    def test_allInTile_false_200(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "date": "1992-01-01T00:00:00Z",
            "allInTile": "false"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(200, response.code)

    @skip("Integration test only. Works only if you have Solr and Cassandra running locally with data ingested")
    def test_integration_all_in_tile(self):
        params = {
            "ds": "AVHRR_OI_L4_GHRSST_NCEI_CLIM",
            "longitude": "-177.775",
            "latitude": "-78.225",
            "day": "1"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(200, response.code)
        print response.body
        body = json.loads(response.body)
        self.assertEqual(560, len(body['data']))

    @skip("Integration test only. Works only if you have Solr and Cassandra running locally with data ingested")
    def test_integration_all_in_tile_false(self):
        params = {
            "ds": "AVHRR_OI_L4_GHRSST_NCEI_CLIM",
            "longitude": "-177.875",
            "latitude": "-78.125",
            "date": "2016-01-01T00:00:00Z",
            "allInTile": "false"
        }
        path = StandardDeviationSearch.StandardDeviationSearchCalcHandlerImpl.path + '?' + urllib.urlencode(params)
        # Increase timeouts when debugging
        # self.http_client.fetch(self.get_url(path), self.stop, connect_timeout=99999999, request_timeout=999999999)
        # response = self.wait(timeout=9999999999)
        response = self.fetch(path)
        self.assertEqual(200, response.code)
        print response.body
        body = json.loads(response.body)
        self.assertAlmostEqual(-177.875, body['data'][0]['longitude'], 3)
        self.assertAlmostEqual(-78.125, body['data'][0]['latitude'], 3)
        self.assertAlmostEqual(0.4956, body['data'][0]['standard_deviation'], 4)


class TestStandardDeviationSearch(unittest.TestCase):
    def setUp(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))
        tile.meta_data = {"std": np.ma.arange(25.0).reshape((1, 5, 5))}

        attrs = {'find_tile_by_polygon_and_most_recent_day_of_year.return_value': [tile]}
        self.tile_service = Mock(spec=NexusTileService, **attrs)

    def test_get_single_exact_std_dev(self):
        result = StandardDeviationSearch.get_single_std_dev(self.tile_service, "fake dataset", 1.0, .5, 83)
        self.assertEqual(1, len(result))
        self.assertEqual((1.0, 0.5, 18.0), result[0])

    def test_get_single_close_std_dev(self):
        result = StandardDeviationSearch.get_single_std_dev(self.tile_service, "fake dataset", 1.3, .25, 83)
        self.assertEqual(1, len(result))
        self.assertEqual((1.0, 0.0, 13.0), result[0])

    def test_get_all_std_dev(self):
        result = StandardDeviationSearch.get_all_std_dev(self.tile_service, "fake dataset", 1.3, .25, 83)
        self.assertEqual(25, len(result))


@skip("Integration test only. Works only if you have Solr and Cassandra running locally with data ingested")
class IntegrationTestStandardDeviationSearch(unittest.TestCase):
    def setUp(self):
        self.tile_service = NexusTileService()

    def test_get_single_exact_std_dev(self):
        result = StandardDeviationSearch.get_single_std_dev(self.tile_service, "AVHRR_OI_L4_GHRSST_NCEI_CLIM", -177.625,
                                                            -78.375, 1)
        self.assertEqual(1, len(result))
        self.assertAlmostEqual(-177.625, result[0][0], 3)
        self.assertAlmostEqual(-78.375, result[0][1], 3)
        self.assertAlmostEqual(0.5166, result[0][2], 4)

    def test_get_single_close_std_dev(self):
        result = StandardDeviationSearch.get_single_std_dev(self.tile_service, "AVHRR_OI_L4_GHRSST_NCEI_CLIM", -177.775,
                                                            -78.225, 1)
        self.assertEqual(1, len(result))
        self.assertAlmostEqual(-177.875, result[0][0], 3)
        self.assertAlmostEqual(-78.125, result[0][1], 3)
        self.assertAlmostEqual(0.4956, result[0][2], 4)

    def test_get_all_std_dev(self):
        result = StandardDeviationSearch.get_all_std_dev(self.tile_service, "AVHRR_OI_L4_GHRSST_NCEI_CLIM", -177.775,
                                                         -78.225, 1)

        self.assertEqual(560, len(result))
