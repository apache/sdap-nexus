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

import unittest
from unittest import mock
import requests
from datetime import datetime
from datetime import timedelta

from webservice.redirect import RemoteSDAPCache
from webservice.redirect import CollectionNotFound
from webservice.redirect.RemoteSDAPCache import RemoteSDAPList

class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

LIST_CONTENT = [
      {
        "shortName": "PM25",
        "title": "PM25",
        "tileCount": 21515,
        "start": 1514818800.0,
        "end": 1640991600.0,
        "iso_start": "2018-01-01T15:00:00+0000",
        "iso_end": "2021-12-31T23:00:00+0000"
      }
    ]

LIST_CONTENT_FORMER = [LIST_CONTENT[0].copy()]
LIST_CONTENT_FORMER[0]['start'] = 0

def mocked_requests_get(*asgs, **kwargs):
    return MockResponse(LIST_CONTENT, 200)


def mocked_requests_get_timeout(*asgs, **kwargs):
    raise requests.exceptions.ConnectTimeout()


def mocked_requests_get_not_found(*asgs, **kwargs):
    return MockResponse({}, 404)



class MyTestCase(unittest.TestCase):

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get(self, mock_get):
        remote_sdap_cache = RemoteSDAPCache()

        collection = remote_sdap_cache.get('https://aq-sdap.stcenter.net/nexus/', 'PM25')
        self.assertEqual(collection["start"], 1514818800.0)

    @mock.patch('requests.get', side_effect=mocked_requests_get_timeout)
    def test_get_timeout(self, mock_get):
        remote_sdap_cache = RemoteSDAPCache()
        with self.assertRaises(CollectionNotFound):
            remote_sdap_cache.get('https://aq-sdap.stcenter.net/nexus/', 'PM25')


    @mock.patch('requests.get', side_effect=mocked_requests_get_not_found)
    def test_get_not_found(self, mock_get):
        remote_sdap_cache = RemoteSDAPCache()
        with self.assertRaises(CollectionNotFound):
            remote_sdap_cache.get('https://aq-sdap.stcenter.net/nexus/', 'PM25')

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_expired(self, mock_get):
        remote_sdap_cache = RemoteSDAPCache()

        remote_sdap_cache.sdap_lists['https://aq-sdap.stcenter.net/nexus/'] = RemoteSDAPList(
            list=LIST_CONTENT_FORMER,
            outdated_at=datetime.now() - timedelta(seconds=3600*25)
        )

        collection = remote_sdap_cache.get('https://aq-sdap.stcenter.net/nexus/', 'PM25')

        # check requests.get is called once
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(collection["start"], 1514818800.0)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_cached_valid(self, mock_get):
        remote_sdap_cache = RemoteSDAPCache()

        remote_sdap_cache.sdap_lists['https://aq-sdap.stcenter.net/nexus'] = RemoteSDAPList(
            list=LIST_CONTENT_FORMER,
            outdated_at=datetime.now() - timedelta(seconds=3600 * 23)
        )

        collection = remote_sdap_cache.get('https://aq-sdap.stcenter.net/nexus/', 'PM25')

        # check requests.get is called once
        self.assertEqual(mock_get.call_count, 0)
        self.assertEqual(collection["start"], 0)



if __name__ == '__main__':
    unittest.main()
