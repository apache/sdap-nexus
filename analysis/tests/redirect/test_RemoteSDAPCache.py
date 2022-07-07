import unittest
from unittest import mock
import requests

from webservice.redirect import RemoteSDAPCache


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

def mocked_requests_get(*asgs, **kwargs):
    json_data = [
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
    return MockResponse(json_data, 200)


def mocked_requests_get_timeout(*asgs, **kwargs):
    raise requests.exceptions.ConnectTimeout()


class MyTestCase(unittest.TestCase):

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get(self, mock_get):
        remote_sdap_cache = RemoteSDAPCache()

        collection = remote_sdap_cache.get('https://aq-sdap.stcenter.net/nexus/', 'PM25')
        self.assertEqual(collection["start"], 1514818800.0)


if __name__ == '__main__':
    unittest.main()
