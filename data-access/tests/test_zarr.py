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
import io
import os

import json
import mock
import numpy as np
import pytest
import s3fs
from moto import mock_s3
from nexustiles.nexustiles import NexusTileService
import webservice.algorithms_spark.Matchup as matchup


class DummyTile:
    def __init__(self, tile_id):
        self.tile_id = tile_id

class MockSparkParam:
    def __init__(self, value):
        self.value = value

mock_s3 = mock_s3()
bucket_name = 'cdms-dev-zarr'
root_key = ""
region = 'us-west-2'

port = 5555
endpoint = f"http://127.0.0.1:{port}"

test_data_path = os.getenv('TEST_DATA', 'data/zarr_test_data/')

@pytest.fixture(scope="module")
def s3():
    # Got this from s3fs test on github
    # https://github.com/fsspec/s3fs/blob/main/s3fs/tests/test_s3fs.py

    import shlex
    import subprocess
    import time
    import requests

    try:
        # should fail since we didn't start server yet
        r = requests.get(endpoint)
    except:
        pass
    else:
        if r.ok:
            raise RuntimeError("moto server already up")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    proc = subprocess.Popen(shlex.split("moto_server s3 -p %s" % port))

    timeout = 5
    while timeout > 0:
        try:
            r = requests.get(endpoint)
            if r.ok:
                break
        except:
            pass
        timeout -= 0.1
        time.sleep(0.1)

    from botocore.session import Session
    session = Session()
    client = session.create_client("s3", endpoint_url=endpoint)

    client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})

    fs = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint})

    for root, dirs, filenames in os.walk(test_data_path):
        for file in filenames:
            local = os.path.join(root, file)
            rel = os.path.relpath(local, test_data_path)

            key = os.path.join(bucket_name, root_key, rel)

            fs.put(local, key)

    yield fs
    proc.terminate()
    proc.wait()

@pytest.fixture()
def bounds():
    yield {
        'min_lat': 20,
        'max_lat': 30,
        'min_lon': -100,
        'max_lon': -79,
        'start_time': '2018-01-01T09:00:00+00:00',
        'end_time': '2018-09-01T00:00:00+00:00'
    }

@pytest.fixture()
def ts_config():
    cfg = f"""
        [s3]
        bucket={bucket_name}
        key={root_key}
        region={region}
        public=false
        [datastore]
        store=zarrS3
        """

    buf = io.StringIO(cfg)
    config = configparser.ConfigParser()
    config.read_file(buf)

    yield config

def do_nothing(arg):
    pass

@pytest.fixture()
def tile_service_direct(s3, ts_config):
    from nexustiles.dao.ZarrProxy import ZarrProxy

    ts_config.set("s3", "key", "MUR25-JPL-L4-GLOB-v04.2")

    svc = NexusTileService(skipMetadatastore=True, config=ts_config, skipDatastore=True)
    svc._datastore = ZarrProxy(ts_config, open_direct=True, test_fs=s3)
    svc._datastore.open_dataset = do_nothing

    def mock_query(ds):
        pass

    mock_solr = mock.MagicMock()
    mock_solr.do_query_raw = mock_query

    yield svc

@pytest.fixture()
def tile_service_indirect(s3, ts_config):
    from nexustiles.dao.ZarrProxy import ZarrProxy

    svc = NexusTileService(skipMetadatastore=True, config=ts_config, skipDatastore=True)
    svc._datastore = ZarrProxy(ts_config, test_fs=s3)

    def mock_query(ds):
        if ds == "id:MUR25-JPL-L4-GLOB-v04.2":
            return json.load(open("mock_mur_meta.json"))
        elif ds == "id:OISSS_L4_multimission_7day_v1":
            return json.load(open("mock_oisss_meta.json"))
        else:
            raise ValueError("unsupported dataset")

    mock_solr = mock.MagicMock()
    mock_solr.do_query_raw = mock_query

    svc._datastore._metadata_store = mock_solr
    svc._datastore.open_dataset("OISSS_L4_multimission_7day_v1", test_fs=s3)
    svc._datastore.open_dataset = do_nothing

    yield svc

def test_setup(s3):
    assert True

def test_bounds(bounds, tile_service_direct):
    tile_id = tile_service_direct.bounds_to_direct_tile_id(
        bounds['min_lat'],
        bounds['min_lon'],
        bounds['max_lat'],
        bounds['max_lon'],
        bounds['start_time'],
        bounds['end_time']
    )

    tiles = [DummyTile(tile_id)]

    tile_data = tile_service_direct.fetch_data_for_tiles(*tiles)[0]

    assert bounds['min_lat'] <= np.amin(tile_data.latitudes)
    assert bounds['max_lat'] >= np.amax(tile_data.latitudes)
    assert bounds['min_lon'] <= np.amin(tile_data.longitudes)
    assert bounds['max_lon'] >= np.amax(tile_data.longitudes)

    assert bounds['start_time'] <= str(np.amin(tile_data.times))
    assert bounds['end_time'] >= str(np.amax(tile_data.times))

def filter_time(res, match_args):
    filtered = []

    for p in res:
        if abs(matchup.iso_time_to_epoch(p[0].time) - matchup.iso_time_to_epoch(p[1].time)) <= match_args['tt_b'].value:
            filtered.append(p)

    return filtered

# [(k,v),...] -> {k: [v,...],...}
def to_map(res):
    mapped = {}

    for p in res:
        k = p[0]
        v = p[1]

        if not k in mapped:
            mapped[k] = [v]
        else:
            mapped[k].append(v)

    return mapped

def validate_point(point, time, lon, lat, value, name, cf_name=None, secondary_point=False):
    assert point.time == time
    assert point.longitude == lon
    assert point.latitude == lat
    if not secondary_point:
        assert point.data[0].variable_value == value
        assert point.data[0].variable_name == name
        assert point.data[0].cf_variable_name == cf_name
    else:
        assert point.data[0].variable_value == value[0]
        assert point.data[0].variable_name == name[0]
        assert point.data[1].variable_value == value[1]
        assert point.data[1].variable_name == name[1]

def test_matchup_direct_open(bounds, tile_service_direct):
    tile_service_factory = mock.MagicMock()
    tile_service_factory.return_value = tile_service_direct

    tile_id = tile_service_direct.bounds_to_direct_tile_id(
        bounds['min_lat'],
        bounds['min_lon'],
        bounds['max_lat'],
        bounds['max_lon'],
        bounds['start_time'],
        bounds['end_time'],
        dataset="MUR"
    )

    tiles = [tile_id]

    with mock.patch('webservice.algorithms_spark.Matchup.edge_endpoints.getEndpointByName') as mock_edge_endpoints:
        match_args = dict(
            tile_ids=tiles,
            primary_b=MockSparkParam('MUR25-JPL-L4-GLOB-v04.2'),
            secondary_b=MockSparkParam('ICOADS Release 3.0'),
            parameter_b=MockSparkParam(None),
            tt_b=MockSparkParam(43200),
            rt_b=MockSparkParam(1000),
            platforms_b=MockSparkParam('42'),
            bounding_wkt_b=MockSparkParam('POLYGON((-100 20, -79 20, -79 30, -100 30, -100 20))'),
            depth_min_b=MockSparkParam(-20.0),
            depth_max_b=MockSparkParam(10.0),
            tile_service_factory=tile_service_factory
        )

        test_dir = os.path.dirname(os.path.realpath(__file__))
        test_data_dir = os.path.join(test_dir, 'data')

        mock_edge_endpoints.return_value = {'url': 'http://test-edge-url'}
        matchup.query_edge = lambda *args, **kwargs: json.load(open(os.path.join(test_data_dir, 'mock_response_direct.json')))
        matchup.query_insitu_schema = lambda  *args, **kwargs: json.load(open(os.path.join(test_data_dir, 'mock_insitu_schema.json')))

        generator = matchup.match_satellite_to_insitu(**match_args)

        result = to_map(filter_time(list(generator), match_args))

        assert len(result) == 4

        keys = list(result.keys())

        validate_point(keys[0], '2018-08-17T09:00:00Z', -90.125, 27.625, 303.447998046875, 'analysed_sst', 'sea_surface_foundation_temperature')
        validate_point(keys[1], '2018-08-21T09:00:00Z', -90.375, 28.125, 303.49200439453125, 'analysed_sst', 'sea_surface_foundation_temperature')
        validate_point(keys[2], '2018-08-22T09:00:00Z', -90.125, 28.125, 303.3800048828125, 'analysed_sst', 'sea_surface_foundation_temperature')
        validate_point(keys[3], '2018-08-27T09:00:00Z', -86.125, 27.625, 303.4729919433594, 'analysed_sst', 'sea_surface_foundation_temperature')

        v0 = result[keys[0]]
        v1 = result[keys[1]]
        v2 = result[keys[2]]
        v3 = result[keys[3]]

        assert len(v0) == 6
        assert len(v1) == 1
        assert len(v2) == 1
        assert len(v3) == 2

        validate_point(v0[0], '2018-08-17T05:00:00Z', -90.13, 27.62, [30.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v0[1], '2018-08-17T05:30:00Z', -90.13, 27.62, [30.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v0[2], '2018-08-17T06:00:00Z', -90.13, 27.62, [30.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v0[3], '2018-08-17T06:30:00Z', -90.13, 27.63, [30.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v0[4], '2018-08-17T07:00:00Z', -90.13, 27.63, [30.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v0[5], '2018-08-17T07:30:00Z', -90.13, 27.63, [30.3,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)

        validate_point(v1[0], '2018-08-21T01:00:00Z', -90.38, 28.12, [30.0,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)

        validate_point(v2[0], '2018-08-22T01:00:00Z', -90.13, 28.12, [30.3,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)

        validate_point(v3[0], '2018-08-27T12:30:00Z', -86.12, 27.62, [30.0,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v3[1], '2018-08-27T13:00:00Z', -86.13, 27.63, [30.0,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)

def test_matchup_select_ds(bounds, tile_service_indirect):
    import webservice.algorithms_spark.Matchup as matchup

    tile_service_factory = mock.MagicMock()
    tile_service_factory.return_value = tile_service_indirect

    bounds['start_time'] = "2017-03-01T09:00:00+00:00"
    bounds['end_time'] = "2017-06-01T00:00:00+00:00"

    tile_id = tile_service_indirect.bounds_to_direct_tile_id(
        bounds['min_lat'],
        bounds['min_lon'],
        bounds['max_lat'],
        bounds['max_lon'],
        bounds['start_time'],
        bounds['end_time'],
        dataset="OISSS"
    )

    tiles = [tile_id]

    with mock.patch('webservice.algorithms_spark.Matchup.edge_endpoints.getEndpointByName') as mock_edge_endpoints:
        match_args = dict(
            tile_ids=tiles,
            primary_b=MockSparkParam('OISSS_L4_multimission_7day_v1'),
            secondary_b=MockSparkParam('ICOADS Release 3.0'),
            parameter_b=MockSparkParam(None),
            tt_b=MockSparkParam(43200),
            rt_b=MockSparkParam(1000),
            platforms_b=MockSparkParam('42'),
            bounding_wkt_b=MockSparkParam('POLYGON((-100 20, -79 20, -79 30, -100 30, -100 20))'),
            depth_min_b=MockSparkParam(-20.0),
            depth_max_b=MockSparkParam(10.0),
            tile_service_factory=tile_service_factory
        )

        test_dir = os.path.dirname(os.path.realpath(__file__))
        test_data_dir = os.path.join(test_dir, 'data')

        mock_edge_endpoints.return_value = {'url': 'http://test-edge-url'}
        matchup.query_edge = lambda *args, **kwargs: json.load(open(os.path.join(test_data_dir, 'mock_response_indirect.json')))
        matchup.query_insitu_schema = lambda  *args, **kwargs: json.load(open(os.path.join(test_data_dir, 'mock_insitu_schema.json')))

        generator = matchup.match_satellite_to_insitu(**match_args)

        #result =

        #result = to_map(filter_time(result, match_args))
        result = to_map(filter_time(list(generator), match_args))

        assert len(result) == 3

        keys = list(result.keys())

        validate_point(keys[0], '2017-04-06T00:00:00Z', -90.625, 23.625, 36.32952880859375,  'sss', 'sea_surface_salinity')
        validate_point(keys[1], '2017-04-10T00:00:00Z', -90.875, 23.875, 36.40681838989258,  'sss', 'sea_surface_salinity')
        validate_point(keys[2], '2017-04-14T00:00:00Z', -89.125, 24.125, 36.365089416503906, 'sss', 'sea_surface_salinity')

        v0 = result[keys[0]]
        v1 = result[keys[1]]
        v2 = result[keys[2]]

        assert len(v0) == 2
        assert len(v1) == 2
        assert len(v2) == 12

        validate_point(v0[ 0], '2017-04-06T04:16:12Z', -90.62, 23.63, [25.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v0[ 1], '2017-04-06T05:01:12Z', -90.62, 23.63, [25.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)

        validate_point(v1[ 0], '2017-04-09T23:30:00Z', -90.87, 23.88, [24.8,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v1[ 1], '2017-04-09T23:34:48Z', -90.88, 23.88, [24.8,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)

        validate_point(v2[ 0], '2017-04-13T22:30:00Z', -89.12, 24.12, [24.6,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 1], '2017-04-13T23:30:00Z', -89.12, 24.12, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 2], '2017-04-13T23:45:00Z', -89.12, 24.12, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 3], '2017-04-14T00:00:00Z', -89.12, 24.12, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 4], '2017-04-14T00:15:00Z', -89.12, 24.13, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 5], '2017-04-14T00:30:00Z', -89.12, 24.13, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 6], '2017-04-14T01:15:00Z', -89.12, 24.13, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 7], '2017-04-14T02:00:00Z', -89.13, 24.13, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 8], '2017-04-14T02:15:00Z', -89.13, 24.12, [24.5,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[ 9], '2017-04-14T03:45:00Z', -89.13, 24.12, [24.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[10], '2017-04-14T04:45:00Z', -89.13, 24.12, [24.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
        validate_point(v2[11], '2017-04-14T09:15:00Z', -89.13, 24.12, [24.4,1], ['sea_water_temperature', 'sea_water_temperature_quality'], secondary_point=True)
