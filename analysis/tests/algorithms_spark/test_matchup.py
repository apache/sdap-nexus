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
import os
import pickle
from datetime import datetime, timezone

import mock
import numpy as np
import pytest
import webservice.algorithms_spark.Matchup as matchup
from nexustiles.model.nexusmodel import Tile
from pyspark.sql import SparkSession
from shapely import wkt
from shapely.geometry import box
from webservice.algorithms_spark.Matchup import DomsPoint, Matchup, DataPoint


class MockSparkParam:
    def __init__(self, value):
        self.value = value


@pytest.fixture(scope='function')
def test_dir():
    test_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(test_dir, '..', 'data')
    yield test_data_dir


@pytest.fixture(scope='function')
def test_tile():
    yield Tile(
        tile_id='test-tile',
        bbox='',
        dataset='test-dataset',
        dataset_id='test-dataset',
        granule='test-granule',
        min_time='2020-07-28T00:00:00',
        max_time='2020-07-28T00:00:00',
        section_spec='2020-07-28T00:00:00',
        meta_data={},
        is_multi=True
    )


@pytest.fixture(scope='function')
def test_matchup_args():
    tile_ids = [1]
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'
    primary_ds_name = 'primary-ds-name'
    matchup_ds_names = 'test'
    parameter = 'sst'
    depth_min = 0.0
    depth_max = 1.0
    time_tolerance = 3.0
    radius_tolerance = 1000000.0
    platforms = '1,2,3,4,5,6,7,8,9'

    yield dict(
        tile_ids=tile_ids,
        primary_b=MockSparkParam(primary_ds_name),
        matchup_b=MockSparkParam(matchup_ds_names),
        parameter_b=MockSparkParam(parameter),
        tt_b=MockSparkParam(time_tolerance),
        rt_b=MockSparkParam(radius_tolerance),
        platforms_b=MockSparkParam(platforms),
        bounding_wkt_b=MockSparkParam(polygon_wkt),
        depth_min_b=MockSparkParam(depth_min),
        depth_max_b=MockSparkParam(depth_max)
    )


def setup_mock_tile_service(tile):
    tile_service_factory = mock.MagicMock()
    tile_service = mock.MagicMock()
    tile_service_factory.return_value = tile_service
    tile_service.get_bounding_box.return_value = box(-90, -45, 90, 45)
    tile_service.get_min_time.return_value = 1627490285
    tile_service.get_max_time.return_value = 1627490285
    tile_service.mask_tiles_to_polygon.return_value = [tile]
    return tile_service_factory


def test_doms_point_is_pickleable():
    edge_point = {
        'id': 'argo-profiles-5903995(46, 0)',
        'time': '2012-10-15T14:24:04Z',
        'point': '-33.467 29.728',
        'sea_water_temperature': 24.5629997253,
        'sea_water_temperature_depth': 2.9796258642,
        'wind_speed': None,
        'sea_water_salinity': None,
        'sea_water_salinity_depth': None,
        'platform': 4,
        'device': 3,
        'fileurl': 'ftp://podaac-ftp.jpl.nasa.gov/allData/argo-profiles-5903995.nc'
    }
    point = DomsPoint.from_edge_point(edge_point)
    assert pickle.dumps(point) is not None


def test_calc(test_matchup_args):
    """
    Assert that the expected functions are called during the matchup
    calculation and that the results are formatted as expected.
    """
    # Mock anything that connects external dependence (Solr, Cassandra, ...)
    tile_service_factory = mock.MagicMock()
    tile_service = mock.MagicMock()
    tile_service_factory.return_value = tile_service
    spark = SparkSession.builder.appName('nexus-analysis').getOrCreate()
    spark_context = spark.sparkContext
    request = mock.MagicMock()
    request.get_argument.return_value = '1,2,3,4'

    # Patch in request arguments
    start_time = datetime.strptime('2020-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=timezone.utc)
    end_time = datetime.strptime('2020-02-01T00:00:00', '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=timezone.utc)
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'
    args = {
        'bounding_polygon': wkt.loads(polygon_wkt),
        'primary_ds_name': 'primary-ds-name',
        'matchup_ds_names': 'matchup-ds-name',
        'parameter_s': 'sst',
        'start_time': start_time,
        'start_seconds_from_epoch': start_time.timestamp(),
        'end_time': end_time,
        'end_seconds_from_epoch': end_time.timestamp(),
        'depth_min': 1.0,
        'depth_max': 2.0,
        'time_tolerance': 3.0,
        'radius_tolerance': 4.0,
        'platforms': '1,2,3,4,5,6,7,8,9',
        'match_once': True,
        'result_size_limit': 10
    }

    def generate_fake_tile(tile_id):
        tile = Tile()
        tile.tile_id = tile_id
        return tile

    # Mock tiles
    fake_tiles = [generate_fake_tile(idx) for idx in range(10)]
    tile_service.find_tiles_in_polygon.return_value = fake_tiles

    # Mock result
    # Format of 'spark_result': keys=domspoint,values=list of domspoint

    doms_point_args = {
        'longitude': -180,
        'latitude': -90,
        'time': '2020-01-15T00:00:00Z'
    }
    d1_sat = DomsPoint(**doms_point_args)
    d2_sat = DomsPoint(**doms_point_args)
    d1_ins = DomsPoint(**doms_point_args)
    d2_ins = DomsPoint(**doms_point_args)

    d1_sat.data = [DataPoint(
        variable_name='sea_surface_temperature',
        variable_value=10.0
    )]
    d2_sat.data = [DataPoint(
        variable_name='sea_surface_temperature',
        variable_value=20.0
    )]
    d1_ins.data = [DataPoint(
        variable_name='sea_surface_temperature',
        variable_value=30.0
    )]
    d2_ins.data = [DataPoint(
        variable_name='sea_surface_temperature',
        variable_value=40.0
    )]

    fake_spark_result = {
        d1_sat: [d1_ins, d2_ins],
        d2_sat: [d1_ins, d2_ins],
    }

    matchup_obj = Matchup(tile_service_factory=tile_service_factory, sc=spark_context)
    matchup_obj.parse_arguments = lambda _: [item for item in args.values()]

    with mock.patch('webservice.algorithms_spark.Matchup.ResultsStorage') as mock_rs, \
            mock.patch(
                'webservice.algorithms_spark.Matchup.spark_matchup_driver') as mock_matchup_driver:
        mock_rs.insertExecution.return_value = 1
        mock_matchup_driver.return_value = fake_spark_result
        matchup_result = matchup_obj.calc(request)

        # Ensure the call to 'spark_matchup_driver' contains the expected params
        assert len(mock_matchup_driver.call_args_list) == 1
        matchup_driver_args = mock_matchup_driver.call_args_list[0].args
        matchup_driver_kwargs = mock_matchup_driver.call_args_list[0].kwargs
        assert matchup_driver_args[0] == [tile.tile_id for tile in fake_tiles]
        assert wkt.loads(matchup_driver_args[1]).equals(wkt.loads(polygon_wkt))
        assert matchup_driver_args[2] == args['primary_ds_name']
        assert matchup_driver_args[3] == args['matchup_ds_names']
        assert matchup_driver_args[4] == args['parameter_s']
        assert matchup_driver_args[5] == args['depth_min']
        assert matchup_driver_args[6] == args['depth_max']
        assert matchup_driver_args[7] == args['time_tolerance']
        assert matchup_driver_args[8] == args['radius_tolerance']
        assert matchup_driver_args[9] == args['platforms']
        assert matchup_driver_args[10] == args['match_once']
        assert matchup_driver_args[11] == tile_service_factory
        assert matchup_driver_kwargs['sc'] == spark_context

        # Ensure the result of the matchup calculation is as expected

        json_matchup_result = json.loads(matchup_result.toJson())
        assert len(json_matchup_result['data']) == 2
        assert len(json_matchup_result['data'][0]['matches']) == 2
        assert len(json_matchup_result['data'][1]['matches']) == 2

        for data in json_matchup_result['data']:
            assert data['x'] == '-180'
            assert data['y'] == '-90'
            for matches in data['matches']:
                assert matches['x'] == '-180'
                assert matches['y'] == '-90'

        assert json_matchup_result['data'][0]['data'][0]['variable_value'] == 10.0
        assert json_matchup_result['data'][1]['data'][0]['variable_value'] == 20.0
        assert json_matchup_result['data'][0]['matches'][0]['data'][0]['variable_value'] == 30.0
        assert json_matchup_result['data'][0]['matches'][1]['data'][0]['variable_value'] == 40.0
        assert json_matchup_result['data'][1]['matches'][0]['data'][0]['variable_value'] == 30.0
        assert json_matchup_result['data'][1]['matches'][1]['data'][0]['variable_value'] == 40.0

        assert json_matchup_result['details']['numInSituMatched'] == 4
        assert json_matchup_result['details']['numGriddedMatched'] == 2


def test_match_satellite_to_insitu(test_dir, test_tile, test_matchup_args):
    """
    Test the test_match_satellite_to_insitu and ensure the matchup is
    done as expected, where the tile points and in-situ points are all
    known and the expected matchup points have been hand-calculated.

    This test case mocks out all external dependencies, so Solr,
    Cassandra, HTTP insitu requests, etc are all mocked.

    The test points are as follows:

    X (0, 20)                         X (20, 20)

            O (5, 15)



                     O (10, 10)




                             O (18, 3)

    X (0, 0)                        X (20, 0)

    The 'X' points are the primary satellite points and the 'O' points
    are the secondary satellite or insitu points

    Visual inspection reveals that primary point (0, 20) should match
    with secondary point (5, 15) and primary point (20, 0) should match
    with (18, 3)
    """
    test_tile.var_names = ['sst']
    test_tile.standard_names = ['sea_surface_temperature']
    test_tile.latitudes = np.array([0, 20], dtype=np.float32)
    test_tile.longitudes = np.array([0, 20], dtype=np.float32)
    test_tile.times = [1627490285]
    test_tile.data = np.array([[[11.0, 21.0], [31.0, 41.0]]])
    test_tile.get_indices = lambda: [[0, 0, 0], [0, 0, 1], [0, 1, 0], [0, 1, 1]]
    test_tile.is_multi = False

    tile_ids = [1]
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'
    primary_ds_name = 'primary-ds-name'
    matchup_ds_names = 'test'
    parameter = 'sst'
    depth_min = 0.0
    depth_max = 1.0
    time_tolerance = 3.0
    radius_tolerance = 1000000.0
    platforms = '1,2,3,4,5,6,7,8,9'

    with mock.patch(
            'webservice.algorithms_spark.Matchup.edge_endpoints.getEndpointByName'
    ) as mock_edge_endpoints:
        # Test the satellite->insitu branch
        # By mocking the getEndpointsByName function we are forcing
        # Matchup to think this dummy matchup dataset is an insitu
        # dataset
        mock_edge_endpoints.return_value = {'url': 'http://test-edge-url'}
        matchup.query_edge = lambda *args, **kwargs: json.load(
            open(os.path.join(test_dir, 'edge_response.json')))

        match_args = dict(
            tile_ids=tile_ids,
            primary_b=MockSparkParam(primary_ds_name),
            matchup_b=MockSparkParam(matchup_ds_names),
            parameter_b=MockSparkParam(parameter),
            tt_b=MockSparkParam(time_tolerance),
            rt_b=MockSparkParam(radius_tolerance),
            platforms_b=MockSparkParam(platforms),
            bounding_wkt_b=MockSparkParam(polygon_wkt),
            depth_min_b=MockSparkParam(depth_min),
            depth_max_b=MockSparkParam(depth_max),
            tile_service_factory=setup_mock_tile_service(test_tile)
        )

        generator = matchup.match_satellite_to_insitu(**match_args)

        def validate_matchup_result(matchup_result, insitu_matchup):
            """
            The matchup results for satellite->insitu vs
            satellite->satellite are almost exactly the same so they
            can be validated using the same logic. They are the same
            because they represent the same data, except one test is in
            tile format (sat to sat) and one is in edge point format
            (insitu). The only difference is the data field is different
            for satellite data.
            """
            # There should be two primary matchup points
            assert len(matchup_result) == 2
            # Each primary point matched with 1 matchup point
            assert len(matchup_result[0]) == 2
            assert len(matchup_result[1]) == 2
            # Check that the satellite point was matched to the expected secondary point
            assert matchup_result[0][1].latitude == 3.0
            assert matchup_result[0][1].longitude == 18.0
            assert matchup_result[1][1].latitude == 15.0
            assert matchup_result[1][1].longitude == 5.0
            # Check that the secondary points have the expected values
            if insitu_matchup:
                assert matchup_result[0][1].data[0].variable_value == 30.0
                assert matchup_result[1][1].data[0].variable_value == 10.0
                assert matchup_result[0][1].data[0].variable_name == 'sea_water_temperature'
                assert matchup_result[1][1].data[0].variable_name == 'sea_water_temperature'
            else:
                assert matchup_result[0][1].data[0].variable_value == 30.0
                assert matchup_result[1][1].data[0].variable_value == 10.0
                assert matchup_result[0][1].data[0].variable_name == 'sst'
                assert matchup_result[0][1].data[0].cf_variable_name == 'sea_surface_temperature'
                assert matchup_result[1][1].data[0].variable_name == 'sst'
                assert matchup_result[1][1].data[0].cf_variable_name == 'sea_surface_temperature'
            # Check that the satellite points have the expected values
            assert matchup_result[0][0].data[0].variable_value == 21.0
            assert matchup_result[1][0].data[0].variable_value == 31.0
            assert matchup_result[0][0].data[0].variable_name == 'sst'
            assert matchup_result[0][0].data[0].cf_variable_name == 'sea_surface_temperature'
            assert matchup_result[1][0].data[0].variable_name == 'sst'
            assert matchup_result[1][0].data[0].cf_variable_name == 'sea_surface_temperature'

        insitu_matchup_result = list(generator)
        validate_matchup_result(insitu_matchup_result, insitu_matchup=True)

        # Test the satellite->satellite branch
        # By mocking the getEndpointsByName function to return None we
        # are forcing Matchup to think this dummy matchup dataset is
        # satellite dataset
        mock_edge_endpoints.return_value = None

        # Open the edge response json. We want to convert these points
        # to tile points so we can test sat to sat matchup
        edge_json = json.load(open(os.path.join(test_dir, 'edge_response.json')))
        points = [wkt.loads(result['point']) for result in edge_json['results']]

        matchup_tile = Tile()
        matchup_tile.var_names = ['sst']
        matchup_tile.standard_names = ['sea_surface_temperature']
        matchup_tile.latitudes = np.array([point.y for point in points], dtype=np.float32)
        matchup_tile.longitudes = np.array([point.x for point in points], dtype=np.float32)
        matchup_tile.times = [edge_json['results'][0]['time']]
        matchup_tile.data = np.array([[[10.0, 0, 0], [0, 20.0, 0], [0, 0, 30.0]]])
        matchup_tile.get_indices = lambda: [[0, 0, 0], [0, 1, 1], [0, 2, 2]]
        matchup_tile.is_multi = False

        match_args['tile_service_factory']().find_tiles_in_polygon.return_value = [matchup_tile]

        generator = matchup.match_satellite_to_insitu(**match_args)

        sat_matchup_result = list(generator)
        validate_matchup_result(sat_matchup_result, insitu_matchup=False)


def test_multi_variable_matchup(test_dir, test_tile, test_matchup_args):
    """
    Test multi-variable satellite to in-situ matchup functionality.
    """
    test_tile.latitudes = np.array([0, 20], dtype=np.float32)
    test_tile.longitudes = np.array([0, 20], dtype=np.float32)
    test_tile.times = [1627490285]
    test_tile.data = np.array([
        [[
            [1.10, 2.10],
            [3.10, 4.10]
        ]],
        [[
            [11.0, 21.0],
            [31.0, 41.0]
        ]]
    ])
    test_tile.is_multi = True
    test_tile.var_names = ['wind_speed', 'wind_dir']
    test_tile.standard_names = ['wind_speed', 'wind_direction']
    test_matchup_args['tile_service_factory'] = setup_mock_tile_service(test_tile)

    with mock.patch(
            'webservice.algorithms_spark.Matchup.edge_endpoints.getEndpointByName'
    ) as mock_edge_endpoints:
        # Test the satellite->insitu branch
        # By mocking the getEndpointsByName function we are forcing
        # Matchup to think this dummy matchup dataset is an insitu
        # dataset
        mock_edge_endpoints.return_value = {'url': 'http://test-edge-url'}
        matchup.query_edge = lambda *args, **kwargs: json.load(
            open(os.path.join(test_dir, 'edge_response.json')))

        generator = matchup.match_satellite_to_insitu(**test_matchup_args)

        insitu_matchup_result = list(generator)

        # wind_speed is first, wind_dir is second
        for data_dict in insitu_matchup_result[0][0].data:
            assert data_dict.variable_name in test_tile.var_names
            if data_dict.variable_name == 'wind_speed':
                assert data_dict.variable_value == 2.10
            elif data_dict.variable_name == 'wind_dir':
                assert data_dict.variable_value == 21.0
        for data_dict in insitu_matchup_result[1][0].data:
            assert data_dict.variable_name in test_tile.var_names
            if data_dict.variable_name == 'wind_speed':
                assert data_dict.variable_value == 3.10
            elif data_dict.variable_name == 'wind_dir':
                assert data_dict.variable_value == 31.0


def test_multi_variable_satellite_to_satellite_matchup(test_dir, test_tile, test_matchup_args):
    """
    Test multi-variable satellite to satellite matchup functionality.
    """
    test_tile.latitudes = np.array([0, 20], dtype=np.float32)
    test_tile.longitudes = np.array([0, 20], dtype=np.float32)
    test_tile.times = [1627490285]
    test_tile.data = np.array([
        [[
            [1.10, 2.10],
            [3.10, 4.10]
        ]],
        [[
            [11.0, 21.0],
            [31.0, 41.0]
        ]]
    ])
    test_tile.is_multi = True
    test_tile.var_names = ['wind_speed', 'wind_dir']
    test_tile.standard_names = ['wind_speed', 'wind_direction']
    test_matchup_args['tile_service_factory'] = setup_mock_tile_service(test_tile)

    with mock.patch(
            'webservice.algorithms_spark.Matchup.edge_endpoints.getEndpointByName'
    ) as mock_edge_endpoints:
        mock_edge_endpoints.return_value = None
        # Open the edge response json. We want to convert these points
        # to tile points so we can test sat to sat matchup
        edge_json = json.load(open(os.path.join(test_dir, 'edge_response.json')))
        points = [wkt.loads(result['point']) for result in edge_json['results']]

        matchup_tile = Tile()
        matchup_tile.var_names = ['sst', 'wind_dir']
        matchup_tile.standard_names = ['sea_surface_temperature', 'wind_direction']
        matchup_tile.latitudes = np.array([point.y for point in points], dtype=np.float32)
        matchup_tile.longitudes = np.array([point.x for point in points], dtype=np.float32)
        matchup_tile.times = [edge_json['results'][0]['time']]
        matchup_tile.data = np.array([
            [[
                [10.0, 0, 0],
                [0, 20.0, 0],
                [0, 0, 30.0]
            ]],
            [[
                [100.0, 0, 0],
                [0, 200.0, 0],
                [0, 0, 300.0]
            ]]
        ])
        # matchup_tile.get_indices = lambda: [[0, 0, 0], [0, 1, 1], [0, 2, 2]]
        matchup_tile.is_multi = True

        test_matchup_args['tile_service_factory']().find_tiles_in_polygon.return_value = [
            matchup_tile
        ]

        generator = matchup.match_satellite_to_insitu(**test_matchup_args)
        matchup_result = list(generator)
        assert len(matchup_result) == 2
        assert len(matchup_result[0]) == 2
        assert len(matchup_result[1]) == 2
        assert len(matchup_result[0][0].data) == 2
        assert len(matchup_result[0][1].data) == 2
        assert len(matchup_result[1][0].data) == 2
        assert len(matchup_result[1][1].data) == 2
