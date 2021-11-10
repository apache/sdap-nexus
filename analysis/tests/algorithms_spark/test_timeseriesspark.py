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
from datetime import datetime, timezone
import mock
import numpy as np
from pytz import timezone as tz
import itertools

import pytest
import webservice.algorithms_spark.TimeSeriesSpark as timeseries
from nexustiles.model.nexusmodel import Tile, TileVariable
from pyspark.sql import SparkSession
from shapely import wkt
from shapely.geometry import box
from webservice.algorithms_spark.TimeSeriesSpark import TimeSeriesSparkHandlerImpl, TimeSeriesResults, calc_average_on_day
from webservice.metrics.MetricsRecord import MetricsRecord

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
        min_time='2020-01-15T00:00:00',
        max_time='2020-01-15T00:00:00',
        section_spec='2020-01-15T00:00:00',
        meta_data={},
        is_multi=True
    )

@pytest.fixture(scope='function')
def test_timeseries_args():
    tile_ids = [1]
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'
    ds_name = 'test-dataset'
    start = '2020-01-15T00:00:00Z'
    end = '2020-01-15T00:00:00Z'
    seasonal = False
    low_pass = False


    yield dict(
        tile_ids=tile_ids,
        ds=MockSparkParam(ds_name),
        start=MockSparkParam(start),
        end=MockSparkParam(end),
        seasonal=MockSparkParam(seasonal),
        low_pass=MockSparkParam(low_pass),
        bounding_wkt=MockSparkParam(polygon_wkt),
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




def test_calc(test_timeseries_args):
    """
    Assert that the expected functions are called during the timeseries
    calculation and that the results are formatted as expected.
    """
    # Mock anything that connects external dependence (Solr, Cassandra, ...)
    tile_service_factory = mock.MagicMock()
    tile_service = mock.MagicMock()
    tile_service_factory.return_value = tile_service
    spark = SparkSession.builder.appName('nexus-analysis').getOrCreate()
    spark_context = spark.sparkContext
    request = mock.MagicMock()
    request.get_argument.return_value = '1,2,3,4,5,6,7,8'

    # Patch in request arguments
    start_time = datetime.strptime('2020-01-01T00:00:00', '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=timezone.utc)
    end_time = datetime.strptime('2020-02-01T00:00:00', '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=timezone.utc)
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'
    args = {
        'ds': ['test-dataset'],
        'bounding_polygon': wkt.loads(polygon_wkt),
        'start_seconds_from_epoch': start_time.timestamp(),
        'end_seconds_from_epoch': end_time.timestamp(),
        'apply_seasonal_cycle_filter': False,
        'apply_low_pass_filter': False,
        'nparts_requested': 1,
        'normalize_dates': False,
    }

    def generate_fake_tile(tile_id):
        tile = Tile()
        tile.tile_id = tile_id
        tile.min_time = 1627490285,
        return tile

    # Mock tiles
    fake_tiles = [generate_fake_tile(idx) for idx in range(10)]
    tile_service.find_days_in_range_asc.return_value = [f.min_time[0]+1 for f in fake_tiles]

    # Mock result
    # 'spark_driver' returns results, meta. 
    # Format of results: [[{stats}, {stats}]]
    # Format of meta: {}

    stats_arr = [
        [
            {
                'min': 0,
                'max': 100,
                'mean': 50,
                'cnt': 10,
                'std': 15,
                'time': int(1578836800),
                'iso_time': datetime.utcfromtimestamp(int(1578836800)).replace(tzinfo=tz('UTC')).strftime('%Y-%m-%dT%H:%M:%S%z')
            },
            {
                'min': 0,
                'max': 100,
                'mean': 50,
                'cnt': 10,
                'std': 15,
                'time': int(1579836800),
                'iso_time': datetime.utcfromtimestamp(int(1579836800)).replace(tzinfo=tz('UTC')).strftime('%Y-%m-%dT%H:%M:%S%z')
            }
        ]
    ]

    fake_spark_result = list(itertools.chain.from_iterable(stats_arr))
    fake_spark_result = sorted(fake_spark_result, key=lambda entry: entry["time"]), {}

    timeseries_obj = TimeSeriesSparkHandlerImpl(tile_service_factory=tile_service_factory, sc=spark_context)
    timeseries_obj.parse_arguments = lambda _: [item for item in args.values()]

    with mock.patch('webservice.algorithms_spark.TimeSeriesSpark.spark_driver') as mock_driver:
        mock_driver.return_value = fake_spark_result
        timeseries_result = timeseries_obj.calc(request)
        # Ensure the call to 'spark_driver' contains the expected params
        assert len(mock_driver.call_args_list) == 1
        mock_driver_args = mock_driver.call_args_list[0].args
        mock_driver_kwargs = mock_driver.call_args_list[0].kwargs    
        assert mock_driver_args[0] == [f.min_time[0]+1 for f in fake_tiles]
        assert mock_driver_args[1] == wkt.loads(polygon_wkt)
        assert mock_driver_args[2] == args['ds'][0]
        assert mock_driver_args[3] == tile_service_factory
        assert mock_driver_args[5] == args['normalize_dates']
        assert mock_driver_kwargs['sc'] == spark_context

        # Ensure the result of the timeseries is as expected
        json_timeseries_result = json.loads(timeseries_result.toJson())
        assert len(json_timeseries_result['meta']) == 1
        assert json_timeseries_result['meta'][0]['shortName'] == args['ds'][0]
        assert json_timeseries_result['meta'][0]['bounds']['north'] == 31
        assert json_timeseries_result['meta'][0]['bounds']['east'] == -30.1
        assert json_timeseries_result['meta'][0]['bounds']['south'] == 29.54
        assert json_timeseries_result['meta'][0]['bounds']['west'] == -34.98
        for data in json_timeseries_result['data']:
            assert data[0]['min'] == 0
            assert data[0]['max'] == 100
            assert data[0]['mean'] == 50
        assert json_timeseries_result['stats'] == {}
