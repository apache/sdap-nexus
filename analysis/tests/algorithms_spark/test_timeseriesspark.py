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
from datetime import datetime
import mock
import numpy as np
from pytz import timezone as tz
import itertools
from random import uniform

import pytest
import analysis.webservice.algorithms_spark.TimeSeriesSpark as timeseries
from nexustiles.model.nexusmodel import Tile
from pyspark.sql import SparkSession
from shapely import wkt
from shapely.geometry import box
from analysis.webservice.algorithms_spark.TimeSeriesSpark import TimeSeriesSparkHandlerImpl

class MockSparkParam:
    def __init__(self, value):
        self.value = value

@pytest.fixture(scope='function')
def timeseries_args():
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


def generate_monthly_stats():
    """
    Generates dummy monthly stats for one year
    """
    times = [1577836800, 1580515200, 1583020800, 1585699200, 1588291200, 1590969600,
    1593561600, 1596240000, 1598918400, 1601510400, 1604188800, 1606780800]

    stats_arr = [[]]
    for time in times:
        vals = [uniform(0,100) for i in range(843)]
        stats_arr[0].append({
            'min': min(vals),
            'max': max(vals),
            'mean': np.mean(vals),
            'cnt': 843,
            'std': np.std(vals),
            'time': int(time),
            'iso_time': datetime.utcfromtimestamp(int(time)).replace(tzinfo=tz('UTC')).strftime('%Y-%m-%dT%H:%M:%S%z')
        })

    clim_stats = {datetime.utcfromtimestamp(result['time']).month: result for result in stats_arr[0]}

    return stats_arr, clim_stats

def generate_daily_stats():
    """
    Generates dummy daily stats for one year
    """
    times = [1577836800 + (i * 86400) for i in range(0,366)]
    
    stats_arr = [[]]
    for time in times:
        vals = [uniform(0,100) for i in range(843)]
        stats_arr[0].append({
            'min': np.min(vals),
            'max': np.max(vals),
            'mean': np.mean(vals),
            'cnt': 843,
            'std': np.std(vals),
            'time': int(time),
            'iso_time': datetime.utcfromtimestamp(int(time)).replace(tzinfo=tz('UTC')).strftime('%Y-%m-%dT%H:%M:%S%z')
        })

    clim_stats = {datetime.utcfromtimestamp(result['time']).month: result for result in stats_arr[0]}

    return stats_arr, clim_stats


def test_calc_average_on_day():
    """
    Tests the calc_average_on_day function in TimeSeriesSpark.py
    """
    timestamps = [1578096000, 1578096001, 1578096002]
    data = [1,2,3]

    test_tile = Tile(
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

    test_tile.data = np.ma.MaskedArray(data, [0, 0, 0])
    test_tile.times = timestamps
    test_tile.latitudes = [45]
    test_tile.longitudes = [60, 60, 60]

    def setup_mock_tile_service(tile):
        tile_service_factory = mock.MagicMock()
        tile_service = mock.MagicMock()
        tile_service_factory.return_value = tile_service
        tile_service.get_bounding_box.return_value = box(-90, -45, 90, 45)
        tile_service.get_min_time.return_value = 1627490285
        tile_service.get_max_time.return_value = 1627490285
        tile_service.get_tiles_bounded_by_box.return_value = [test_tile]
        tile_service.mask_tiles_to_polygon.return_value = [test_tile]
        return tile_service_factory

    def callback(calculation):
        """
        Dummy function used for metrics callback
        """
        pass

    # Spark tile format: (polygon string, ds name, list of time stamps, fill value)
    spark_tile = (wkt.loads('POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'), 
                    'dataset', timestamps, -9999.)

    avg_args = dict(
        tile_service_factory = setup_mock_tile_service(test_tile),
        metrics_callback = callback,
        normalize_dates = False,
        tile_in_spark = spark_tile
    )
    stats = timeseries.calc_average_on_day(**avg_args)

    assert stats[0][0]['min'] == np.min(data)
    assert stats[0][0]['max'] == np.max(data)
    assert stats[0][0]['mean'] == np.mean(data)
    assert stats[0][0]['cnt'] == len(data)
    assert stats[0][0]['std'] == np.std(data)
    assert stats[0][0]['time'] == timestamps[0]
    assert stats[0][0]['iso_time'] == datetime.utcfromtimestamp(int(timestamps[0])).replace(tzinfo=tz('UTC')).strftime('%Y-%m-%dT%H:%M:%S%z')
    

def test_calc(timeseries_args):
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

    # Generate the dummy data
    stats_arr, clim_stats = generate_monthly_stats()

    # Dummy bounding polygon
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'

    # Patch in request arguments
    args = {
        'ds': ['test-dataset'],
        'bounding_polygon': wkt.loads(polygon_wkt),
        'start_seconds_from_epoch': stats_arr[0][0]['time'],
        'end_seconds_from_epoch': stats_arr[0][-1]['time'],
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
    # Format of results: [[{stats}, {stats},...]]
    # Format of meta: {}
    fake_spark_result = list(itertools.chain.from_iterable(stats_arr))
    fake_spark_result = sorted(fake_spark_result, key=lambda entry: entry["time"]), {}

    timeseries_obj = TimeSeriesSparkHandlerImpl(tile_service_factory=tile_service_factory, sc=spark_context)
    timeseries_obj.parse_arguments = lambda _: [item for item in args.values()]

    # Mock the spark driver with our fake spark results
    # Also mock _create_nc_file_time1d to avoid writing the .nc file 
    with mock.patch('webservice.algorithms_spark.TimeSeriesSpark.spark_driver') as mock_driver, \
        mock.patch('webservice.algorithms_spark.NexusCalcSparkHandler.NexusCalcSparkHandler._create_nc_file_time1d') as mock_create_nc:
        mock_driver.return_value = fake_spark_result
        timeseries_result = timeseries_obj.calc(request)
        # Ensure the call to 'spark_driver' contains the expected params
        assert len(mock_driver.call_args_list) == 1

        # Check args
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

        # Meta
        assert len(json_timeseries_result['meta']) == 1
        assert json_timeseries_result['meta'][0]['shortName'] == args['ds'][0]
        assert json_timeseries_result['meta'][0]['bounds']['north'] == 31
        assert json_timeseries_result['meta'][0]['bounds']['east'] == -30.1
        assert json_timeseries_result['meta'][0]['bounds']['south'] == 29.54
        assert json_timeseries_result['meta'][0]['bounds']['west'] == -34.98
        assert json_timeseries_result['meta'][0]['time']['start'] == stats_arr[0][0]['time']
        assert json_timeseries_result['meta'][0]['time']['stop'] == stats_arr[0][-1]['time']

        # Data
        for result_data, stats_data in zip(json_timeseries_result['data'][0], stats_arr[0]):
            assert result_data['min'] == stats_data['min']
            assert result_data['max'] == stats_data['max']
            assert result_data['mean'] == stats_data['mean']
            assert result_data['cnt'] == stats_data['cnt']
            assert result_data['std'] == stats_data['std']
            assert result_data['time'] == stats_data['time']
            assert result_data['iso_time'] == stats_data['iso_time']

        # Stats
        assert json_timeseries_result['stats'] == {}


"""
Seasonal filter = true works as follows:
- Performs normal calculations on data
- Performs calculations on clim data
    - Indexes results by month
    - If there aren't results for each month it raises exception
- Calculates and stores min, max, mean seasonal values
    - The way we mock the services means the clim data is the same as the
        first day of the non-clim data.
"""
def test_calc_seasonal_filter(timeseries_args):
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

    # Generate the dummy data
    stats_arr, clim_stats = generate_monthly_stats()

    # Dummy bounding polygon
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'
    
    # Patch in request arguments
    args = {
        'ds': ['test-dataset'],
        'bounding_polygon': wkt.loads(polygon_wkt),
        'start_seconds_from_epoch': stats_arr[0][0]['time'],
        'end_seconds_from_epoch': stats_arr[0][-1]['time'],
        'apply_seasonal_cycle_filter': True,
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
    # Format of results: [[{stats}, {stats}, ...]]
    # Format of meta: {}
    fake_spark_result = list(itertools.chain.from_iterable(stats_arr))
    fake_spark_result = sorted(fake_spark_result, key=lambda entry: entry["time"]), {}

    timeseries_obj = TimeSeriesSparkHandlerImpl(tile_service_factory=tile_service_factory, sc=spark_context)
    timeseries_obj.parse_arguments = lambda _: [item for item in args.values()]

    # Mock the spark driver with our fake spark results
    # Also mock _create_nc_file_time1d to avoid writing the .nc file 
    with mock.patch('webservice.algorithms_spark.TimeSeriesSpark.spark_driver') as mock_driver, \
        mock.patch('webservice.algorithms_spark.NexusCalcSparkHandler.NexusCalcSparkHandler._create_nc_file_time1d') as mock_create_nc:
        mock_driver.return_value = fake_spark_result
        timeseries_result = timeseries_obj.calc(request)
        
        # Ensure the call to 'spark_driver' contains the expected params

        # Seasonal filter uses clim, so call_args_list is of length 2
        assert len(mock_driver.call_args_list) == 2

        # Check data args
        mock_driver_args = mock_driver.call_args_list[0].args
        mock_driver_kwargs = mock_driver.call_args_list[0].kwargs    
        assert mock_driver_args[0] == [f.min_time[0]+1 for f in fake_tiles]
        assert mock_driver_args[1] == wkt.loads(polygon_wkt)
        assert mock_driver_args[2] == args['ds'][0]
        assert mock_driver_args[3] == tile_service_factory
        assert mock_driver_args[5] == args['normalize_dates']
        assert mock_driver_kwargs['sc'] == spark_context

        # And clim args
        mock_driver_args = mock_driver.call_args_list[1].args
        mock_driver_kwargs = mock_driver.call_args_list[1].kwargs    
        assert mock_driver_args[0] == [f.min_time[0]+1 for f in fake_tiles]
        assert mock_driver_args[1] == wkt.loads(polygon_wkt)
        assert mock_driver_args[2] == args['ds'][0] + '_clim'
        assert mock_driver_args[3] == tile_service_factory
        assert mock_driver_kwargs['sc'] == spark_context

        # Ensure the result of the timeseries is as expected
        json_timeseries_result = json.loads(timeseries_result.toJson())

        # Meta
        assert len(json_timeseries_result['meta']) == 1
        assert json_timeseries_result['meta'][0]['shortName'] == args['ds'][0]
        assert json_timeseries_result['meta'][0]['bounds']['north'] == 31
        assert json_timeseries_result['meta'][0]['bounds']['east'] == -30.1
        assert json_timeseries_result['meta'][0]['bounds']['south'] == 29.54
        assert json_timeseries_result['meta'][0]['bounds']['west'] == -34.98
        assert json_timeseries_result['meta'][0]['time']['start'] == stats_arr[0][0]['time']
        assert json_timeseries_result['meta'][0]['time']['stop'] == stats_arr[0][-1]['time']

        # Data
        for result_data, stats_data in zip(json_timeseries_result['data'][0], stats_arr[0]):
            month = datetime.utcfromtimestamp(result_data['time']).month

            assert result_data['min'] == stats_data['min']
            assert result_data['max'] == stats_data['max']
            assert result_data['mean'] == stats_data['mean']
            assert result_data['cnt'] == stats_data['cnt']
            assert result_data['std'] == stats_data['std']
            assert result_data['time'] == stats_data['time']
            assert result_data['iso_time'] == stats_data['iso_time']
            # Clim uses the first of the month values
            assert result_data['minSeasonal'] == result_data['min'] - clim_stats[month]['min']
            assert result_data['maxSeasonal'] == result_data['max'] - clim_stats[month]['max']
            assert result_data['meanSeasonal'] == result_data['mean'] - clim_stats[month]['mean']

        # Stats
        assert json_timeseries_result['stats'] == {}

def test_calc_seasonal_filter_daily(timeseries_args):
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

    # Generate the dummy data
    stats_arr, clim_stats = generate_daily_stats()

    # Dummy bounding polygon
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'

    # Patch in request arguments
    args = {
        'ds': ['test-dataset'],
        'bounding_polygon': wkt.loads(polygon_wkt),
        'start_seconds_from_epoch': stats_arr[0][0]['time'],
        'end_seconds_from_epoch': stats_arr[0][-1]['time'],
        'apply_seasonal_cycle_filter': True,
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
    fake_spark_result = list(itertools.chain.from_iterable(stats_arr))
    fake_spark_result = sorted(fake_spark_result, key=lambda entry: entry["time"]), {}

    timeseries_obj = TimeSeriesSparkHandlerImpl(tile_service_factory=tile_service_factory, sc=spark_context)
    timeseries_obj.parse_arguments = lambda _: [item for item in args.values()]

    # Mock the spark driver with our fake spark results
    # Also mock _create_nc_file_time1d to avoid writing the .nc file 
    with mock.patch('webservice.algorithms_spark.TimeSeriesSpark.spark_driver') as mock_driver, \
        mock.patch('webservice.algorithms_spark.NexusCalcSparkHandler.NexusCalcSparkHandler._create_nc_file_time1d') as mock_create_nc:
        mock_driver.return_value = fake_spark_result
        timeseries_result = timeseries_obj.calc(request)

        # Ensure the call to 'spark_driver' contains the expected params

        # Seasonal filter uses clim, so call_args_list is of length 2
        assert len(mock_driver.call_args_list) == 2

        # Check data args
        mock_driver_args = mock_driver.call_args_list[0].args
        mock_driver_kwargs = mock_driver.call_args_list[0].kwargs    
        assert mock_driver_args[0] == [f.min_time[0]+1 for f in fake_tiles]
        assert mock_driver_args[1] == wkt.loads(polygon_wkt)
        assert mock_driver_args[2] == args['ds'][0]
        assert mock_driver_args[3] == tile_service_factory
        assert mock_driver_args[5] == args['normalize_dates']
        assert mock_driver_kwargs['sc'] == spark_context

        # And clim args
        mock_driver_args = mock_driver.call_args_list[1].args
        mock_driver_kwargs = mock_driver.call_args_list[1].kwargs    
        assert mock_driver_args[0] == [f.min_time[0]+1 for f in fake_tiles]
        assert mock_driver_args[1] == wkt.loads(polygon_wkt)
        assert mock_driver_args[2] == args['ds'][0] + '_clim'
        assert mock_driver_args[3] == tile_service_factory
        assert mock_driver_kwargs['sc'] == spark_context

        # Ensure the result of the timeseries is as expected
        json_timeseries_result = json.loads(timeseries_result.toJson())

        # Meta
        assert len(json_timeseries_result['meta']) == 1
        assert json_timeseries_result['meta'][0]['shortName'] == args['ds'][0]
        assert json_timeseries_result['meta'][0]['bounds']['north'] == 31
        assert json_timeseries_result['meta'][0]['bounds']['east'] == -30.1
        assert json_timeseries_result['meta'][0]['bounds']['south'] == 29.54
        assert json_timeseries_result['meta'][0]['bounds']['west'] == -34.98
        assert json_timeseries_result['meta'][0]['time']['start'] == stats_arr[0][0]['time']
        assert json_timeseries_result['meta'][0]['time']['stop'] == stats_arr[0][-1]['time']

        # Data
        for result_data, stats_data in zip(json_timeseries_result['data'][0], stats_arr[0]):
            month = datetime.utcfromtimestamp(result_data['time']).month

            assert result_data['min'] == stats_data['min']
            assert result_data['max'] == stats_data['max']
            assert result_data['mean'] == stats_data['mean']
            assert result_data['cnt'] == stats_data['cnt']
            assert result_data['std'] == stats_data['std']
            assert result_data['time'] == stats_data['time']
            assert result_data['iso_time'] == stats_data['iso_time']
            assert result_data['minSeasonal'] == result_data['min'] - clim_stats[month]['min']
            assert result_data['maxSeasonal'] == result_data['max'] - clim_stats[month]['max']
            assert result_data['meanSeasonal'] == result_data['mean'] - clim_stats[month]['mean']

        # Stats
        assert json_timeseries_result['stats'] == {}

def test_calc_seasonal_lowpass_filter(timeseries_args):
    """
    Lowpass assertions are commented out for now - will resume work as needed.
    """
    # Mock anything that connects external dependence (Solr, Cassandra, ...)
    tile_service_factory = mock.MagicMock()
    tile_service = mock.MagicMock()
    tile_service_factory.return_value = tile_service
    spark = SparkSession.builder.appName('nexus-analysis').getOrCreate()
    spark_context = spark.sparkContext
    request = mock.MagicMock()
    request.get_argument.return_value = '1,2,3,4,5,6,7,8'

    # Generate the dummy data
    stats_arr, clim_stats = generate_monthly_stats()

    # Dummy bounding polygon
    polygon_wkt = 'POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))'
    
    # Patch in request arguments
    args = {
        'ds': ['test-dataset'],
        'bounding_polygon': wkt.loads(polygon_wkt),
        'start_seconds_from_epoch': stats_arr[0][0]['time'],
        'end_seconds_from_epoch': stats_arr[0][-1]['time'],
        'apply_seasonal_cycle_filter': True,
        'apply_low_pass_filter': True,
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
    # Format of results: [[{stats}, {stats}, ...]]
    # Format of meta: {}
    fake_spark_result = list(itertools.chain.from_iterable(stats_arr))
    fake_spark_result = sorted(fake_spark_result, key=lambda entry: entry["time"]), {}

    timeseries_obj = TimeSeriesSparkHandlerImpl(tile_service_factory=tile_service_factory, sc=spark_context)
    timeseries_obj.parse_arguments = lambda _: [item for item in args.values()]

    # Mock the spark driver with our fake spark results
    # Also mock _create_nc_file_time1d to avoid writing the .nc file 
    with mock.patch('webservice.algorithms_spark.TimeSeriesSpark.spark_driver') as mock_driver, \
        mock.patch('webservice.algorithms_spark.NexusCalcSparkHandler.NexusCalcSparkHandler._create_nc_file_time1d') as mock_create_nc:
        mock_driver.return_value = fake_spark_result
        timeseries_result = timeseries_obj.calc(request)
        
        # Ensure the call to 'spark_driver' contains the expected params

        # Seasonal filter uses clim, so call_args_list is of length 2
        assert len(mock_driver.call_args_list) == 2

        # Check data args
        mock_driver_args = mock_driver.call_args_list[0].args
        mock_driver_kwargs = mock_driver.call_args_list[0].kwargs    
        assert mock_driver_args[0] == [f.min_time[0]+1 for f in fake_tiles]
        assert mock_driver_args[1] == wkt.loads(polygon_wkt)
        assert mock_driver_args[2] == args['ds'][0]
        assert mock_driver_args[3] == tile_service_factory
        assert mock_driver_args[5] == args['normalize_dates']
        assert mock_driver_kwargs['sc'] == spark_context

        # And clim args
        mock_driver_args = mock_driver.call_args_list[1].args
        mock_driver_kwargs = mock_driver.call_args_list[1].kwargs    
        assert mock_driver_args[0] == [f.min_time[0]+1 for f in fake_tiles]
        assert mock_driver_args[1] == wkt.loads(polygon_wkt)
        assert mock_driver_args[2] == args['ds'][0] + '_clim'
        assert mock_driver_args[3] == tile_service_factory
        assert mock_driver_kwargs['sc'] == spark_context

        # Ensure the result of the timeseries is as expected
        json_timeseries_result = json.loads(timeseries_result.toJson())

        # Meta
        assert len(json_timeseries_result['meta']) == 1
        assert json_timeseries_result['meta'][0]['shortName'] == args['ds'][0]
        assert json_timeseries_result['meta'][0]['bounds']['north'] == 31
        assert json_timeseries_result['meta'][0]['bounds']['east'] == -30.1
        assert json_timeseries_result['meta'][0]['bounds']['south'] == 29.54
        assert json_timeseries_result['meta'][0]['bounds']['west'] == -34.98
        assert json_timeseries_result['meta'][0]['time']['start'] == stats_arr[0][0]['time']
        assert json_timeseries_result['meta'][0]['time']['stop'] == stats_arr[0][-1]['time']

        # Data
        for result_data, stats_data in zip(json_timeseries_result['data'][0], stats_arr[0]):
            month = datetime.utcfromtimestamp(result_data['time']).month

            assert result_data['min'] == stats_data['min']
            assert result_data['max'] == stats_data['max']
            assert result_data['mean'] == stats_data['mean']
            assert result_data['cnt'] == stats_data['cnt']
            assert result_data['std'] == stats_data['std']
            assert result_data['time'] == stats_data['time']
            assert result_data['iso_time'] == stats_data['iso_time']
            # Clim uses the first of the month values, which is the same as the data
            assert result_data['minSeasonal'] == result_data['min'] - clim_stats[month]['min']
            assert result_data['maxSeasonal'] == result_data['max'] - clim_stats[month]['max']
            assert result_data['meanSeasonal'] == result_data['mean'] - clim_stats[month]['mean']

            # Low pass data
            # TODO: need to determine expected low pass results
            # assert result_data['minLowPass']  result_data['min']
            # assert result_data['maxLowPass']  result_data['max']
            # assert result_data['meanLowPass']  result_data['mean']
            # assert result_data['minSeasonalLowPass']  result_data['minSeasonal']
            # assert result_data['maxSeasonalLowPass']  result_data['maxSeasonal']
            # assert result_data['meanSeasonalLowPass']  result_data['meanSeasonal']

        # Stats
        assert json_timeseries_result['stats'] == {}
