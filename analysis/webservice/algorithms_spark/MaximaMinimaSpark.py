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


from datetime import datetime
from functools import partial

import numpy as np
import shapely.geometry
from pytz import timezone

from webservice.NexusHandler import nexus_handler
from webservice.algorithms_spark.NexusCalcSparkHandler import NexusCalcSparkHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


@nexus_handler
class MaximaMinimaSparkHandlerImpl(NexusCalcSparkHandler):
    name = "Maxima and Minima Map Spark"
    path = "/maxMinMapSpark"
    description = "Computes a map of maxmima and minima of a field given an arbitrary geographical area and time range"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "String",
            "description": "The dataset used to generate the map. Required"
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required"
        },
        "spark": {
            "name": "Spark Configuration",
            "type": "comma-delimited value",
            "description": "Configuration used to launch in the Spark cluster. Value should be 3 elements separated by "
                           "commas. 1) Spark Master 2) Number of Spark Executors 3) Number of Spark Partitions. Only "
                           "Number of Spark Partitions is used by this function. Optional (Default: local,1,1)"
        }
    }

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")

        try:
            ds = request.get_dataset()
            if type(ds) == list or type(ds) == tuple:
                ds = next(iter(ds))
        except:
            raise NexusProcessingException(
                reason="'ds' argument is required. Must be a string",
                code=400)

        # Do not allow time series on Climatology
        if next(iter([clim for clim in ds if 'CLIM' in clim]), False):
            raise NexusProcessingException(
                reason="Cannot compute Latitude/Longitude Time Average plot on a climatology", code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
            request.get_min_lon = lambda: bounding_polygon.bounds[0]
            request.get_min_lat = lambda: bounding_polygon.bounds[1]
            request.get_max_lon = lambda: bounding_polygon.bounds[2]
            request.get_max_lat = lambda: bounding_polygon.bounds[3]
        except:
            try:
                west, south, east, north = request.get_min_lon(), request.get_min_lat(), \
                                           request.get_max_lon(), request.get_max_lat()
                bounding_polygon = shapely.geometry.Polygon(
                    [(west, south), (east, south), (east, north), (west, north), (west, south)])
            except:
                raise NexusProcessingException(
                    reason="'b' argument is required. Must be comma-delimited float formatted as "
                           "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                    code=400)

        try:
            start_time = request.get_start_datetime()
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or "
                       "string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or "
                       "string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        if start_time > end_time:
            raise NexusProcessingException(
                reason="The starting time must be before the ending time. Received startTime: %s, endTime: %s" % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        nparts_requested = request.get_nparts()

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        return ds, bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch, nparts_requested


    def calc(self, compute_options, **args):
        """

        :param compute_options: StatsComputeOptions
        :param args: dict
        :return:
        """

        ds, bbox, start_time, end_time, nparts_requested = self.parse_arguments(compute_options)
        self._setQueryParams(ds,
                             (float(bbox.bounds[1]),
                              float(bbox.bounds[3]),
                              float(bbox.bounds[0]),
                              float(bbox.bounds[2])),
                             start_time,
                             end_time)

        # for single timestamp, get bounds of all tiles
        nexus_tiles = self._find_global_tile_set()

        if len(nexus_tiles) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        self.log.debug('Found {0} tiles'.format(len(nexus_tiles)))
        print('Found {} tiles'.format(len(nexus_tiles)))

        daysinrange = self._tile_service.find_days_in_range_asc(bbox.bounds[1],
                                                                bbox.bounds[3],
                                                                bbox.bounds[0],
                                                                bbox.bounds[2],
                                                                ds,
                                                                start_time,
                                                                end_time)
        ndays = len(daysinrange)
        if ndays == 0:
            raise NoDataException(reason="No data found for selected timeframe")
        self.log.debug('Found {0} days in range'.format(ndays))
        for i, d in enumerate(daysinrange):
            self.log.debug('{0}, {1}'.format(i, datetime.utcfromtimestamp(d)))


        self.log.debug('Using Native resolution: lat_res={0}, lon_res={1}'.format(self._latRes, self._lonRes))
        self.log.debug('nlats={0}, nlons={1}'.format(self._nlats, self._nlons))
        self.log.debug('center lat range = {0} to {1}'.format(self._minLatCent,
                                                              self._maxLatCent))
        self.log.debug('center lon range = {0} to {1}'.format(self._minLonCent,
                                                              self._maxLonCent))

        # Create array of tuples to pass to Spark map function
        nexus_tiles_spark = [[self._find_tile_bounds(t),
                              self._startTime, self._endTime,
                              self._ds] for t in nexus_tiles]

        # Remove empty tiles (should have bounds set to None)
        bad_tile_inds = np.where([t[0] is None for t in nexus_tiles_spark])[0]
        for i in np.flipud(bad_tile_inds):
            del nexus_tiles_spark[i]

        # Expand Spark map tuple array by duplicating each entry N times,
        # where N is the number of ways we want the time dimension carved up.
        # Set the time boundaries for each of the Spark map tuples so that
        # every Nth element in the array gets the same time bounds.
        max_time_parts = 72
        num_time_parts = min(max_time_parts, ndays)

        spark_part_time_ranges = np.tile(np.array([a[[0,-1]] for a in np.array_split(np.array(daysinrange), num_time_parts)]), (len(nexus_tiles_spark),1))
        nexus_tiles_spark = np.repeat(nexus_tiles_spark, num_time_parts, axis=0)
        nexus_tiles_spark[:, 1:3] = spark_part_time_ranges

        # Launch Spark computations
        spark_nparts = self._spark_nparts(nparts_requested)
        self.log.info('Using {} partitions'.format(spark_nparts))

        rdd = self._sc.parallelize(nexus_tiles_spark, spark_nparts)
        max_min_part = rdd.map(partial(self._map, self._tile_service_factory))
        max_min_count = \
            max_min_part.combineByKey(lambda val: val,
                                        lambda x, val: (np.maximum(x[0], val[0]),   # Max
                                                        np.minimum(x[1], val[1]),   # Min
                                                        np.maximum(x[2], val[2]),  # Absolute Max
                                                        np.minimum(x[3], val[3]),  # Absolute Min
                                                        (x[4] + val[4])),           # Count
                                        lambda x, y: (np.maximum(x[0], y[0]),   # Max
                                                      np.minimum(x[1], y[1]),   # Min
                                                      np.maximum(x[2], y[2]),   # Absolute Max
                                                      np.minimum(x[3], y[3]),   # Absolute Min
                                                      (x[4] + y[4])))           # Count
        fill = self._fill
        avg_tiles = \
            max_min_count.map(lambda (bounds, (max_tile, min_tile, abs_max_tile, abs_min_tile, cnt_tile)):
                              (bounds, [[{'maxima': max_tile[y, x] if (cnt_tile[y, x] > 0) else fill,
                                          'minima': min_tile[y, x] if (cnt_tile[y, x] > 0) else fill,
                                          'absolute_maxima': abs_max_tile[y, x] if (cnt_tile[y, x] > 0) else fill,
                                          'absolute_minima': abs_min_tile[y, x] if (cnt_tile[y, x] > 0) else fill,
                                          'cnt': cnt_tile[y, x]}
                                         for x in range(max_tile.shape[1])]
                                        for y in range(max_tile.shape[0])])).collect()

        # Combine subset results to produce global map.
        #
        # The tiles below are NOT Nexus objects.  They are tuples
        # with the time avg map data and lat-lon bounding box.
        a = np.zeros((self._nlats, self._nlons, 4), dtype=np.float64, order='C')
        n = np.zeros((self._nlats, self._nlons), dtype=np.uint32, order='C')
        for tile in avg_tiles:
            if tile is not None:
                ((tile_min_lat, tile_max_lat, tile_min_lon, tile_max_lon),
                 tile_stats) = tile
                # need to arrange max/min data in tuple to pull out later - can this step be skipped?
                tile_data = np.ma.array(
                    [[(tile_stats[y][x]['maxima'], tile_stats[y][x]['minima'], tile_stats[y][x]['absolute_maxima'], tile_stats[y][x]['absolute_minima']) for x in range(len(tile_stats[0]))] for y in range(len(tile_stats))])
                tile_cnt = np.array(
                    [[tile_stats[y][x]['cnt'] for x in range(len(tile_stats[0]))] for y in range(len(tile_stats))])
                tile_data.mask = ~(tile_cnt.astype(bool))
                y0 = self._lat2ind(tile_min_lat)
                y1 = y0 + tile_data.shape[0] - 1
                x0 = self._lon2ind(tile_min_lon)
                x1 = x0 + tile_data.shape[1] - 1
                if np.any(np.logical_not(tile_data.mask)):
                    self.log.debug(
                        'writing tile lat {0}-{1}, lon {2}-{3}, map y {4}-{5}, map x {6}-{7}'.format(tile_min_lat,
                                                                                                     tile_max_lat,
                                                                                                     tile_min_lon,
                                                                                                     tile_max_lon, y0,
                                                                                                     y1, x0, x1))
                    a[y0:y1 + 1, x0:x1 + 1] = tile_data
                    n[y0:y1 + 1, x0:x1 + 1] = tile_cnt
                else:
                    self.log.debug(
                        'All pixels masked in tile lat {0}-{1}, lon {2}-{3}, map y {4}-{5}, map x {6}-{7}'.format(
                            tile_min_lat, tile_max_lat,
                            tile_min_lon, tile_max_lon,
                            y0, y1, x0, x1))

        # Store global map in a NetCDF file.
        # self._create_nc_file(a, 'tam.nc', 'val', fill=self._fill)

        # Create dict for JSON response
        results = [[{'maxima': a[y, x, 0], 'minima': a[y, x, 1], 'absolute_maxima': a[y, x, 2], 'absolute_minima': a[y, x, 3], 'cnt': int(n[y, x]),
                     'lat': self._ind2lat(y), 'lon': self._ind2lon(x)}
                    for x in range(a.shape[1])] for y in range(a.shape[0])]

        return NexusResults(results=results, meta={}, stats=None,
                            computeOptions=None, minLat=bbox.bounds[1],
                            maxLat=bbox.bounds[3], minLon=bbox.bounds[0],
                            maxLon=bbox.bounds[2], ds=ds, startTime=start_time,
                            endTime=end_time)

    # this operates on only one nexus tile bound over time. Can assume all nexus_tiles are the same shape
    @staticmethod
    def _map(tile_service_factory, tile_in_spark):
        # tile_in_spark is a spatial tile that corresponds to nexus tiles of the same area
        tile_bounds = tile_in_spark[0]
        (min_lat, max_lat, min_lon, max_lon,
         min_y, max_y, min_x, max_x) = tile_bounds
        startTime = tile_in_spark[1]
        endTime = tile_in_spark[2]
        ds = tile_in_spark[3]
        tile_service = tile_service_factory()

        tile_inbounds_shape = (max_y - min_y + 1, max_x - min_x + 1)

        # hardcorded - limiting the amount of nexus tiles pulled at a time
        days_at_a_time = 30

        t_incr = 86400 * days_at_a_time
        min_tile = np.array(np.ones(tile_inbounds_shape, dtype=np.float64)) * 1e30
        max_tile = np.array(np.ones(tile_inbounds_shape, dtype=np.float64)) * -1e30

        abs_min_tile = np.array(np.ones(tile_inbounds_shape, dtype=np.float64)) * 1e30
        abs_max_tile = np.array(np.ones(tile_inbounds_shape, dtype=np.float64)) * -1e30

        cnt_tile = np.array(np.zeros(tile_inbounds_shape, dtype=np.uint32))
        t_start = startTime
        while t_start <= endTime:
            t_end = min(t_start + t_incr, endTime)

            nexus_tiles = \
                tile_service.get_tiles_bounded_by_box(min_lat, max_lat,
                                                      min_lon, max_lon,
                                                      ds=ds,
                                                      start_time=t_start,
                                                      end_time=t_end)

            for tile in nexus_tiles:
                # Take max and min of the data - just used the masked arrays, no extra .data
                min_tile = np.ma.minimum(min_tile, tile.data[0, min_y:max_y + 1, min_x:max_x + 1])
                max_tile = np.ma.maximum(max_tile, tile.data[0, min_y:max_y + 1, min_x:max_x + 1])

                abs_min_tile = np.ma.minimum(abs_min_tile, np.absolute(tile.data[0, min_y:max_y + 1, min_x:max_x + 1]))
                abs_max_tile = np.ma.maximum(abs_max_tile, np.absolute(tile.data[0, min_y:max_y + 1, min_x:max_x + 1]))

                # Taking the opposite of the value of the bool of mask - add 0 if it's a masked value
                cnt_tile += (~tile.data.mask[0, min_y:max_y + 1, min_x:max_x + 1]).astype(np.uint8)
            t_start = t_end + 1

        return (min_lat, max_lat, min_lon, max_lon), \
               (max_tile.data, min_tile.data, abs_max_tile.data, abs_min_tile.data, cnt_tile)

