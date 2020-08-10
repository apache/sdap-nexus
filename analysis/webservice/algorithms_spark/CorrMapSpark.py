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
from functools import partial

import numpy as np

from webservice.NexusHandler import nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.algorithms_spark.NexusCalcSparkHandler import NexusCalcSparkHandler
from webservice.webmodel import NexusProcessingException, NexusResults, NoDataException


@nexus_handler
class CorrMapNexusSparkHandlerImpl(NexusCalcSparkHandler):
    name = "Correlation Map Spark"
    path = "/corrMapSpark"
    description = "Computes a correlation map between two datasets given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC

    @staticmethod
    def _map(tile_service_factory, tile_in):
        # Unpack input
        tile_bounds, start_time, end_time, ds = tile_in
        (min_lat, max_lat, min_lon, max_lon,
         min_y, max_y, min_x, max_x) = tile_bounds

        # Create arrays to hold intermediate results during
        # correlation coefficient calculation.
        tile_inbounds_shape = (max_y - min_y + 1, max_x - min_x + 1)
        sumx_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumy_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumxx_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumyy_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumxy_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        n_tile = np.zeros(tile_inbounds_shape, dtype=np.uint32)

        # Can only retrieve some number of days worth of data from Solr
        # at a time.  Set desired value here.
        days_at_a_time = 90
        # days_at_a_time = 30
        # days_at_a_time = 7
        # days_at_a_time = 1
        # print 'days_at_a_time = ', days_at_a_time
        t_incr = 86400 * days_at_a_time

        tile_service = tile_service_factory

        # Compute the intermediate summations needed for the Pearson 
        # Correlation Coefficient.  We use a one-pass online algorithm
        # so that not all of the data needs to be kept in memory all at once.
        t_start = start_time
        while t_start <= end_time:
            t_end = min(t_start + t_incr, end_time)
            # t1 = time()
            # print 'nexus call start at time %f' % t1
            # sys.stdout.flush()
            ds1tiles = tile_service.get_tiles_bounded_by_box(min_lat,
                                                             max_lat,
                                                             min_lon,
                                                             max_lon,
                                                             ds[0],
                                                             t_start,
                                                             t_end)
            ds2tiles = tile_service.get_tiles_bounded_by_box(min_lat,
                                                             max_lat,
                                                             min_lon,
                                                             max_lon,
                                                             ds[1],
                                                             t_start,
                                                             t_end)
            # t2 = time()
            # print 'nexus call end at time %f' % t2
            # print 'secs in nexus call: ', t2-t1
            # sys.stdout.flush()

            len1 = len(ds1tiles)
            len2 = len(ds2tiles)
            # print 't %d to %d - Got %d and %d tiles' % (t_start, t_end,
            #                                            len1, len2)
            # sys.stdout.flush()
            i1 = 0
            i2 = 0
            time1 = 0
            time2 = 0
            while i1 < len1 and i2 < len2:
                tile1 = ds1tiles[i1]
                tile2 = ds2tiles[i2]
                # print 'tile1.data = ',tile1.data
                # print 'tile2.data = ',tile2.data
                # print 'i1, i2, t1, t2 times: ', i1, i2, tile1.times[0], tile2.times[0]
                assert tile1.times[0] >= time1, 'DS1 time out of order!'
                assert tile2.times[0] >= time2, 'DS2 time out of order!'
                time1 = tile1.times[0]
                time2 = tile2.times[0]
                # print 'i1=%d,i2=%d,time1=%d,time2=%d'%(i1,i2,time1,time2)
                if time1 < time2:
                    i1 += 1
                    continue
                elif time2 < time1:
                    i2 += 1
                    continue
                assert (time1 == time2), \
                    "Mismatched tile times %d and %d" % (time1, time2)
                # print 'processing time:',time1,time2
                t1_data = tile1.data.data
                t1_mask = tile1.data.mask
                t2_data = tile2.data.data
                t2_mask = tile2.data.mask
                t1_data = np.nan_to_num(t1_data)
                t2_data = np.nan_to_num(t2_data)
                joint_mask = ((~t1_mask).astype(np.uint8) *
                              (~t2_mask).astype(np.uint8))
                # print 'joint_mask=',joint_mask
                sumx_tile += (t1_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                              joint_mask[0, min_y:max_y + 1, min_x:max_x + 1])
                # print 'sumx_tile=',sumx_tile
                sumy_tile += (t2_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                              joint_mask[0, min_y:max_y + 1, min_x:max_x + 1])
                # print 'sumy_tile=',sumy_tile
                sumxx_tile += (t1_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                               t1_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                               joint_mask[0, min_y:max_y + 1, min_x:max_x + 1])
                # print 'sumxx_tile=',sumxx_tile
                sumyy_tile += (t2_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                               t2_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                               joint_mask[0, min_y:max_y + 1, min_x:max_x + 1])
                # print 'sumyy_tile=',sumyy_tile
                sumxy_tile += (t1_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                               t2_data[0, min_y:max_y + 1, min_x:max_x + 1] *
                               joint_mask[0, min_y:max_y + 1, min_x:max_x + 1])
                # print 'sumxy_tile=',sumxy_tile
                n_tile += joint_mask[0, min_y:max_y + 1, min_x:max_x + 1]
                # print 'n_tile=',n_tile
                i1 += 1
                i2 += 1
            t_start = t_end + 1

        # print 'Finished tile', tile_bounds
        # sys.stdout.flush()
        return ((min_lat, max_lat, min_lon, max_lon), (sumx_tile, sumy_tile,
                                                       sumxx_tile, sumyy_tile,
                                                       sumxy_tile, n_tile))

    def calc(self, computeOptions, **args):

        self._setQueryParams(computeOptions.get_dataset(),
                             (float(computeOptions.get_min_lat()),
                              float(computeOptions.get_max_lat()),
                              float(computeOptions.get_min_lon()),
                              float(computeOptions.get_max_lon())),
                             computeOptions.get_start_time(),
                             computeOptions.get_end_time())
        nparts_requested = computeOptions.get_nparts()

        self.log.debug('ds = {0}'.format(self._ds))
        if not len(self._ds) == 2:
            raise NexusProcessingException(
                reason="Requires two datasets for comparison. Specify request parameter ds=Dataset_1,Dataset_2",
                code=400)
        if next(iter([clim for clim in self._ds if 'CLIM' in clim]), False):
            raise NexusProcessingException(reason="Cannot compute correlation on a climatology", code=400)

        nexus_tiles = self._find_global_tile_set()
        # print 'tiles:'
        # for tile in nexus_tiles:
        #     print tile.granule
        #     print tile.section_spec
        #     print 'lat:', tile.latitudes
        #     print 'lon:', tile.longitudes

        #                                                          nexus_tiles)
        if len(nexus_tiles) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        self.log.debug('Found {0} tiles'.format(len(nexus_tiles)))
        self.log.debug('Using Native resolution: lat_res={0}, lon_res={1}'.format(self._latRes, self._lonRes))
        self.log.debug('nlats={0}, nlons={1}'.format(self._nlats, self._nlons))

        daysinrange = self._get_tile_service().find_days_in_range_asc(self._minLat,
                                                                      self._maxLat,
                                                                      self._minLon,
                                                                      self._maxLon,
                                                                      self._ds[0],
                                                                      self._startTime,
                                                                      self._endTime)
        ndays = len(daysinrange)
        if ndays == 0:
            raise NoDataException(reason="No data found for selected timeframe")
        self.log.debug('Found {0} days in range'.format(ndays))
        for i, d in enumerate(daysinrange):
            self.log.debug('{0}, {1}'.format(i, datetime.utcfromtimestamp(d)))

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

        spark_part_time_ranges = np.tile(
            np.array([a[[0, -1]] for a in np.array_split(np.array(daysinrange), num_time_parts)]),
            (len(nexus_tiles_spark), 1))
        nexus_tiles_spark = np.repeat(nexus_tiles_spark, num_time_parts, axis=0)
        nexus_tiles_spark[:, 1:3] = spark_part_time_ranges

        # Launch Spark computations
        spark_nparts = self._spark_nparts(nparts_requested)
        self.log.info('Using {} partitions'.format(spark_nparts))

        rdd = self._sc.parallelize(nexus_tiles_spark, spark_nparts)
        sum_tiles_part = rdd.map(partial(self._map, self._tile_service_factory))
        # print "sum_tiles_part = ",sum_tiles_part.collect()
        sum_tiles = \
            sum_tiles_part.combineByKey(lambda val: val,
                                        lambda x, val: (x[0] + val[0],
                                                        x[1] + val[1],
                                                        x[2] + val[2],
                                                        x[3] + val[3],
                                                        x[4] + val[4],
                                                        x[5] + val[5]),
                                        lambda x, y: (x[0] + y[0],
                                                      x[1] + y[1],
                                                      x[2] + y[2],
                                                      x[3] + y[3],
                                                      x[4] + y[4],
                                                      x[5] + y[5]))
        # Convert the N (pixel-wise count) array for each tile to be a 
        # NumPy masked array.  That is the last array in the tuple of 
        # intermediate summation arrays.  Set mask to True if count is 0.
        sum_tiles = \
            sum_tiles.map(lambda (bounds, (sum_x, sum_y, sum_xx,
            sum_yy, sum_xy, n)):
                          (bounds, (sum_x, sum_y, sum_xx, sum_yy, sum_xy,
                                    np.ma.array(n,
                                                mask=~(n.astype(bool))))))

        # print 'sum_tiles = ',sum_tiles.collect()

        # For each pixel in each tile compute an array of Pearson 
        # correlation coefficients.  The map function is called once 
        # per tile.  The result of this map operation is a list of 3-tuples of
        # (bounds, r, n) for each tile (r=Pearson correlation coefficient
        # and n=number of input values that went into each pixel with 
        # any masked values not included).
        corr_tiles = \
            sum_tiles.map(lambda (bounds, (sum_x, sum_y, sum_xx, sum_yy,
            sum_xy, n)):
                          (bounds,
                           np.ma.array(((sum_xy - sum_x * sum_y / n) /
                                        np.sqrt((sum_xx - sum_x * sum_x / n) *
                                                (sum_yy - sum_y * sum_y / n))),
                                       mask=~(n.astype(bool))),
                           n)).collect()

        r = np.zeros((self._nlats, self._nlons), dtype=np.float64, order='C')
        n = np.zeros((self._nlats, self._nlons), dtype=np.uint32, order='C')

        # The tiles below are NOT Nexus objects.  They are tuples
        # with the following for each correlation map subset:
        # (1) lat-lon bounding box, (2) array of correlation r values, 
        # and (3) array of count n values.
        for tile in corr_tiles:
            ((tile_min_lat, tile_max_lat, tile_min_lon, tile_max_lon),
             tile_data, tile_cnt) = tile
            y0 = self._lat2ind(tile_min_lat)
            y1 = self._lat2ind(tile_max_lat)
            x0 = self._lon2ind(tile_min_lon)
            x1 = self._lon2ind(tile_max_lon)
            self.log.debug(
                'writing tile lat {0}-{1}, lon {2}-{3}, map y {4}-{5}, map x {6}-{7}'.format(tile_min_lat, tile_max_lat,
                                                                                             tile_min_lon, tile_max_lon,
                                                                                             y0, y1, x0, x1))
            r[y0:y1 + 1, x0:x1 + 1] = tile_data
            n[y0:y1 + 1, x0:x1 + 1] = tile_cnt

        # Store global map in a NetCDF file.
        self._create_nc_file(r, 'corrmap.nc', 'r')

        # Create dict for JSON response
        results = [[{'r': r[y, x], 'cnt': int(n[y, x]),
                     'lat': self._ind2lat(y), 'lon': self._ind2lon(x)}
                    for x in range(r.shape[1])] for y in range(r.shape[0])]

        return CorrelationResults(results)


class CorrelationResults(NexusResults):
    def __init__(self, results):
        NexusResults.__init__(self)
        self.results = results

    def toJson(self):
        json_d = {
            "stats": {},
            "meta": [None, None],
            "data": self.results
        }
        return json.dumps(json_d, indent=4)
