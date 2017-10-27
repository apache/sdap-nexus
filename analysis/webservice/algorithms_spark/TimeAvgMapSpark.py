"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging

import numpy as np
from nexustiles.nexustiles import NexusTileService

# from time import time
from webservice.NexusHandler import nexus_handler, SparkHandler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException


@nexus_handler
class TimeAvgMapSparkHandlerImpl(SparkHandler):
    name = "Time Average Map Spark"
    path = "/timeAvgMapSpark"
    description = "Computes a Latitude/Longitude Time Average plot given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        SparkHandler.__init__(self)
        self.log = logging.getLogger(__name__)
        # self.log.setLevel(logging.DEBUG)

    @staticmethod
    def _map(tile_in_spark):
        tile_bounds = tile_in_spark[0]
        (min_lat, max_lat, min_lon, max_lon,
         min_y, max_y, min_x, max_x) = tile_bounds
        startTime = tile_in_spark[1]
        endTime = tile_in_spark[2]
        ds = tile_in_spark[3]
        tile_service = NexusTileService()
        # print 'Started tile {0}'.format(tile_bounds)
        # sys.stdout.flush()
        tile_inbounds_shape = (max_y - min_y + 1, max_x - min_x + 1)
        # days_at_a_time = 90
        days_at_a_time = 30
        # days_at_a_time = 7
        # days_at_a_time = 1
        # print 'days_at_a_time = {0}'.format(days_at_a_time)
        t_incr = 86400 * days_at_a_time
        sum_tile = np.array(np.zeros(tile_inbounds_shape, dtype=np.float64))
        cnt_tile = np.array(np.zeros(tile_inbounds_shape, dtype=np.uint32))
        t_start = startTime
        while t_start <= endTime:
            t_end = min(t_start + t_incr, endTime)
            # t1 = time()
            # print 'nexus call start at time {0}'.format(t1)
            # sys.stdout.flush()
            # nexus_tiles = \
            #    TimeAvgMapSparkHandlerImpl.query_by_parts(tile_service,
            #                                              min_lat, max_lat, 
            #                                              min_lon, max_lon, 
            #                                              ds, 
            #                                              t_start, 
            #                                              t_end,
            #                                              part_dim=2)
            nexus_tiles = \
                tile_service.get_tiles_bounded_by_box(min_lat, max_lat,
                                                      min_lon, max_lon,
                                                      ds=ds,
                                                      start_time=t_start,
                                                      end_time=t_end)
            # t2 = time()
            # print 'nexus call end at time %f' % t2
            # print 'secs in nexus call: ', t2 - t1
            # print 't %d to %d - Got %d tiles' % (t_start, t_end,
            #                                     len(nexus_tiles))
            # for nt in nexus_tiles:
            #    print nt.granule
            #    print nt.section_spec
            #    print 'lat min/max:', np.ma.min(nt.latitudes), np.ma.max(nt.latitudes)
            #    print 'lon min/max:', np.ma.min(nt.longitudes), np.ma.max(nt.longitudes)
            # sys.stdout.flush()

            for tile in nexus_tiles:
                tile.data.data[:, :] = np.nan_to_num(tile.data.data)
                sum_tile += tile.data.data[0, min_y:max_y + 1, min_x:max_x + 1]
                cnt_tile += (~tile.data.mask[0,
                              min_y:max_y + 1,
                              min_x:max_x + 1]).astype(np.uint8)
            t_start = t_end + 1

        # print 'cnt_tile = ', cnt_tile
        # cnt_tile.mask = ~(cnt_tile.data.astype(bool))
        # sum_tile.mask = cnt_tile.mask
        # avg_tile = sum_tile / cnt_tile
        # stats_tile = [[{'avg': avg_tile.data[y,x], 'cnt': cnt_tile.data[y,x]} for x in range(tile_inbounds_shape[1])] for y in range(tile_inbounds_shape[0])]
        # print 'Finished tile', tile_bounds
        # print 'Tile avg = ', avg_tile
        # sys.stdout.flush()
        return ((min_lat, max_lat, min_lon, max_lon), (sum_tile, cnt_tile))

    def calc(self, computeOptions, **args):
        """

        :param computeOptions: StatsComputeOptions
        :param args: dict
        :return:
        """

        spark_master, spark_nexecs, spark_nparts = computeOptions.get_spark_cfg()
        self._setQueryParams(computeOptions.get_dataset()[0],
                             (float(computeOptions.get_min_lat()),
                              float(computeOptions.get_max_lat()),
                              float(computeOptions.get_min_lon()),
                              float(computeOptions.get_max_lon())),
                             computeOptions.get_start_time(),
                             computeOptions.get_end_time(),
                             spark_master=spark_master,
                             spark_nexecs=spark_nexecs,
                             spark_nparts=spark_nparts)

        if 'CLIM' in self._ds:
            raise NexusProcessingException(
                reason="Cannot compute Latitude/Longitude Time Average plot on a climatology", code=400)

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
        nlats = int((self._maxLat - self._minLatCent) / self._latRes) + 1
        nlons = int((self._maxLon - self._minLonCent) / self._lonRes) + 1
        self.log.debug('nlats={0}, nlons={1}'.format(nlats, nlons))
        self.log.debug('center lat range = {0} to {1}'.format(self._minLatCent,
                                                              self._maxLatCent))
        self.log.debug('center lon range = {0} to {1}'.format(self._minLonCent,
                                                              self._maxLonCent))

        # for tile in nexus_tiles:
        #    print 'lats: ', tile.latitudes.compressed()
        #    print 'lons: ', tile.longitudes.compressed()
        # Create array of tuples to pass to Spark map function
        nexus_tiles_spark = [[self._find_tile_bounds(t),
                              self._startTime, self._endTime,
                              self._ds] for t in nexus_tiles]
        # print 'nexus_tiles_spark = ', nexus_tiles_spark
        # Remove empty tiles (should have bounds set to None)
        bad_tile_inds = np.where([t[0] is None for t in nexus_tiles_spark])[0]
        for i in np.flipud(bad_tile_inds):
            del nexus_tiles_spark[i]

        # Expand Spark map tuple array by duplicating each entry N times,
        # where N is the number of ways we want the time dimension carved up.
        num_time_parts = 72
        # num_time_parts = 1
        nexus_tiles_spark = np.repeat(nexus_tiles_spark, num_time_parts, axis=0)
        self.log.debug('repeated len(nexus_tiles_spark) = {0}'.format(len(nexus_tiles_spark)))

        # Set the time boundaries for each of the Spark map tuples.
        # Every Nth element in the array gets the same time bounds.
        spark_part_times = np.linspace(self._startTime, self._endTime,
                                       num_time_parts + 1, dtype=np.int64)

        spark_part_time_ranges = \
            np.repeat([[[spark_part_times[i],
                         spark_part_times[i + 1]] for i in range(num_time_parts)]],
                      len(nexus_tiles_spark) / num_time_parts, axis=0).reshape((len(nexus_tiles_spark), 2))
        self.log.debug('spark_part_time_ranges={0}'.format(spark_part_time_ranges))
        nexus_tiles_spark[:, 1:3] = spark_part_time_ranges
        # print 'nexus_tiles_spark final = '
        # for i in range(len(nexus_tiles_spark)):
        #    print nexus_tiles_spark[i]

        # Launch Spark computations
        rdd = self._sc.parallelize(nexus_tiles_spark, self._spark_nparts)
        sum_count_part = rdd.map(self._map)
        sum_count = \
            sum_count_part.combineByKey(lambda val: val,
                                        lambda x, val: (x[0] + val[0],
                                                        x[1] + val[1]),
                                        lambda x, y: (x[0] + y[0], x[1] + y[1]))
        fill = self._fill
        avg_tiles = \
            sum_count.map(lambda (bounds, (sum_tile, cnt_tile)):
                          (bounds, [[{'avg': (sum_tile[y, x] / cnt_tile[y, x])
                          if (cnt_tile[y, x] > 0)
                          else fill,
                                      'cnt': cnt_tile[y, x]}
                                     for x in
                                     range(sum_tile.shape[1])]
                                    for y in
                                    range(sum_tile.shape[0])])).collect()

        # Combine subset results to produce global map.
        #
        # The tiles below are NOT Nexus objects.  They are tuples
        # with the time avg map data and lat-lon bounding box.
        a = np.zeros((nlats, nlons), dtype=np.float64, order='C')
        n = np.zeros((nlats, nlons), dtype=np.uint32, order='C')
        for tile in avg_tiles:
            if tile is not None:
                ((tile_min_lat, tile_max_lat, tile_min_lon, tile_max_lon),
                 tile_stats) = tile
                tile_data = np.ma.array(
                    [[tile_stats[y][x]['avg'] for x in range(len(tile_stats[0]))] for y in range(len(tile_stats))])
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
        self._create_nc_file(a, 'tam.nc', 'val', fill=self._fill)

        # Create dict for JSON response
        results = [[{'avg': a[y, x], 'cnt': int(n[y, x]),
                     'lat': self._ind2lat(y), 'lon': self._ind2lon(x)}
                    for x in range(a.shape[1])] for y in range(a.shape[0])]

        return TimeAvgMapSparkResults(results=results, meta={}, computeOptions=computeOptions)


class TimeAvgMapSparkResults(NexusResults):
    def __init__(self, results=None, meta=None, computeOptions=None):
        NexusResults.__init__(self, results=results, meta=meta, stats=None, computeOptions=computeOptions)
