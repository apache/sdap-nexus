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


# distutils: include_dirs = /usr/local/lib/python2.7/site-packages/cassandra


import sys
import numpy as np
from time import time
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NoDataException
from netCDF4 import Dataset


# from mpl_toolkits.basemap import Basemap


# @nexus_handler
class TimeAvgMapCalcHandlerImpl(NexusCalcHandler):
    name = "Time Average Map"
    path = "/timeAvgMap"
    description = "Computes a Latitude/Longitude Time Average plot given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def _find_native_resolution(self):
        # Get a quick set of tiles (1 degree at center of box) at 1 time stamp
        midLat = (self._minLat + self._maxLat) / 2
        midLon = (self._minLon + self._maxLon) / 2
        ntiles = 0
        t = self._endTime
        t_incr = 86400
        while ntiles == 0:
            nexus_tiles = self._get_tile_service().get_tiles_bounded_by_box(midLat - 0.5, midLat + 0.5, midLon - 0.5,
                                                                      midLon + 0.5, ds=self._ds, start_time=t - t_incr,
                                                                      end_time=t)
            ntiles = len(nexus_tiles)
            print 'find_native_res: got %d tiles' % len(nexus_tiles)
            sys.stdout.flush()
            lat_res = 0.
            lon_res = 0.
            if ntiles > 0:
                for tile in nexus_tiles:
                    if lat_res < 1e-10:
                        lats = tile.latitudes.compressed()
                        if (len(lats) > 1):
                            lat_res = lats[1] - lats[0]
                    if lon_res < 1e-10:
                        lons = tile.longitudes.compressed()
                        if (len(lons) > 1):
                            lon_res = lons[1] - lons[0]
                    if (lat_res >= 1e-10) and (lon_res >= 1e-10):
                        break
            if (lat_res < 1e-10) or (lon_res < 1e-10):
                t -= t_incr

        self._latRes = lat_res
        self._lonRes = lon_res

    def _find_global_tile_set(self):
        ntiles = 0
        t = self._endTime
        t_incr = 86400
        while ntiles == 0:
            nexus_tiles = self._get_tile_service().get_tiles_bounded_by_box(self._minLat, self._maxLat, self._minLon,
                                                                      self._maxLon, ds=self._ds, start_time=t - t_incr,
                                                                      end_time=t)
            ntiles = len(nexus_tiles)
            print 'find_global_tile_set got %d tiles' % ntiles
            sys.stdout.flush()
            t -= t_incr
        return nexus_tiles

    def _prune_tiles(self, nexus_tiles):
        del_ind = np.where([np.all(tile.data.mask) for tile in nexus_tiles])[0]
        for i in np.flipud(del_ind):
            del nexus_tiles[i]

    # @staticmethod
    # def _map(tile_in):
    def _map(self, tile_in):
        print 'Started tile %s' % tile_in.section_spec
        print 'tile lats = ', tile_in.latitudes
        print 'tile lons = ', tile_in.longitudes
        print 'tile = ', tile_in.data
        sys.stdout.flush()
        lats = tile_in.latitudes
        lons = tile_in.longitudes
        if len(lats > 0) and len(lons > 0):
            min_lat = np.ma.min(lats)
            max_lat = np.ma.max(lats)
            min_lon = np.ma.min(lons)
            max_lon = np.ma.max(lons)
            good_inds_lat = np.where(lats.mask == False)
            good_inds_lon = np.where(lons.mask == False)
            min_y = np.min(good_inds_lat)
            max_y = np.max(good_inds_lat)
            min_x = np.min(good_inds_lon)
            max_x = np.max(good_inds_lon)
            tile_inbounds_shape = (max_y - min_y + 1, max_x - min_x + 1)
            days_at_a_time = 90
            t_incr = 86400 * days_at_a_time
            avg_tile = np.ma.array(np.zeros(tile_inbounds_shape,
                                            dtype=np.float64))
            cnt_tile = np.ma.array(np.zeros(tile_inbounds_shape,
                                            dtype=np.uint32))
            t_start = self._startTime
            while t_start <= self._endTime:
                t_end = min(t_start + t_incr, self._endTime)
                t1 = time()
                print 'nexus call start at time %f' % t1
                sys.stdout.flush()
                nexus_tiles = self._get_tile_service().get_tiles_bounded_by_box(min_lat - self._latRes / 2,
                                                                          max_lat + self._latRes / 2,
                                                                          min_lon - self._lonRes / 2,
                                                                          max_lon + self._lonRes / 2, ds=self._ds,
                                                                          start_time=t_start, end_time=t_end)
                t2 = time()
                print 'nexus call end at time %f' % t2
                print 'secs in nexus call: ', t2 - t1
                sys.stdout.flush()
                self._prune_tiles(nexus_tiles)
                print 't %d to %d - Got %d tiles' % (t_start, t_end,
                                                     len(nexus_tiles))
                sys.stdout.flush()
                for tile in nexus_tiles:
                    tile.data.data[:, :] = np.nan_to_num(tile.data.data)
                    avg_tile.data[:, :] += tile.data[0,
                                           min_y:max_y + 1,
                                           min_x:max_x + 1]
                    cnt_tile.data[:, :] += (~tile.data.mask[0,
                                             min_y:max_y + 1,
                                             min_x:max_x + 1]).astype(np.uint8)
                t_start = t_end + 1

            print 'cnt_tile = ', cnt_tile
            cnt_tile.mask = ~(cnt_tile.data.astype(bool))
            avg_tile.mask = cnt_tile.mask
            avg_tile /= cnt_tile
            print 'Finished tile %s' % tile_in.section_spec
            print 'Tile avg = ', avg_tile
            sys.stdout.flush()
        else:
            avg_tile = None
            min_lat = None
            max_lat = None
            min_lon = None
            max_lon = None
            print 'Tile %s outside of bounding box' % tile_in.section_spec
            sys.stdout.flush()
        return (avg_tile, min_lat, max_lat, min_lon, max_lon)

    def _lat2ind(self, lat):
        return int((lat - self._minLatCent) / self._latRes)

    def _lon2ind(self, lon):
        return int((lon - self._minLonCent) / self._lonRes)

    def _create_nc_file(self, a):
        print 'a=', a
        print 'shape a = ', a.shape
        sys.stdout.flush()
        lat_dim, lon_dim = a.shape
        rootgrp = Dataset("tam.nc", "w", format="NETCDF4")
        rootgrp.createDimension("lat", lat_dim)
        rootgrp.createDimension("lon", lon_dim)
        rootgrp.createVariable("TRMM_3B42_daily_precipitation_V7", "f4",
                               dimensions=("lat", "lon",))
        rootgrp.createVariable("lat", "f4", dimensions=("lat",))
        rootgrp.createVariable("lon", "f4", dimensions=("lon",))
        rootgrp.variables["TRMM_3B42_daily_precipitation_V7"][:, :] = a
        rootgrp.variables["lat"][:] = np.linspace(self._minLatCent,
                                                  self._maxLatCent, lat_dim)
        rootgrp.variables["lon"][:] = np.linspace(self._minLonCent,
                                                  self._maxLonCent, lon_dim)
        rootgrp.close()

    def calc(self, computeOptions, **args):
        """

        :param computeOptions: StatsComputeOptions
        :param args: dict
        :return:
        """

        self._minLat = float(computeOptions.get_min_lat())
        self._maxLat = float(computeOptions.get_max_lat())
        self._minLon = float(computeOptions.get_min_lon())
        self._maxLon = float(computeOptions.get_max_lon())
        self._ds = computeOptions.get_dataset()[0]
        self._startTime = computeOptions.get_start_time()
        self._endTime = computeOptions.get_end_time()

        self._find_native_resolution()
        print 'Using Native resolution: lat_res=%f, lon_res=%f' % (self._latRes, self._lonRes)
        self._minLatCent = self._minLat + self._latRes / 2
        self._minLonCent = self._minLon + self._lonRes / 2
        nlats = int((self._maxLat - self._minLatCent) / self._latRes) + 1
        nlons = int((self._maxLon - self._minLonCent) / self._lonRes) + 1
        self._maxLatCent = self._minLatCent + (nlats - 1) * self._latRes
        self._maxLonCent = self._minLonCent + (nlons - 1) * self._lonRes
        print 'nlats=', nlats, 'nlons=', nlons
        print 'center lat range = %f to %f' % (self._minLatCent,
                                               self._maxLatCent)
        print 'center lon range = %f to %f' % (self._minLonCent,
                                               self._maxLonCent)
        sys.stdout.flush()
        a = np.zeros((nlats, nlons), dtype=np.float64, order='C')

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

        print 'Initially found %d tiles' % len(nexus_tiles)
        sys.stdout.flush()
        self._prune_tiles(nexus_tiles)
        print 'Pruned to %d tiles' % len(nexus_tiles)
        sys.stdout.flush()
        # for tile in nexus_tiles:
        #    print 'lats: ', tile.latitudes.compressed()
        #    print 'lons: ', tile.longitudes.compressed()

        avg_tiles = map(self._map, nexus_tiles)
        print 'shape a = ', a.shape
        sys.stdout.flush()
        # The tiles below are NOT Nexus objects.  They are tuples
        # with the time avg map data and lat-lon bounding box.
        for tile in avg_tiles:
            if tile is not None:
                (tile_data, tile_min_lat, tile_max_lat,
                 tile_min_lon, tile_max_lon) = tile
                print 'shape tile_data = ', tile_data.shape
                print 'tile data mask = ', tile_data.mask
                sys.stdout.flush()
                if np.any(np.logical_not(tile_data.mask)):
                    y0 = self._lat2ind(tile_min_lat)
                    y1 = self._lat2ind(tile_max_lat)
                    x0 = self._lon2ind(tile_min_lon)
                    x1 = self._lon2ind(tile_max_lon)
                    print 'writing tile lat %f-%f, lon %f-%f, map y %d-%d, map x %d-%d' % \
                          (tile_min_lat, tile_max_lat,
                           tile_min_lon, tile_max_lon, y0, y1, x0, x1)
                    sys.stdout.flush()
                    a[y0:y1 + 1, x0:x1 + 1] = tile_data
                else:
                    print 'All pixels masked in tile lat %f-%f, lon %f-%f, map y %d-%d, map x %d-%d' % \
                          (tile_min_lat, tile_max_lat,
                           tile_min_lon, tile_max_lon, y0, y1, x0, x1)
                    sys.stdout.flush()

        self._create_nc_file(a)

        return TimeAvgMapResults(results={}, meta={}, computeOptions=computeOptions)


class TimeAvgMapResults(NexusResults):

    def __init__(self, results=None, meta=None, computeOptions=None):
        NexusResults.__init__(self, results=results, meta=meta, stats=None, computeOptions=computeOptions)
