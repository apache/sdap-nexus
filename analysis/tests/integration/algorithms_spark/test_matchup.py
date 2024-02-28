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

import pytest
from shapely import wkt
from analysis.webservice.algorithms_spark.Matchup import *


@pytest.mark.integration
class TestMatchup(unittest.TestCase):
    def setUp(self):
        from os import environ
        environ['PYSPARK_DRIVER_PYTHON'] = '/Users/greguska/anaconda/envs/nexus-analysis/bin/python2.7'
        environ['PYSPARK_PYTHON'] = '/Users/greguska/anaconda/envs/nexus-analysis/bin/python2.7'
        environ['SPARK_HOME'] = '/Users/greguska/sandbox/spark-2.0.0-bin-hadoop2.7'

    def test_mur_match(self):
        from shapely.wkt import loads
        from nexustiles.nexustiles import NexusTileService

        polygon = loads("POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))")
        primary_ds = "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"
        matchup_ds = "spurs"
        parameter = "sst"
        start_time = 1350259200  # 2012-10-15T00:00:00Z
        end_time = 1350345600  # 2012-10-16T00:00:00Z
        time_tolerance = 86400
        depth_tolerance = 5.0
        radius_tolerance = 1500.0
        platforms = "1,2,3,4,5,6,7,8,9"

        tile_service = NexusTileService()
        tile_ids = [tile.tile_id for tile in
                    tile_service.find_tiles_in_polygon(polygon, primary_ds, start_time, end_time, fetch_data=False,
                                                       fl='id')]
        result = spark_matchup_driver(tile_ids, wkt.dumps(polygon), primary_ds, matchup_ds, parameter, time_tolerance,
                                      depth_tolerance, radius_tolerance, platforms)
        for k, v in result.items():
            print("primary: %s\n\tmatches:\n\t\t%s" % (
                "lon: %s, lat: %s, time: %s, sst: %s" % (k.longitude, k.latitude, k.time, k.sst),
                '\n\t\t'.join(
                    ["lon: %s, lat: %s, time: %s, sst: %s" % (i.longitude, i.latitude, i.time, i.sst) for i in v])))

    def test_smap_match(self):
        from shapely.wkt import loads
        from nexustiles.nexustiles import NexusTileService

        polygon = loads("POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))")
        primary_ds = "SMAP_L2B_SSS"
        matchup_ds = "spurs"
        parameter = "sss"
        start_time = 1350259200  # 2012-10-15T00:00:00Z
        end_time = 1350345600  # 2012-10-16T00:00:00Z
        time_tolerance = 86400
        depth_tolerance = 5.0
        radius_tolerance = 1500.0
        platforms = "1,2,3,4,5,6,7,8,9"

        tile_service = NexusTileService()
        tile_ids = [tile.tile_id for tile in
                    tile_service.find_tiles_in_polygon(polygon, primary_ds, start_time, end_time, fetch_data=False,
                                                       fl='id')]
        result = spark_matchup_driver(tile_ids, wkt.dumps(polygon), primary_ds, matchup_ds, parameter, time_tolerance,
                                      depth_tolerance, radius_tolerance, platforms)
        for k, v in result.items():
            print("primary: %s\n\tmatches:\n\t\t%s" % (
                "lon: %s, lat: %s, time: %s, sst: %s" % (k.longitude, k.latitude, k.time, k.sst),
                '\n\t\t'.join(
                    ["lon: %s, lat: %s, time: %s, sst: %s" % (i.longitude, i.latitude, i.time, i.sst) for i in v])))

    def test_ascatb_match(self):
        from shapely.wkt import loads
        from nexustiles.nexustiles import NexusTileService

        polygon = loads("POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))")
        primary_ds = "ASCATB-L2-Coastal"
        matchup_ds = "spurs"
        parameter = "wind"
        start_time = 1351468800  # 2012-10-29T00:00:00Z
        end_time = 1351555200  # 2012-10-30T00:00:00Z
        time_tolerance = 86400
        depth_tolerance = 5.0
        radius_tolerance = 110000.0  # 110 km
        platforms = "1,2,3,4,5,6,7,8,9"

        tile_service = NexusTileService()
        tile_ids = [tile.tile_id for tile in
                    tile_service.find_tiles_in_polygon(polygon, primary_ds, start_time, end_time, fetch_data=False,
                                                       fl='id')]
        result = spark_matchup_driver(tile_ids, wkt.dumps(polygon), primary_ds, matchup_ds, parameter, time_tolerance,
                                      depth_tolerance, radius_tolerance, platforms)
        for k, v in result.items():
            print("primary: %s\n\tmatches:\n\t\t%s" % (
                "lon: %s, lat: %s, time: %s, wind u,v: %s,%s" % (k.longitude, k.latitude, k.time, k.wind_u, k.wind_v),
                '\n\t\t'.join(
                    ["lon: %s, lat: %s, time: %s, wind u,v: %s,%s" % (
                        i.longitude, i.latitude, i.time, i.wind_u, i.wind_v) for i in v])))
