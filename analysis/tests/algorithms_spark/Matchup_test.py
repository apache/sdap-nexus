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
"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import pickle
import random
import timeit
import unittest

from webservice.algorithms_spark.Matchup import *


class TestMatch_Points(unittest.TestCase):
    def test_one_point_match_exact(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 0))

        self.assertEquals(1, len(matches))

        p_match_point, match = matches[0]

        self.assertEqual(primary, p_match_point)
        self.assertEqual(matchup, match)

    def test_one_point_match_within_tolerance_150km(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.0, latitude=3.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(1, len(matches))

        p_match_point, match = matches[0]

        self.assertEqual(primary, p_match_point)
        self.assertEqual(matchup, match)

    def test_one_point_match_within_tolerance_200m(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.001, latitude=2.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 200))  # tolerance 200 m

        self.assertEquals(1, len(matches))

        p_match_point, match = matches[0]

        self.assertEqual(primary, p_match_point)
        self.assertEqual(matchup, match)

    def test_one_point_not_match_tolerance_150km(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.0, latitude=4.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(0, len(matches))

    def test_one_point_not_match_tolerance_100m(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.001, latitude=2.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 100))  # tolerance 100 m

        self.assertEquals(0, len(matches))

    def test_multiple_point_match(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        primary_points = [primary]

        matchup_points = [
            DomsPoint(longitude=1.0, latitude=3.0, time=1000, depth=10.0, data_id=2),
            DomsPoint(longitude=2.0, latitude=2.0, time=1000, depth=0.0, data_id=3),
            DomsPoint(longitude=0.5, latitude=1.5, time=1000, depth=3.0, data_id=4)
        ]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(3, len(matches))

        self.assertSetEqual({primary}, {x[0] for x in matches})

        list_of_matches = [x[1] for x in matches]

        self.assertEquals(3, len(list_of_matches))
        self.assertItemsEqual(matchup_points, list_of_matches)

    def test_multiple_point_match_multiple_times(self):
        primary_points = [
            DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1),
            DomsPoint(longitude=1.5, latitude=1.5, time=1000, depth=5.0, data_id=2)
        ]

        matchup_points = [
            DomsPoint(longitude=1.0, latitude=3.0, time=1000, depth=10.0, data_id=3),
            DomsPoint(longitude=2.0, latitude=2.0, time=1000, depth=0.0, data_id=4),
            DomsPoint(longitude=0.5, latitude=1.5, time=1000, depth=3.0, data_id=5)
        ]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(5, len(matches))

        self.assertSetEqual({p for p in primary_points}, {x[0] for x in matches})

        # First primary point matches all 3 secondary
        self.assertEquals(3, [x[0] for x in matches].count(primary_points[0]))
        self.assertItemsEqual(matchup_points, [x[1] for x in matches if x[0] == primary_points[0]])

        # Second primary point matches only last 2 secondary
        self.assertEquals(2, [x[0] for x in matches].count(primary_points[1]))
        self.assertItemsEqual(matchup_points[1:], [x[1] for x in matches if x[0] == primary_points[1]])

    def test_one_of_many_primary_matches_one_of_many_matchup(self):
        primary_points = [
            DomsPoint(longitude=-33.76764, latitude=30.42946, time=1351553994, data_id=1),
            DomsPoint(longitude=-33.75731, latitude=29.86216, time=1351554004, data_id=2)
        ]

        matchup_points = [
            DomsPoint(longitude=-33.762, latitude=28.877, time=1351521432, depth=3.973, data_id=3),
            DomsPoint(longitude=-34.916, latitude=28.879, time=1351521770, depth=2.9798, data_id=4),
            DomsPoint(longitude=-31.121, latitude=31.256, time=1351519892, depth=4.07, data_id=5)
        ]

        matches = list(match_points_generator(primary_points, matchup_points, 110000))  # tolerance 110 km

        self.assertEquals(1, len(matches))

        self.assertSetEqual({p for p in primary_points if p.data_id == 2}, {x[0] for x in matches})

        # First primary point matches none
        self.assertEquals(0, [x[0] for x in matches].count(primary_points[0]))

        # Second primary point matches only first secondary
        self.assertEquals(1, [x[0] for x in matches].count(primary_points[1]))
        self.assertItemsEqual(matchup_points[0:1], [x[1] for x in matches if x[0] == primary_points[1]])

    @unittest.skip("This test is just for timing, doesn't actually assert anything.")
    def test_time_many_primary_many_matchup(self):
        import logging
        import sys
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
        log = logging.getLogger(__name__)
        # Generate 160000 DomsPoints distributed equally in a box from -2.0 lat/lon to 2.0 lat/lon
        log.info("Generating primary points")
        x = np.arange(-2.0, 2.0, 0.01)
        y = np.arange(-2.0, 2.0, 0.01)
        primary_points = [DomsPoint(longitude=xy[0], latitude=xy[1], time=1000, depth=5.0, data_id=i) for i, xy in
                          enumerate(np.array(np.meshgrid(x, y)).T.reshape(-1, 2))]

        # Generate 2000 DomsPoints distributed randomly in a box from -2.0 lat/lon to 2.0 lat/lon
        log.info("Generating matchup points")
        matchup_points = [
            DomsPoint(longitude=random.uniform(-2.0, 2.0), latitude=random.uniform(-2.0, 2.0), time=1000, depth=5.0,
                      data_id=i) for i in xrange(0, 2000)]

        log.info("Starting matchup")
        log.info("Best of repeat(3, 2) matchups: %s seconds" % min(
            timeit.repeat(lambda: list(match_points_generator(primary_points, matchup_points, 1500)), repeat=3,
                          number=2)))


class TestDOMSPoint(unittest.TestCase):
    def test_is_pickleable(self):
        edge_point = json.loads("""{
"id": "argo-profiles-5903995(46, 0)",
"time": "2012-10-15T14:24:04Z",
"point": "-33.467 29.728",
"sea_water_temperature": 24.5629997253,
"sea_water_temperature_depth": 2.9796258642,
"wind_speed": null,
"sea_water_salinity": null,
"sea_water_salinity_depth": null,
"platform": 4,
"device": 3,
"fileurl": "ftp://podaac-ftp.jpl.nasa.gov/allData/insitu/L2/spurs1/argo/argo-profiles-5903995.nc"
}""")
        point = DomsPoint.from_edge_point(edge_point)
        self.assertIsNotNone(pickle.dumps(point))


def check_all():
    return check_solr() and check_cass() and check_edge()


def check_solr():
    # TODO eventually this might do something.
    return False


def check_cass():
    # TODO eventually this might do something.
    return False


def check_edge():
    # TODO eventually this might do something.
    return False


@unittest.skipUnless(check_all(),
                     "These tests require local instances of Solr, Cassandra, and Edge to be running.")
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
        for k, v in result.iteritems():
            print "primary: %s\n\tmatches:\n\t\t%s" % (
                "lon: %s, lat: %s, time: %s, sst: %s" % (k.longitude, k.latitude, k.time, k.sst),
                '\n\t\t'.join(
                    ["lon: %s, lat: %s, time: %s, sst: %s" % (i.longitude, i.latitude, i.time, i.sst) for i in v]))

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
        for k, v in result.iteritems():
            print "primary: %s\n\tmatches:\n\t\t%s" % (
                "lon: %s, lat: %s, time: %s, sst: %s" % (k.longitude, k.latitude, k.time, k.sst),
                '\n\t\t'.join(
                    ["lon: %s, lat: %s, time: %s, sst: %s" % (i.longitude, i.latitude, i.time, i.sst) for i in v]))

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
        for k, v in result.iteritems():
            print "primary: %s\n\tmatches:\n\t\t%s" % (
                "lon: %s, lat: %s, time: %s, wind u,v: %s,%s" % (k.longitude, k.latitude, k.time, k.wind_u, k.wind_v),
                '\n\t\t'.join(
                    ["lon: %s, lat: %s, time: %s, wind u,v: %s,%s" % (
                        i.longitude, i.latitude, i.time, i.wind_u, i.wind_v) for i in v]))
