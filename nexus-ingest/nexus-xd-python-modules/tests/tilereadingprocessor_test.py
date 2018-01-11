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
import importlib
import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from os import environ, path
from nexusproto.serialization import from_shaped_array


class TestSummaryData(unittest.TestCase):
    def setUp(self):
        environ['READER'] = 'GRIDTILE'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'
        environ['VARIABLE'] = 'analysed_sst'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['LATITUDE'] = 'lat'

        self.module = importlib.import_module('nexusxd.tilereadingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['READER']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['VARIABLE']
        del environ['LONGITUDE']
        del environ['TIME']
        del environ['LATITUDE']

    def test_summary_exists(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'empty_mur.nc4')

        results = list(self.module.read_grid_data(None,
                                                  "time:0:1,lat:0:10,lon:0:10;time:0:1,lat:10:20,lon:0:10;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('summary'))

    def test_section_spec_set(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'empty_mur.nc4')

        results = list(self.module.read_grid_data(None,
                                                  "time:0:1,lat:0:10,lon:0:10;time:0:1,lat:10:20,lon:0:10;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        # Tests for the first tile
        self.assertEquals('time:0:1,lat:0:10,lon:0:10', results[0].summary.section_spec)

        # Tests for the second tile
        self.assertEquals('time:0:1,lat:10:20,lon:0:10', results[1].summary.section_spec)

    def test_granule_set(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'empty_mur.nc4')

        results = list(self.module.read_grid_data(None,
                                                  "time:0:1,lat:0:10,lon:0:10;time:0:1,lat:10:20,lon:0:10;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertEquals('empty_mur.nc4', nexus_tile.summary.granule)


class TestReadMurData(unittest.TestCase):
    def setUp(self):
        environ['READER'] = 'GRIDTILE'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'
        environ['VARIABLE'] = 'analysed_sst'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['LATITUDE'] = 'lat'

        self.module = importlib.import_module('nexusxd.tilereadingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['READER']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['VARIABLE']
        del environ['LONGITUDE']
        del environ['TIME']
        del environ['LATITUDE']

    def test_read_empty_mur(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'empty_mur.nc4')

        results = list(self.module.read_grid_data(None,
                                                  "time:0:1,lat:0:10,lon:0:10;time:0:1,lat:10:20,lon:0:10;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('tile'))
            self.assertTrue(nexus_tile.tile.HasField('grid_tile'))

            tile = nexus_tile.tile.grid_tile
            self.assertEquals(10, len(from_shaped_array(tile.latitude)))
            self.assertEquals(10, len(from_shaped_array(tile.longitude)))

            the_data = np.ma.masked_invalid(from_shaped_array(tile.variable_data))
            self.assertEquals((1, 10, 10), the_data.shape)
            self.assertEquals(0, np.ma.count(the_data))
            self.assertTrue(tile.HasField('time'))

    def test_read_not_empty_mur(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_mur.nc4')

        results = list(self.module.read_grid_data(None,
                                                  "time:0:1,lat:0:10,lon:0:10;time:0:1,lat:10:20,lon:0:10;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.variable_data))
        self.assertEquals((1, 10, 10), tile1_data.shape)
        self.assertEquals(100, np.ma.count(tile1_data))

        tile2_data = np.ma.masked_invalid(from_shaped_array(results[1].tile.grid_tile.variable_data))
        self.assertEquals((1, 10, 10), tile2_data.shape)
        self.assertEquals(100, np.ma.count(tile2_data))

        self.assertFalse(np.allclose(tile1_data, tile2_data, equal_nan=True), "Both tiles contain identical data")


class TestReadAscatbData(unittest.TestCase):
    def setUp(self):
        environ['READER'] = 'SWATHTILE'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'
        environ['VARIABLE'] = 'wind_speed'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['LATITUDE'] = 'lat'

        self.module = importlib.import_module('nexusxd.tilereadingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['READER']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['VARIABLE']
        del environ['LONGITUDE']
        del environ['TIME']
        del environ['LATITUDE']
        try:
            del environ['META']
        except KeyError:
            pass

    # for data in read_swath_data(None,
    #                       "NUMROWS:0:1,NUMCELLS:0:5;NUMROWS:1:2,NUMCELLS:0:5;file:///Users/greguska/data/ascat/ascat_20130314_004801_metopb_02520_eps_o_coa_2101_ovw.l2.nc"):
    #     import sys
    #     from struct import pack
    #     sys.stdout.write(pack("!L", len(data)) + data)

    # VARIABLE=wind_speed,LATITUDE=lat,LONGITUDE=lon,TIME=time,META=wind_dir,READER=SWATHTILE,TEMP_DIR=/tmp
    def test_read_not_empty_ascatb(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_ascatb.nc4')

        results = list(self.module.read_swath_data(None,
                                                   "NUMROWS:0:1,NUMCELLS:0:82;NUMROWS:1:2,NUMCELLS:0:82;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('tile'))
            self.assertTrue(nexus_tile.tile.HasField('swath_tile'))
            self.assertEquals(0, len(nexus_tile.tile.swath_tile.meta_data))

            tile = nexus_tile.tile.swath_tile
            self.assertEquals(82, from_shaped_array(tile.latitude).size)
            self.assertEquals(82, from_shaped_array(tile.longitude).size)

        tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.swath_tile.variable_data))
        self.assertEquals((1, 82), tile1_data.shape)
        self.assertEquals(82, np.ma.count(tile1_data))

        tile2_data = np.ma.masked_invalid(from_shaped_array(results[1].tile.swath_tile.variable_data))
        self.assertEquals((1, 82), tile2_data.shape)
        self.assertEquals(82, np.ma.count(tile2_data))

        self.assertFalse(np.allclose(tile1_data, tile2_data, equal_nan=True), "Both tiles contain identical data")

    def test_read_not_empty_ascatb_meta(self):
        environ['META'] = 'wind_dir'
        reload(self.module)

        # with open('./ascat_longitude_more_than_180.bin', 'w') as f:
        #     results = list(self.module.read_swath_data(None,
        #                                                "NUMROWS:0:1,NUMCELLS:0:82;NUMROWS:1:2,NUMCELLS:0:82;file:///Users/greguska/Downloads/ascat_longitude_more_than_180.nc4"))
        #     f.write(results[0])

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_ascatb.nc4')

        results = list(self.module.read_swath_data(None,
                                                   "NUMROWS:0:1,NUMCELLS:0:82;NUMROWS:1:2,NUMCELLS:0:82;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('tile'))
            self.assertTrue(nexus_tile.tile.HasField('swath_tile'))
            self.assertLess(0, len(nexus_tile.tile.swath_tile.meta_data))

        self.assertEquals(1, len(results[0].tile.swath_tile.meta_data))
        tile1_meta_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.swath_tile.meta_data[0].meta_data))
        self.assertEquals((1, 82), tile1_meta_data.shape)
        self.assertEquals(82, np.ma.count(tile1_meta_data))

        self.assertEquals(1, len(results[1].tile.swath_tile.meta_data))
        tile2_meta_data = np.ma.masked_invalid(from_shaped_array(results[1].tile.swath_tile.meta_data[0].meta_data))
        self.assertEquals((1, 82), tile2_meta_data.shape)
        self.assertEquals(82, np.ma.count(tile2_meta_data))

        self.assertFalse(np.allclose(tile1_meta_data, tile2_meta_data, equal_nan=True),
                         "Both tiles' meta contain identical data")


class TestReadSmapData(unittest.TestCase):
    def setUp(self):
        environ['READER'] = 'SWATHTILE'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'
        environ['VARIABLE'] = 'smap_sss'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'row_time'
        environ['LATITUDE'] = 'lat'
        # REV_START_TIME = "2015-090T16:31:44.000"
        environ['GLBLATTR_DAY'] = 'REV_START_TIME'
        environ['GLBLATTR_DAY_FORMAT'] = '%Y-%jT%H:%M:%S.%f'

        self.module = importlib.import_module('nexusxd.tilereadingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['READER']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['VARIABLE']
        del environ['LONGITUDE']
        del environ['TIME']
        del environ['LATITUDE']
        del environ['GLBLATTR_DAY']
        del environ['GLBLATTR_DAY_FORMAT']
        try:
            del environ['META']
        except KeyError:
            pass

    def test_read_not_empty_smap(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_smap.h5')

        results = list(self.module.read_swath_data(None,
                                                   "phony_dim_0:0:76,phony_dim_1:0:1;phony_dim_0:0:76,phony_dim_1:1:2;file://%s" % test_file))

        self.assertEquals(2, len(results))

        # with open('./smap_nonempty_nexustile.bin', 'w') as f:
        #     f.write(results[0])

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('tile'))
            self.assertTrue(nexus_tile.tile.HasField('swath_tile'))
            self.assertEquals(0, len(nexus_tile.tile.swath_tile.meta_data))

            tile = nexus_tile.tile.swath_tile
            self.assertEquals(76, from_shaped_array(tile.latitude).size)
            self.assertEquals(76, from_shaped_array(tile.longitude).size)

        tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.swath_tile.variable_data))
        self.assertEquals((76, 1), tile1_data.shape)
        self.assertEquals(43, np.ma.count(tile1_data))
        self.assertAlmostEquals(-50.056,
                                np.ma.min(np.ma.masked_invalid(from_shaped_array(results[0].tile.swath_tile.latitude))),
                                places=3)
        self.assertAlmostEquals(-47.949,
                                np.ma.max(np.ma.masked_invalid(from_shaped_array(results[0].tile.swath_tile.latitude))),
                                places=3)

        tile2_data = np.ma.masked_invalid(from_shaped_array(results[1].tile.swath_tile.variable_data))
        self.assertEquals((76, 1), tile2_data.shape)
        self.assertEquals(43, np.ma.count(tile2_data))

        self.assertFalse(np.allclose(tile1_data, tile2_data, equal_nan=True), "Both tiles contain identical data")

        self.assertEquals(1427820162, np.ma.masked_invalid(from_shaped_array(results[0].tile.swath_tile.time))[0])


class TestReadCcmpData(unittest.TestCase):
    def setUp(self):
        environ['READER'] = 'GRIDTILE'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'
        environ['VARIABLE'] = 'uwnd'
        environ['LATITUDE'] = 'latitude'
        environ['LONGITUDE'] = 'longitude'
        environ['TIME'] = 'time'
        environ['META'] = 'vwnd'

        self.module = importlib.import_module('nexusxd.tilereadingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['READER']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['VARIABLE']
        del environ['LATITUDE']
        del environ['LONGITUDE']
        del environ['TIME']
        del environ['META']

    def test_read_not_empty_ccmp(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_ccmp.nc')

        results = list(self.module.read_grid_data(None,
                                                  "time:0:1,longitude:0:87,latitude:0:38;time:1:2,longitude:0:87,latitude:0:38;time:2:3,longitude:0:87,latitude:0:38;time:3:4,longitude:0:87,latitude:0:38;file://%s" % test_file))

        self.assertEquals(4, len(results))

        # with open('./ccmp_nonempty_nexustile.bin', 'w') as f:
        #     f.write(results[0])

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('tile'))
            self.assertTrue(nexus_tile.tile.HasField('grid_tile'))
            self.assertEquals(1, len(nexus_tile.tile.grid_tile.meta_data))

            tile = nexus_tile.tile.grid_tile
            self.assertEquals(38, from_shaped_array(tile.latitude).size)
            self.assertEquals(87, from_shaped_array(tile.longitude).size)
            self.assertEquals((1, 38, 87), from_shaped_array(tile.variable_data).shape)

        tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.variable_data))
        self.assertEquals(3306, np.ma.count(tile1_data))
        self.assertAlmostEquals(-78.375,
                                np.ma.min(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
                                places=3)
        self.assertAlmostEquals(-69.125,
                                np.ma.max(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
                                places=3)

        self.assertEquals(1451606400, results[0].tile.grid_tile.time)


class TestReadAvhrrData(unittest.TestCase):
    def setUp(self):
        environ['READER'] = 'GRIDTILE'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'
        environ['VARIABLE'] = 'analysed_sst'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['LATITUDE'] = 'lat'

        self.module = importlib.import_module('nexusxd.tilereadingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['READER']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['VARIABLE']
        del environ['LONGITUDE']
        del environ['TIME']
        del environ['LATITUDE']

    def test_read_not_empty_avhrr(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_avhrr.nc4')

        results = list(self.module.read_grid_data(None,
                                                  "time:0:1,lat:0:10,lon:0:10;file://%s" % test_file))

        self.assertEquals(1, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('tile'))
            self.assertTrue(nexus_tile.tile.HasField('grid_tile'))

            tile = nexus_tile.tile.grid_tile
            self.assertEquals(10, from_shaped_array(tile.latitude).size)
            self.assertEquals(10, from_shaped_array(tile.longitude).size)
            self.assertEquals((1, 10, 10), from_shaped_array(tile.variable_data).shape)

        tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.variable_data))
        self.assertEquals(100, np.ma.count(tile1_data))
        self.assertAlmostEquals(-39.875,
                                np.ma.min(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
                                places=3)
        self.assertAlmostEquals(-37.625,
                                np.ma.max(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
                                places=3)

        self.assertEquals(1462060800, results[0].tile.grid_tile.time)
        self.assertAlmostEquals(289.71,
                                np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.variable_data))[0, 0, 0],
                                places=3)

class TestReadWSWMData(unittest.TestCase):
    def setUp(self):
        environ['READER'] = 'TIMESERIES'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'
        environ['VARIABLE'] = 'Qout'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['LATITUDE'] = 'lat'

        self.module = importlib.import_module('nexusxd.tilereadingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['READER']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['VARIABLE']
        del environ['LONGITUDE']
        del environ['TIME']
        del environ['LATITUDE']

    def test_read_not_empty_wswm(self):
        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_wswm.nc')

        results = list(self.module.read_time_series_data(None,
                                                  "time:0:1,rivid:0:500;time:0:1,rivid:500:1000;file://%s" % test_file))

        self.assertEquals(2, len(results))

        results = [nexusproto.NexusTile.FromString(nexus_tile_data) for nexus_tile_data in results]

        for nexus_tile in results:
            self.assertTrue(nexus_tile.HasField('tile'))
            self.assertTrue(nexus_tile.tile.HasField('time_series_tile'))

            tile = nexus_tile.tile.time_series_tile
            self.assertEquals(500, from_shaped_array(tile.latitude).size)
            self.assertEquals(500, from_shaped_array(tile.longitude).size)
            self.assertEquals((1, 500), from_shaped_array(tile.variable_data).shape)

        tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.variable_data))
        self.assertEquals(500, np.ma.count(tile1_data))
        self.assertAlmostEquals(41.390,
                                np.ma.min(np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.latitude))),
                                places=3)
        self.assertAlmostEquals(42.071,
                                np.ma.max(np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.latitude))),
                                places=3)

        self.assertEquals(852098400L, results[0].tile.time_series_tile.time)
        self.assertAlmostEquals(0.009,
                                np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.variable_data))[0, 0],
                                places=3)


if __name__ == '__main__':
    unittest.main()
