"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import importlib
import unittest
from os import environ, path

import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array


class TestConversion(unittest.TestCase):
    def setUp(self):
        environ['WIND_U'] = 'uwnd'
        environ['WIND_V'] = 'vwnd'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

        self.module = importlib.import_module('nexusxd.computespeeddirfromuv')
        reload(self.module)

    def tearDown(self):
        del environ['WIND_U']
        del environ['WIND_V']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']

    def test_dir_from_north(self):
        # Negative v means wind is wind blowing to the South
        u = 0
        v = -1

        speed, direction = self.module.calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from North (0 degrees)
        self.assertEqual(0, direction)

    def test_dir_from_east(self):
        # Negative u means wind is blowing to the West
        u = -1
        v = 0

        speed, direction = self.module.calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from East (90 degrees)
        self.assertEqual(90, direction)

    def test_dir_from_south(self):
        # Positive v means wind is blowing to the North
        u = 0
        v = 1

        speed, direction = self.module.calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from South (180 degrees)
        self.assertEqual(180, direction)

    def test_dir_from_west(self):
        # Positive u means wind is blowing to the East
        u = 1
        v = 0

        speed, direction = self.module.calculate_speed_direction(u, v)

        # Degrees are where the wind is blowing from (relative to true North)
        self.assertEqual(1, speed)
        # Wind from West (270 degrees)
        self.assertEqual(270, direction)

    def test_speed(self):
        # Speed is simply sqrt(u^2 + v^2)
        u = 2
        v = 2

        speed, direction = self.module.calculate_speed_direction(u, v)

        self.assertAlmostEqual(2.8284271, speed)
        # Wind should be from the southwest
        self.assertTrue(180 < direction < 270)


class TestCcmpData(unittest.TestCase):
    def setUp(self):
        environ['WIND_U'] = 'uwnd'
        environ['WIND_V'] = 'vwnd'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

        self.module = importlib.import_module('nexusxd.computespeeddirfromuv')
        reload(self.module)

    def tearDown(self):
        del environ['WIND_U']
        del environ['WIND_V']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']

    def test_speed_dir_computation(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ccmp_nonempty_nexustile.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        results = list(self.module.transform(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile = nexusproto.NexusTile.FromString(results[0])

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('grid_tile'))

        # Check data
        tile_data = np.ma.masked_invalid(from_shaped_array(nexus_tile.tile.grid_tile.variable_data))
        self.assertEquals(3306, np.ma.count(tile_data))

        # Check meta data
        meta_list = nexus_tile.tile.grid_tile.meta_data
        self.assertEquals(3, len(meta_list))
        wind_dir = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_dir')
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_dir.meta_data)).shape)
        self.assertIsNotNone(wind_dir)
        wind_speed = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_speed')
        self.assertIsNotNone(wind_speed)
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_speed.meta_data)).shape)
        wind_v = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'vwnd')
        self.assertIsNotNone(wind_v)
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_v.meta_data)).shape)


if __name__ == '__main__':
    unittest.main()