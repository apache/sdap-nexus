
import importlib
import unittest
from os import environ, path

import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array


class TestAscatbUData(unittest.TestCase):
    def setUp(self):
        environ['U_OR_V'] = 'U'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

        self.module = importlib.import_module('nexusxd.winddirspeedtouv')
        reload(self.module)

    def tearDown(self):
        del environ['U_OR_V']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']

    def test_u_conversion(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        results = list(self.module.transform(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile = nexusproto.NexusTile.FromString(results[0])

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('swath_tile'))

        # Check data
        tile_data = np.ma.masked_invalid(from_shaped_array(nexus_tile.tile.swath_tile.variable_data))
        self.assertEquals(82, np.ma.count(tile_data))

        # Check meta data
        meta_list = nexus_tile.tile.swath_tile.meta_data
        self.assertEquals(3, len(meta_list))
        wind_dir = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_dir')
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_dir.meta_data)).shape)
        self.assertIsNotNone(wind_dir)
        wind_speed = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_speed')
        self.assertIsNotNone(wind_speed)
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_speed.meta_data)).shape)
        wind_v = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_v')
        self.assertIsNotNone(wind_v)
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_v.meta_data)).shape)

class TestAscatbVData(unittest.TestCase):
    def setUp(self):
        environ['U_OR_V'] = 'V'
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

        self.module = importlib.import_module('nexusxd.winddirspeedtouv')
        reload(self.module)

    def tearDown(self):
        del environ['U_OR_V']
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']

    def test_u_conversion(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        results = list(self.module.transform(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile = nexusproto.NexusTile.FromString(results[0])

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('swath_tile'))

        # Check data
        tile_data = np.ma.masked_invalid(from_shaped_array(nexus_tile.tile.swath_tile.variable_data))
        self.assertEquals(82, np.ma.count(tile_data))

        # Check meta data
        meta_list = nexus_tile.tile.swath_tile.meta_data
        self.assertEquals(3, len(meta_list))
        wind_dir = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_dir')
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_dir.meta_data)).shape)
        self.assertIsNotNone(wind_dir)
        wind_speed = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_speed')
        self.assertIsNotNone(wind_speed)
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_speed.meta_data)).shape)
        wind_u = next(meta_obj for meta_obj in meta_list if meta_obj.name == 'wind_u')
        self.assertIsNotNone(wind_u)
        self.assertEquals(tile_data.shape, np.ma.masked_invalid(from_shaped_array(wind_u.meta_data)).shape)


if __name__ == '__main__':
    unittest.main()
