
import importlib
import unittest
from os import environ, path

import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array


class TestAvhrrData(unittest.TestCase):
    def setUp(self):
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

        self.module = importlib.import_module('nexusxd.kelvintocelsius')
        reload(self.module)

    def tearDown(self):
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']

    def test_kelvin_to_celsius(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'avhrr_nonempty_nexustile.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        nexus_tile_before = nexusproto.NexusTile.FromString(nexustile_str)
        sst_before = from_shaped_array(nexus_tile_before.tile.grid_tile.variable_data)

        results = list(self.module.transform(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile_after = nexusproto.NexusTile.FromString(results[0])
        sst_after = from_shaped_array(nexus_tile_after.tile.grid_tile.variable_data)

        # Just spot check a couple of values
        expected_sst = np.subtract(sst_before[0][0][0], np.float32(273.15))
        self.assertEqual(expected_sst, sst_after[0][0][0])

        expected_sst = np.subtract(sst_before[0][9][9], np.float32(273.15))
        self.assertEqual(expected_sst, sst_after[0][9][9])
