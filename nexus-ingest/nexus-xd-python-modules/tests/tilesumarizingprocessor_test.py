
import importlib
import unittest
from os import environ, path

import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array


class TestSummarizeTile(unittest.TestCase):
    def setUp(self):
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

        self.module = importlib.import_module('nexusxd.tilesumarizingprocessor')
        reload(self.module)

    def tearDown(self):
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']

    def test_summarize_swath(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'smap_nonempty_nexustile.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        results = list(self.module.summarize_nexustile(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile = nexusproto.NexusTile.FromString(results[0])

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('swath_tile'))
        self.assertTrue(nexus_tile.HasField('summary'))

        # Check summary
        tile_summary = nexus_tile.summary
        self.assertAlmostEquals(-50.056, tile_summary.bbox.lat_min, places=3)
        self.assertAlmostEquals(-47.949, tile_summary.bbox.lat_max, places=3)
        self.assertAlmostEquals(22.376, tile_summary.bbox.lon_min, places=3)
        self.assertAlmostEquals(36.013, tile_summary.bbox.lon_max, places=3)

        self.assertAlmostEquals(33.067, tile_summary.stats.min, places=3)
        self.assertEquals(40, tile_summary.stats.max)
        self.assertAlmostEquals(36.6348, tile_summary.stats.mean, places=3)
        self.assertEquals(43, tile_summary.stats.count)

        self.assertEquals(1427820162, tile_summary.stats.min_time)
        self.assertEquals(1427820162, tile_summary.stats.max_time)

    def test_summarize_grid(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'avhrr_nonempty_nexustile.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        results = list(self.module.summarize_nexustile(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile = nexusproto.NexusTile.FromString(results[0])

        self.assertTrue(nexus_tile.HasField('tile'))
        self.assertTrue(nexus_tile.tile.HasField('grid_tile'))
        self.assertTrue(nexus_tile.HasField('summary'))

        # Check summary
        tile_summary = nexus_tile.summary
        self.assertAlmostEquals(-39.875, tile_summary.bbox.lat_min, places=3)
        self.assertAlmostEquals(-37.625, tile_summary.bbox.lat_max, places=3)
        self.assertAlmostEquals(-129.875, tile_summary.bbox.lon_min, places=3)
        self.assertAlmostEquals(-127.625, tile_summary.bbox.lon_max, places=3)

        self.assertAlmostEquals(288.5099, tile_summary.stats.min, places=3)
        self.assertAlmostEquals(290.4, tile_summary.stats.max, places=3)
        self.assertAlmostEquals(289.4443, tile_summary.stats.mean, places=3)
        self.assertEquals(100, tile_summary.stats.count)

        self.assertEquals(1462838400, tile_summary.stats.min_time)
        self.assertEquals(1462838400, tile_summary.stats.max_time)

if __name__ == '__main__':
    unittest.main()
