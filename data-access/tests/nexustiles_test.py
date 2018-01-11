
import ConfigParser
import time
import unittest
from StringIO import StringIO

from nexustiles.nexustiles import NexusTileService
from shapely.geometry import box


class TestService(unittest.TestCase):
    def setUp(self):
        config = StringIO("""[cassandra]
host=127.0.0.1
keyspace=nexustiles
local_datacenter=datacenter1
protocol_version=3
port=32769

[solr]
host=localhost:8986
core=nexustiles""")
        cp = ConfigParser.RawConfigParser()
        cp.readfp(config)

        self.tile_service = NexusTileService(config=cp)

    def test_get_distinct_bounding_boxes_in_polygon(self):
        boxes = self.tile_service.get_distinct_bounding_boxes_in_polygon(box(-180, -90, 180, 90),
                                                                         "MXLDEPTH_ECCO_version4_release1",
                                                                         1, time.time())
        for b in boxes:
            print b.bounds

    def test_get_distinct_bounding_boxes_in_polygon_mur(self):
        boxes = self.tile_service.get_distinct_bounding_boxes_in_polygon(box(-180, -90, 180, 90),
                                                                         "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1",
                                                                         1, time.time())
        for b in boxes:
            print b.bounds

    def test_find_tiles_by_exact_bounds(self):
        tiles = self.tile_service.find_tiles_by_exact_bounds((175.01, -42.68, 180.0, -40.2),
                                                             "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1",
                                                             1, time.time())
        for tile in tiles:
            print tile.get_summary()

    def test_sorted_box(self):

        tiles = self.tile_service.get_tiles_bounded_by_box(-42.68, -40.2, 175.01, 180.0,
                                                   "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1",
                                                   1, time.time())
        for tile in tiles:
            print tile.min_time


# from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon
# import numpy as np
#
# service = NexusTileService()

# assert service is not None

# tiles = service.find_tiles_in_box(-90, 90, -180, 180, ds='AVHRR_OI_L4_GHRSST_NCEI')
#
# print '\n'.join([str(tile.data.shape) for tile in tiles])

# ASCATB
# tiles = service.find_tile_by_id('43c63dce-1f6e-3c09-a7b2-e0efeb3a72f2')
# MUR
# tiles = service.find_tile_by_id('d9b5afe3-bd7f-3824-ad8a-d8d3b364689c')
# SMAP
# tiles = service.find_tile_by_id('7eee40ef-4c6e-32d8-9a67-c83d4183f724')
# tile = tiles[0]
#
# print get_approximate_value_for_lat_lon([tile], np.min(tile.latitudes), np.min(tile.longitudes) + .005)
# print tile.latitudes
# print tile.longitudes
# print tile.data
# tile
# print type(tile.data)
#
# assert len(tiles) == 1
#
# tile = tiles[0]
# assert tile.meta_data is not None
#
# print tile.get_summary()
