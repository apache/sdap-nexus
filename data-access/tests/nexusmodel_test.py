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
import numpy as np
from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon, Tile, BBox


class TestApproximateValueMethod(unittest.TestCase):
    def test_lat_exact_lon_exact(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(22.0, get_approximate_value_for_lat_lon([tile], 1.0, 0))

    def test_lat_lon_exact_min(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(0, get_approximate_value_for_lat_lon([tile], -1.0, -2.0))

    def test_lat_approx_lon_exact(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(5.0, get_approximate_value_for_lat_lon([tile], -0.4, -2.0))

    def test_lat_approx_lon_approx(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(7.0, get_approximate_value_for_lat_lon([tile], -0.4, 0.01))

    def test_lat_exact_lon_approx(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(18.0, get_approximate_value_for_lat_lon([tile], 0.5, 1.01))

    def test_lat_greater_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], 2.0, 0)))

    def test_lat_less_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], -2.0, 0)))

    def test_lon_greater_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], 0, 3)))

    def test_lon_less_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], 0, -3)))

    def test_repeated_lats(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, -0.5, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        # -0.5   [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(6.0, get_approximate_value_for_lat_lon([tile], -0.4, -1))

    def test_multiple_tiles(self):
        tile1 = Tile()
        tile1.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile1.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile1.times = np.ma.array([0])
        tile1.data = np.ma.arange(12).reshape((1, 4, 3))

        tile2 = Tile()
        tile2.latitudes = np.ma.array([4.0, 5.0, 6.0, 7.0])
        tile2.longitudes = np.ma.array([-3.0, -4.0, -5.0])
        tile2.times = np.ma.array([0])
        tile2.data = np.ma.arange(12, 24).reshape((1, 4, 3))

        self.assertAlmostEqual(1, get_approximate_value_for_lat_lon([tile1, tile2], 0.4, -1))

    def test_multiple_tiles_same_long(self):
        tile1 = Tile()
        tile1.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile1.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile1.times = np.ma.array([0])
        tile1.data = np.ma.arange(12).reshape((1, 4, 3))

        tile2 = Tile()
        tile2.latitudes = np.ma.array([4.0, 5.0, 6.0, 7.0])
        tile2.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile2.times = np.ma.array([0])
        tile2.data = np.ma.arange(12, 24).reshape((1, 4, 3))

        self.assertAlmostEqual(1, get_approximate_value_for_lat_lon([tile1, tile2], 0.4, -1))


class TestTileContainsMethod(unittest.TestCase):
    def test_masked_tile(self):
        tile = Tile()
        tile.bbox = BBox(30.5, 37.5, -51.5, -36.5)
        tile.latitudes = np.ma.arange(30.5, 38.5, 1.0)
        tile.longitudes = np.ma.arange(-51.5, -35.5, 1.0)
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(128.0).reshape((1, 8, 16))

        # tile.latitudes [ 30.5  31.5  32.5  33.5  34.5  35.5  36.5  37.5]
        tile.latitudes = np.ma.masked_outside(tile.latitudes, 35, 45)
        # tile.latitudes [-- -- -- -- -- 35.5 36.5 37.5]

        # tile.longitudes [-51.5 -50.5 -49.5 -48.5 -47.5 -46.5 -45.5 -44.5 -43.5 -42.5 -41.5 -40.5 -39.5  -38.5 -37.5 -36.5]
        tile.longitudes = np.ma.masked_outside(tile.longitudes, -50, -40)
        # tile.longitudes [-- -- -49.5 -48.5 -47.5 -46.5 -45.5 -44.5 -43.5 -42.5 -41.5 -40.5 -- -- -- --]

        # Tile no longer contains 35, -50
        self.assertFalse(tile.contains_point(35, -50))


class TestTileUpdateStats(unittest.TestCase):
    def test_update_tile_stats(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        tile.update_stats()

        self.assertAlmostEqual(0.0, tile.tile_stats.min)
        self.assertAlmostEqual(24.0, tile.tile_stats.max)
        self.assertAlmostEqual(12.0, tile.tile_stats.mean)
        self.assertEqual(25, tile.tile_stats.count)


class TestMergeTilesMethod(unittest.TestCase):
    def test_merge_tiles(self):
        tile1 = Tile()
        tile1.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile1.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile1.times = np.ma.array([0])
        tile1.data = np.ma.arange(12).reshape((1, 4, 3))

        tile2 = Tile()
        tile2.latitudes = np.ma.array([4.0, 5.0, 6.0, 7.0])
        tile2.longitudes = np.ma.array([-3.0, -4.0, -5.0])
        tile2.times = np.ma.array([0])
        tile2.data = np.ma.arange(12, 24).reshape((1, 4, 3))

        from nexustiles.model.nexusmodel import merge_tiles

        times, lats, longs, data = merge_tiles([tile1, tile2])

        self.assertTrue(np.ma.allequal(times, np.array([0])))
        self.assertTrue(np.ma.allequal(lats, np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])))
        self.assertTrue(np.ma.allequal(longs, np.array([-5.0, -4.0, -3.0, -2.0, -1.0, 0.0])))
        expected = np.ma.array([[[0, 0, 0, 2, 1, 0],
                                 [0, 0, 0, 5, 4, 3],
                                 [0, 0, 0, 8, 7, 6],
                                 [0, 0, 0, 11, 10, 9],
                                 [14, 13, 12, 0, 0, 0],
                                 [17, 16, 15, 0, 0, 0],
                                 [20, 19, 18, 0, 0, 0],
                                 [23, 22, 21, 0, 0, 0]]], mask=np.array([[[True, True, True, False, False, False],
                                                                          [True, True, True, False, False, False],
                                                                          [True, True, True, False, False, False],
                                                                          [True, True, True, False, False, False],
                                                                          [False, False, False, True, True, True],
                                                                          [False, False, False, True, True, True],
                                                                          [False, False, False, True, True, True],
                                                                          [False, False, False, True, True, True]]]))
        self.assertTrue(np.ma.allequal(np.ma.getmaskarray(data), np.ma.getmaskarray(expected)))
        self.assertTrue(np.ma.allequal(data, expected))

    def test_merge_tiles_vertical(self):
        tile1 = Tile()
        tile1.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile1.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile1.times = np.ma.array([0])
        tile1.data = np.ma.arange(12).reshape((1, 4, 3))

        tile2 = Tile()
        tile2.latitudes = np.ma.array([4.0, 5.0, 6.0, 7.0])
        tile2.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile2.times = np.ma.array([0])
        tile2.data = np.ma.arange(12, 24).reshape((1, 4, 3))

        from nexustiles.model.nexusmodel import merge_tiles

        times, lats, longs, data = merge_tiles([tile1, tile2])

        self.assertTrue(np.ma.allequal(times, np.array([0])))
        self.assertTrue(np.ma.allequal(lats, np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])))
        self.assertTrue(np.ma.allequal(longs, np.array([-2.0, -1.0, 0.0])))
        expected = np.ma.array([[[2, 1, 0],
                                 [5, 4, 3],
                                 [8, 7, 6],
                                 [11, 10, 9],
                                 [14, 13, 12],
                                 [17, 16, 15],
                                 [20, 19, 18],
                                 [23, 22, 21]]], mask=False)
        self.assertTrue(np.ma.allequal(np.ma.getmaskarray(data), np.ma.getmaskarray(expected)))
        self.assertTrue(np.ma.allequal(data, expected))

    def test_merge_tiles_horizontal(self):
        tile1 = Tile()
        tile1.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile1.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile1.times = np.ma.array([0])
        tile1.data = np.ma.arange(12).reshape((1, 4, 3))

        tile2 = Tile()
        tile2.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile2.longitudes = np.ma.array([-3.0, -4.0, -5.0])
        tile2.times = np.ma.array([0])
        tile2.data = np.ma.arange(12, 24).reshape((1, 4, 3))

        from nexustiles.model.nexusmodel import merge_tiles

        times, lats, longs, data = merge_tiles([tile1, tile2])

        self.assertTrue(np.ma.allequal(times, np.array([0])))
        self.assertTrue(np.ma.allequal(lats, np.array([0.0, 1.0, 2.0, 3.0])))
        self.assertTrue(np.ma.allequal(longs, np.array([-5.0, -4.0, -3.0, -2.0, -1.0, 0.0])))
        expected = np.ma.array([[[14, 13, 12, 2, 1, 0],
                                 [17, 16, 15, 5, 4, 3],
                                 [20, 19, 18, 8, 7, 6],
                                 [23, 22, 21, 11, 10, 9]]], mask=False)
        self.assertTrue(np.ma.allequal(np.ma.getmaskarray(data), np.ma.getmaskarray(expected)))
        self.assertTrue(np.ma.allequal(data, expected))

    def test_merge_tiles_overlapping(self):
        tile1 = Tile()
        tile1.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile1.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile1.times = np.ma.array([0])
        tile1.data = np.ma.arange(12).reshape((1, 4, 3))

        tile2 = Tile()
        tile2.latitudes = np.ma.array([0.0, 1.0, 2.0, 3.0])
        tile2.longitudes = np.ma.array([0.0, -1.0, -2.0])
        tile2.times = np.ma.array([0])
        tile2.data = np.ma.arange(12, 24).reshape((1, 4, 3))

        from nexustiles.model.nexusmodel import merge_tiles

        self.assertRaises(Exception, lambda _: merge_tiles([tile1, tile2]))
