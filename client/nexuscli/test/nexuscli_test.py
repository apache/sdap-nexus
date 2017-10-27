"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import unittest
from datetime import datetime

from shapely.geometry import box

import nexuscli


class TestCli(unittest.TestCase):
    def test_time_series(self):
        ts = nexuscli.time_series(("AVHRR_OI_L4_GHRSST_NCEI", "MEASURES_SLA_JPL_1603"), box(-150, 45, -120, 60),
                                  datetime(2016, 1, 1), datetime(2016, 12, 31))

        self.assertEqual(2, len(ts))

    def test_list(self):
        ds_list = nexuscli.dataset_list()

        print(ds_list)
        self.assertTrue(len(ds_list) > 0)

    def test_daily_difference_average(self):
        ts = nexuscli.daily_difference_average("AVHRR_OI_L4_GHRSST_NCEI", box(-150, 45, -120, 60),
                                               datetime(2013, 1, 1), datetime(2014, 12, 31))

        self.assertEqual(1, len(ts))
