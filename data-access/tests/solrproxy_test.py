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
import ConfigParser

import logging
import pkg_resources
import time

from nexustiles.dao.SolrProxy import SolrProxy
from shapely.geometry import box


class TestQuery(unittest.TestCase):
    def setUp(self):
        config = ConfigParser.RawConfigParser()

        config.readfp(pkg_resources.resource_stream(__name__, "config/datastores.ini"), filename='datastores.ini')

        self.proxy = SolrProxy(config)
        logging.basicConfig(level=logging.DEBUG)

    def find_distinct_section_specs_in_polygon_test(self):
        result = self.proxy.find_distinct_bounding_boxes_in_polygon(box(-180, -90, 180, 90),
                                                                   "MXLDEPTH_ECCO_version4_release1",
                                                                    1, time.time())

        print len(result)
        for r in sorted(result):
            print r

    def find_all_tiles_in_polygon_with_spec_test(self):
        result = self.proxy.find_all_tiles_in_polygon(box(-180, -90, 180, 90),
                                                      "AVHRR_OI_L4_GHRSST_NCEI",
                                                      fq={'sectionSpec_s:\"time:0:1,lat:100:120,lon:0:40\"'},
                                                      rows=1, limit=1)

        print result

    def find_tiles_by_id_test(self):
        result = self.proxy.find_tiles_by_id(['0cc95db3-293b-3553-b7a3-42920c3ffe4d'], ds="AVHRR_OI_L4_GHRSST_NCEI")

        print result

    def find_max_date_from_tiles_test(self):
        result = self.proxy.find_max_date_from_tiles(["a764f12b-ceac-38d6-9d1d-89a6b68db32b"],
                                                     "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1", rows=1, limit=1)

        print result

    def find_tiles_by_exact_bounds_test(self):
        result = self.proxy.find_tiles_by_exact_bounds(175.01, -42.68, 180.0, -40.2,
                                                       "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1", rows=5000)

        print len(result)

    def get_data_series_list_test(self):
        result = self.proxy.get_data_series_list()

        print len(result)

    def test_find_all_tiles_by_metadata(self):
        result = self.proxy.find_all_tiles_by_metadata(['granule_s:19811114120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'], ds="AVHRR_OI_L4_GHRSST_NCEI")

        print len(result)

    def test_get_tile_count(self):
        tile_count = self.proxy.get_tile_count("AVHRR_OI_L4_GHRSST_NCEI", bounding_polygon=box(-180, -90, 180, 90),
                                               start_time=1, end_time=time.time(),
                                               metadata=['granule_s:19811114120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'])

        print(tile_count)
