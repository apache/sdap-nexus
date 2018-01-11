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
