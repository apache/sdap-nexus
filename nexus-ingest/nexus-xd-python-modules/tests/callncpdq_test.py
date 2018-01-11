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
import subprocess
import unittest
from os import environ, path, remove

from netCDF4 import Dataset


class TestMeasuresData(unittest.TestCase):
    def setUp(self):
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

    def tearDown(self):
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']
        del environ['DIMENSION_ORDER']

    @unittest.skipIf(int(subprocess.call(["ncpdq"])) == 127, "requires ncpdq")
    def test_permute_all_variables(self):
        environ['DIMENSION_ORDER'] = 'Time:Latitude:Longitude'
        self.module = importlib.import_module('nexusxd.callncpdq')
        reload(self.module)

        expected_dimensions = environ['DIMENSION_ORDER'].split(':')

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_measures_alt.nc')

        output_path = list(self.module.call_ncpdq(self, test_file))[0]

        with Dataset(output_path) as ds:
            sla_var = ds['SLA']
            actual_dimensions = [str(dim) for dim in sla_var.dimensions]

        remove(output_path)
        self.assertEqual(expected_dimensions, actual_dimensions)

    @unittest.skipIf(int(subprocess.call(["ncpdq"])) == 127, "requires ncpdq")
    def test_permute_one_variable(self):
        environ['DIMENSION_ORDER'] = 'Time:Latitude:Longitude'
        environ['PERMUTE_VARIABLE'] = 'SLA'
        self.module = importlib.import_module('nexusxd.callncpdq')
        reload(self.module)

        expected_dimensions = environ['DIMENSION_ORDER'].split(':')

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_measures_alt.nc')

        output_path = list(self.module.call_ncpdq(self, test_file))[0]

        with Dataset(output_path) as ds:
            sla_var = ds['SLA']
            actual_dimensions = [str(dim) for dim in sla_var.dimensions]

        remove(output_path)
        self.assertEqual(expected_dimensions, actual_dimensions)
