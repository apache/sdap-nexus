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
from os import environ, path
import nexusproto.NexusContent_pb2 as nexusproto


class TestRunChainMethod(unittest.TestCase):
    def test_run_chain_read_filter_all(self):
        environ['CHAIN'] = 'nexusxd.tilereadingprocessor.read_grid_data:nexusxd.emptytilefilter.filter_empty_tiles'
        environ['INBOUND_PORT'] = '7890'
        environ['VARIABLE'] = 'analysed_sst'
        environ['OUTBOUND_PORT'] = '7891'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['READER'] = 'GRIDTILE'
        environ['LATITUDE'] = 'lat'

        test_file = path.join(path.dirname(__file__), 'datafiles', 'empty_mur.nc4')

        from nexusxd import processorchain
        gen = processorchain.run_chain(None, "time:0:1,lat:0:1,lon:0:1;time:0:1,lat:1:2,lon:0:1;file://%s" % test_file)
        for message in gen:
            self.fail("Should not produce any messages. Message: %s" % message)

    def test_run_chain_read_filter_none(self):
        environ['CHAIN'] = 'nexusxd.tilereadingprocessor.read_grid_data:nexusxd.emptytilefilter.filter_empty_tiles'
        environ['INBOUND_PORT'] = '7890'
        environ['VARIABLE'] = 'analysed_sst'
        environ['OUTBOUND_PORT'] = '7891'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['READER'] = 'GRIDTILE'
        environ['LATITUDE'] = 'lat'

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_mur.nc4')

        from nexusxd import processorchain
        results = list(processorchain.run_chain(None, "time:0:1,lat:0:1,lon:0:1;time:0:1,lat:1:2,lon:0:1;file://%s" % test_file))

        self.assertEquals(2, len(results))

    def test_run_chain_read_filter_kelvin_summarize(self):
        environ['CHAIN'] = 'nexusxd.tilereadingprocessor.read_grid_data:nexusxd.emptytilefilter.filter_empty_tiles:nexusxd.kelvintocelsius.transform:nexusxd.tilesumarizingprocessor.summarize_nexustile'
        environ['INBOUND_PORT'] = '7890'
        environ['VARIABLE'] = 'analysed_sst'
        environ['OUTBOUND_PORT'] = '7891'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['READER'] = 'GRIDTILE'
        environ['LATITUDE'] = 'lat'

        test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_mur.nc4')

        from nexusxd import processorchain
        results = list(
            processorchain.run_chain(None, "time:0:1,lat:0:1,lon:0:1;time:0:1,lat:1:2,lon:0:1;file://%s" % test_file))

        self.assertEquals(2, len(results))

    def test_run_chain_partial_empty(self):
        environ[
            'CHAIN'] = 'nexusxd.tilereadingprocessor.read_grid_data:nexusxd.emptytilefilter.filter_empty_tiles:nexusxd.kelvintocelsius.transform:nexusxd.tilesumarizingprocessor.summarize_nexustile'
        environ['INBOUND_PORT'] = '7890'
        environ['VARIABLE'] = 'analysed_sst'
        environ['OUTBOUND_PORT'] = '7891'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'
        environ['READER'] = 'GRIDTILE'
        environ['LATITUDE'] = 'lat'

        test_file = path.join(path.dirname(__file__), 'datafiles', 'partial_empty_mur.nc4')

        from nexusxd import processorchain
        results = list(
            processorchain.run_chain(None, "time:0:1,lat:0:10,lon:0:10;time:0:1,lat:489:499,lon:0:10;file://%s" % test_file))

        self.assertEquals(1, len(results))
        tile = nexusproto.NexusTile.FromString(results[0])

        self.assertTrue(tile.summary.HasField('bbox'), "bbox is missing")


if __name__ == '__main__':
    unittest.main()
