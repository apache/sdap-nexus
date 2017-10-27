"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import importlib
import unittest
from os import environ, path

import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array


class TestSSHData(unittest.TestCase):
    def setUp(self):
        environ['REGRID_VARIABLES'] = 'SLA,SLA_ERR'
        environ['LATITUDE'] = 'Latitude'
        environ['LONGITUDE'] = 'Longitude'
        environ['TIME'] = 'Time'

        self.module = importlib.import_module('nexusxd.regrid1x1')

    def tearDown(self):
        del environ['REGRID_VARIABLES']
        del environ['LATITUDE']
        del environ['LONGITUDE']
        del environ['TIME']

    def test_ssh_grid(self):
        # environ['VARIABLE_VALID_RANGE'] = 'SLA:-100.0:100.0:SLA_ERR:-5000:5000'
        # reload(self.module)
        test_file = '/Users/greguska/regrid/ssh_grids_v1609_1992100212.nc'  # path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        results = list(self.module.regrid(None, test_file))

        self.assertEquals(1, len(results))


class TestGRACEData(unittest.TestCase):
    def setUp(self):
        environ['REGRID_VARIABLES'] = 'lwe_thickness'
        environ['LATITUDE'] = 'lat'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'

        self.module = importlib.import_module('nexusxd.regrid1x1')

    def tearDown(self):
        del environ['REGRID_VARIABLES']
        del environ['LATITUDE']
        del environ['LONGITUDE']
        del environ['TIME']

    def test_lwe_grid(self):
        # environ['VARIABLE_VALID_RANGE'] = 'SLA:-100.0:100.0:SLA_ERR:-5000:5000'
        # reload(self.module)
        test_file = '/Users/greguska/regrid/GRCTellus.JPL.200204_201608.GLO.RL05M_1.MSCNv02CRIv02.nc'  # path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        results = list(self.module.regrid(None, test_file))

        self.assertEquals(1, len(results))


class TestIceShelfData(unittest.TestCase):
    def setUp(self):
        environ['REGRID_VARIABLES'] = 'height_raw,height_filt,height_err'
        environ['LATITUDE'] = 'lat'
        environ['LONGITUDE'] = 'lon'
        environ['TIME'] = 'time'

        self.module = importlib.import_module('nexusxd.regrid1x1')

    def tearDown(self):
        del environ['REGRID_VARIABLES']
        del environ['LATITUDE']
        del environ['LONGITUDE']
        del environ['TIME']

    def test_height_raw(self):
        # environ['VARIABLE_VALID_RANGE'] = 'SLA:-100.0:100.0:SLA_ERR:-5000:5000'
        # reload(self.module)
        test_file = '/Users/greguska/data/ice_shelf_dh/ice_shelf_dh_v1.nc'  # path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        results = list(self.module.regrid(None, test_file))

        self.assertEquals(1, len(results))
