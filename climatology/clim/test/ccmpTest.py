"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import unittest
import ClimatologySpark2


class CCMPTest(unittest.TestCase):
    def cmmp_test(self):
        dsName = 'CCMPWind'
        nEpochs = '1'
        nWindow = '1'
        averager = 'pixelMean'
        sparkConfig = 'multicore,4,4'
        outHdfsPath = 'cache/clim'

        ClimatologySpark2.main([dsName, nEpochs, nWindow, averager, sparkConfig, outHdfsPath])
