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
import sys, re
import numpy as np
from netCDF4 import Dataset

class XD_Stream_Analyzer:

    def __init__(self, xd_stream):
        self._stream = xd_stream

    def get_var_names(self):
        # Try one version of the NEXUS Spring XD syntax.
        p = re.compile('tilereadingprocessor.+?--environment=(.+?)[-|\|]')
        m = p.search(self._stream)
        if m is None:
            # Try a different version of the NEXUS Spring XD syntax.
            p = re.compile('tilesumarizingprocessor.summarize_nexustile,(.+?)[-|\|]')
            m = p.search(self._stream)
            if m is None:
                raise ValueError, 'Unable to find variable names in Spring XD stream definition.'
        vars_str = m.group(1)
        key_val_dict = dict([key_val_pair.split('=')
                             for key_val_pair in vars_str.split(',')])
        return key_val_dict

    def does_subtract_180(self):
        p = re.compile('subtract180longitude')
        print 'stream is: ',self._stream
        m = p.search(self._stream)
        return (m is not None)

class XD_Granule_Checker:

    def __init__(self, xd_stream, granule_name):
        self._stream = xd_stream
        self._granule = granule_name
        self._xdsa = XD_Stream_Analyzer(xd_stream)
        self._check_nc4_exists()

    def _check_nc4_exists(self):
        # Check that granule exists and is a valid NetCDF4 file.
        try:
            self._rootgrp = Dataset(self._granule, "r")
        except ValueError:
            print 'Could not open {} as NetCDF4 file.'.format(self._granule)
        
    def _get_var_names(self):
        return self._rootgrp.variables

    def _check_xd_var_present(self, xd_generic_var_str, xd_vars, nc_vars):
        if (xd_generic_var_str in xd_vars and
            xd_vars[xd_generic_var_str] not in nc_vars):
            raise ValueError, 'Variable {0} not found in {1}.'.\
                format(xd_vars[xd_generic_var_str], self._granule)

    def check_xd_vars_present(self):
        self._xd_vars = self._xdsa.get_var_names()
        self._nc_vars = self._get_var_names()
        self._check_xd_var_present('VARIABLE', self._xd_vars, self._nc_vars)
        self._check_xd_var_present('LONGITUDE', self._xd_vars, self._nc_vars)
        self._check_xd_var_present('LATITUDE', self._xd_vars, self._nc_vars)
        self._check_xd_var_present('TIME', self._xd_vars, self._nc_vars) 

    def check_lon_range(self, nc_lon_var):
        nc_min_lon = int(round(np.min(nc_lon_var[:])))
        nc_max_lon = int(round(np.max(nc_lon_var[:])))
        xd_sub_180 = self._xdsa.does_subtract_180()
        if xd_sub_180 and (abs(nc_min_lon) > 10):
            raise ValueError, 'Granule longitudes were expected to be in range [0,360], but were found to be in range [{0},{1}]'.format(nc_min_lon, nc_max_lon)
        if (not xd_sub_180) and (abs(nc_min_lon+180) > 10):
            raise ValueError, 'Granule longitudes were expected to be in range [-180,180], but were found to be in range [{0},{1}]'.format(nc_min_lon, nc_max_lon)

    def check_all(self):
        self.check_xd_vars_present()
        self.check_lon_range(self._nc_vars[self._xd_vars['LONGITUDE']])

def main():
    # Parse command line.
    if len(sys.argv) != 3:
        print 'Usage: {} stream_file, granule_file'.format(sys.argv[0])
        print 'stream_file must contain a Spring XD stream definition string'
        print 'granule_file must container a valid NetCDF4 data granule'
        sys.exit(0)
    stream_fname = sys.argv[1]
    granule_fname = sys.argv[2]

    # Read Spring XD stream definition from input file.
    with open(stream_fname, "r") as stream_file:
        stream_str = stream_file.read()

    # Check input granule for compliance with Spring XD stream definition.
    checker = XD_Granule_Checker(stream_str, granule_fname)
    checker.check_all()

if __name__ == '__main__':
    main()
