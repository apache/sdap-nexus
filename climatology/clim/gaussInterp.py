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
#
# gaussInterp routine -- Gaussian weighted smoothing in lat, lon, and time
#
# Based on Ed Armstrong's routines.
#
# Calls into optimized routines in Fortran or cython.
# See gaussInterp_f.f  or gaussInterp.pyx
#

import numpy as N, time
from gaussInterp_f import gaussinterp_f


def gaussInterp(var,                      # bundle of input arrays: masked variable, coordinates
                varNames,                 # list of names in order: primary variable, coordinates in order lat, lon, time
                outlat, outlon,           # output lat/lon coordinate vectors
                wlat, wlon,               # window of lat/lon neighbors to gaussian weight, expressed in delta lat (degrees)
                slat, slon, stime,        # sigma for gaussian downweighting with distance in lat, lon (deg), & time (days)
                vfactor=-0.6931,          # factor in front of gaussian expression
                missingValue=-9999.,      # value to mark missing values in interp result
                verbose=1,                # integer to set verbosity level
                optimization='fortran'):  # Mode of optimization, using 'fortran' or 'cython'
    '''Gaussian interpolate in lat, lon, and time to a different lat/lon grid, and over a time window to the center time.
Bundle of arrays (var) contains a 3D masked variable and coordinate arrays for lat, lon, and time read from netdf/hdf files.
Returns the 2D interpolated variable (masked) and a status for failures. 
    '''
    v = var[varNames[0]][:]                     # real*4 in Fortran code, is v.dtype correct?
    vmask = N.ma.getmask(v).astype('int8')[:]   # integer*1, convert bool mask to one-byte integer for Fortran
    vtime  = var[varNames[1]][:]                # integer*4 in Fortran
    lat = var[varNames[2]][:]                   # real*4
    lon = var[varNames[3]][:]                   # real*4
    if optimization == 'fortran':
        vinterp, vweight, status = \
             gaussinterp_f(v, vmask, vtime, lat, lon,
                           outlat, outlon, wlat, wlon, slat, slon, stime, vfactor, missingValue, verbose)
    else:
        vinterp, vweight, status = \
             gaussInterp_(v, vmask, vtime, lat, lon,
                          outlat, outlon, wlat, wlon, slat, slon, stime, vfactor, missingValue, verbose)

    vinterp = N.ma.masked_where(vweight == 0.0, vinterp)
    return (vinterp, vweight, status)

