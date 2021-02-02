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
#     gaussInterp_slow routine -- Gaussian weighted smoothing in lat, lon, and time
#
# Based on Ed Armstrong's routines. Pure python implementation.
#
#
# Gaussian weighting = exp( vfactor * (((x - x0)/sx)^2 + ((y - y0)/sy)^2 + ((t - t0)/st)^2 ))
#
# where deltas are distances in lat, lon and time and sx, sy, st are one e-folding sigmas.
#
# Cutoffs for neighbors allowed in the interpolation are set by distance in lat/lon (see dlat/dlon);
# for time all epochs are included.
#

import sys
import numpy as np
from math import exp
from numba import jit, int32

VERBOSE = 0


def gaussInterp_slow(var,                # bundle of input arrays: masked variable, coordinates
                varNames,                # list of names in order: primary variable, coordinates in order lat, lon, time
                outlat, outlon,          # output lat/lon coordinate vectors
                wlat, wlon,              # window of lat/lon neighbors to gaussian weight, expressed in delta lat (degrees)
                slat, slon, stime,       # sigma for gaussian downweighting with distance in lat, lon (deg), & time (days)
                vfactor=-0.6931,         # factor in front of gaussian expression
                missingValue=-9999.,     # value to mark missing values in interp result
                verbose=VERBOSE,         # integer to set verbosity level
                optimization='python'):  # Mode of optimization, using 'fortran' or 'cython' or 'python'
    '''Gaussian interpolate in lat, lon, and time to a different lat/lon grid, and over a time window to the center time.
Bundle of arrays (var) contains a 3D masked variable and coordinate arrays for lat, lon, and time read from netdf/hdf files.
Returns the 2D interpolated variable (masked) and a status for failures. 
    '''
    v = var[varNames[0]][:]
    vmask = np.ma.getmask(v)[:]
    vtime = var[varNames[1]][:]
    lat = var[varNames[2]][:]
    lon = var[varNames[3]][:]

    vinterp, vweight, status = \
         gaussInterp_(v, vmask, vtime, lat, lon,
                      outlat, outlon, wlat, wlon, slat, slon, stime, vfactor, missingValue)

    vinterp = np.ma.masked_where(vweight == 0.0, vinterp)
    return (vinterp, vweight, status)

#@jit(nopython=False)
def gaussInterp_(var,     # variable & mask arrays with dimensions of time, lon, lat
                 vmask,         
                 vtime,    # coordinate vectors for inputs
                 lat,
                 lon,     
                 outlat,  # coordinate vectors for grid to interpolate to
                 outlon,
                 wlat, wlon,           # window of lat/lon neighbors to gaussian weight, expressed in delta lat (degrees)
                 slat, slon, stime,    # sigma for gaussian downweighting with distance in lat, lon (deg), & time (days)
                 vfactor,              # factor in front of gaussian expression
                 missingValue):        # value to mark missing values in interp result
    '''Gaussian interpolate in lat, lon, and time to a different lat/lon grid, and over a time window to the center time.
Returns the 2D interpolated variable (masked), the weight array, and a status for failures.
    '''
    vinterp = np.zeros( (outlat.shape[0], outlon.shape[0]), dtype=var.dtype )  # interpolated variable, missing values not counted
    vweight = np.zeros( (outlat.shape[0], outlon.shape[0]), dtype=var.dtype )  # weight of values interpolated (can be zero)
    status = 0     # negative status indicates error

    ntime = vtime.shape[0]
    nlat = lat.shape[0]
    nlon = lon.shape[0]

    noutlat = outlat.shape[0]
    noutlon = outlon.shape[0]

    midTime = vtime[int(ntime/2 + 0.5)]
    wlat2 = wlat / 2.
    wlon2 = wlon / 2.
    lat0 = lat[0]
    lon0 = lon[0]
    dlat = lat[1] - lat[0]
    dlon = lon[1] - lon[0]

    for i in range(noutlat):
        print(outlat[i], file=sys.stderr)
        for j in range(noutlon):
           if VERBOSE: print('\n(i,j) = %d, %d' % (i, j), file=sys.stderr)
           if VERBOSE: print('\n(outlat,outlon) = %f, %f' % (outlat[i], outlon[j]), file=sys.stderr)

           imin = clamp(int((outlat[i] - wlat2 - lat0)/dlat + 0.5), 0, nlat-1)
           imax = clamp(int((outlat[i] + wlat2 - lat0)/dlat + 0.5), 0, nlat-1)
           if imin > imax: (imin, imax) = (imax, imin)                            # input latitudes could be descending
           if VERBOSE: print('(imin, imax) = %d, %d' % (imin, imax), file=sys.stderr)
           if VERBOSE: print('(minlat, maxlat) = %f, %f' % (lat[imin], lat[imax]), file=sys.stderr)
           jmin = clamp(int((outlon[j] - wlon2 - lon0)/dlon + 0.5), 0, nlon-1)
           jmax = clamp(int((outlon[j] + wlon2 - lon0)/dlon + 0.5), 0, nlon-1)
           if VERBOSE: print('(jmin, jmax) = %d, %d' % (jmin, jmax), file=sys.stderr)
           if VERBOSE: print('(minlon, maxlon) = %f, %f' % (lon[jmin], lon[jmax]), file=sys.stderr)
#           stencil = np.zeros( (ntime, imax-imin+1, jmax-jmin+1) )

           for kin in range(ntime):
               for iin in range(imin, imax+1):
                   for jin in range(jmin, jmax+1):
                       if not vmask[kin,iin,jin]:
                           fac = exp( vfactor *
                                     (((outlat[i] - lat[iin])/slat)**2
                                    + ((outlon[j] - lon[jin])/slon)**2
                                    + ((midTime   - vtime[kin])/stime)**2))
#                           stencil[kin, iin-imin, jin-jmin] = fac
                           val = var[kin, iin, jin]
                           if VERBOSE > 1: print(kin, iin, jin, vtime[kin], lat[iin], lon[jin], val, fac, val*fac, file=sys.stderr)

                           vinterp[i,j] = vinterp[i,j] + val * fac
                           vweight[i,j] = vweight[i,j] + fac


           if vweight[i,j] != 0.0:
               vinterp[i,j] = vinterp[i,j] / vweight[i,j]
#               if VERBOSE > 1: print >>sys.stderr, 'stencil:\n', stencil
#               if VERBOSE: print >>sys.stderr, 'stencil max:\n', np.max(np.max(stencil))
#               if VERBOSE: print >>sys.stderr, 'stencil min:\n', np.min(np.min(stencil))
           else:
               vinterp[i,j] = missingValue

    return (vinterp, vweight, status)

#@jit( int32(int32,int32,int32), nopython=False)
def clamp(i, n, m):
    if i < n: return n
    if i > m: return m
    return i
