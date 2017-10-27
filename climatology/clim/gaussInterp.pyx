c
c     gaussInterp routine -- Gaussian weighted smoothing in lat, lon, and time
c
c Based on Ed Armstrong's routines. Designed to be called from python using f2py.
c
c
c Gaussian weighting = exp( vfactor * (((x - x0)/sx)^2 + ((y - y0)/sy)^2 + ((t - t0)/st)^2 ))
c
c where deltas are distances in lat, lon and time and sx, sy, st are one e-folding sigmas.
c
c Cutoffs for neighbors allowed in the interpolation are set by distance in lat/lon (see dlat/dlon);
c for time all epochs are included.
c

import sys
import numpy as np
cimport numpy as np
from math import exp

from gaussInterp_f import gaussInterp_f

Var_t = np.float
ctypedef np.float_t Var_t

VERBOSE = 1


def gaussInterp(var,                     # bundle of input arrays: masked variable, coordinates
                varNames,                # list of names in order: primary variable, coordinates in order lat, lon, time
                outlat, outlon,          # output lat/lon coordinate vectors
                wlat, wlon,              # window of lat/lon neighbors to gaussian weight, expressed in delta lat (degrees)
                slat, slon, stime,       # sigma for gaussian downweighting with distance in lat, lon (deg), & time (days)
                vfactor=-0.6931,         # factor in front of gaussian expression
                missingValue=-9999.,     # value to mark missing values in interp result
                verbose=VERBOSE,         # integer to set verbosity level
                optimization='cython'):  # Mode of optimization, using 'fortran' or 'cython'
    '''Gaussian interpolate in lat, lon, and time to a different lat/lon grid, and over a time window to the center time.
Bundle of arrays (var) contains a 3D masked variable and coordinate arrays for lat, lon, and time read from netdf/hdf files.
Returns the 2D interpolated variable (masked) and a status for failures. 
    '''
    v = var[varNames[0]][:]
    vmask = np.ma.getmask(v)[:]
    vtime = var[varNames[1]][:]
    lat = var[varNames[2]][:]
    lon = var[varNames[3]][:]
    if optimization == 'fortran':
        vinterp, vweight, status = 
             gaussInterp_f(v, vmask, vtime, lat, lon,
                           outlat, outlon, wlat, wlon, slat, slon, stime, vfactor, missingValue)
    else:
        vinterp, vweight, status = 
             gaussInterp_(v, vmask, vtime, lat, lon,
                          outlat, outlon, wlat, wlon, slat, slon, stime, vfactor, missingValue)
    vinterp = np.ma.array(vinterp, mask=np.ma.make_mask(vweight))    # apply mask
    return (vinterp, vweight, status)


#@cython.boundscheck(False)
def gaussInterp_(np.ndarray[Var_t, ndim=3] var,          # variable & mask arrays with dimensions of lat,lon,time
                 np.ndarray[Var_t, ndim=3] vmask,         
                 np.ndarray[np.int_t, ndim=1] vtime,
                 np.ndarray[np.float_t, ndim=1] lat,
                 np.ndarray[np.float_t, ndim=1] lon,     # coordinate vectors for inputs
                 np.ndarray[np.float_t, ndim=1] outlat,  # coordinate vectors for grid to interpolate to
                 np.ndarray[np.float_t, ndim=1] outlon,
                 float wlat, float wlon,                 # window of lat/lon neighbors to gaussian weight, expressed in delta lat (degrees)
                 float slat, float slon, float stime,    # sigma for gaussian downweighting with distance in lat, lon (deg), & time (days)
                 float vfactor,                          # factor in front of gaussian expression
                 float missingValue):                    # value to mark missing values in interp result
    '''Gaussian interpolate in lat, lon, and time to a different lat/lon grid, and over a time window to the center time.
Returns the 2D interpolated variable (masked), the weight array, and a status for failures.
    '''
    assert var.dtype == Var_t  and mask.dtype == np.int
    
    cdef np.ndarray[Var_t, ndim=2] vinterp = np.zeros( (outlat.shape[0], outlon.shape[0]), dtype=Var_t )  # interpolated variable, missing values not counted
    cdef np.ndarray[Var_t, ndim=2] vweight = np.zeros( (outlat.shape[0], outlon.shape[0]), dtype=Var_t )  # weight of values interpolated (can be zero)
    cdef int status = 0     # negative status indicates error

    cdef int imin, imax, jmin, jmax
    cdef int iin, jin, kin
    cdef int i, j

    cdef int ntime = time.shape[0]
    cdef int nlat = lat.shape[0]
    cdef int nlon = lon.shape[0]

    cdef int noutlat = outlat.shape[0]
    cdef int noutlon = outlon.shape[0]

    cdef float wlat2 = wlat / 2.
    cdef float wlon2 = wlon / 2.
    cdef float lat0 = lat[0]
    cdef float lon0 = lon[0]
    cdef float dlat = lat[1] - lat[0]
    cdef float dlon = lon[1] - lon[0]
    cdef double midTime = time[int(ntime/2 + 0.5)]

    for i xrange(noutlat):
        print >>sys.stderr, outlat[i]
        for j in xrange(noutlon):
           imin = clamp(int((outlat[i] - wlat2 - lat0)/dlat + 0.5), 0, nlat-1)
           imax = clamp(int((outlat[i] + wlat2 - lat0)/dlat + 0.5), 0, nlat-1)
           jmin = clamp(int((outlon[j] - wlon2 - lon0)/dlon + 0.5), 0, nlon-1)
           jmax = clamp(int((outlon[j] + wlon2 - lon0)/dlon + 0.5), 0, nlon-1)

           for kin in xrange(ntime):
               for iin in xrange(imin, imax+1):
                   for jin in xrange(jmin, jmax+1):
                       if not vmask[kin, iin, jin]:
                           fac = exp( vfactor *
                                     (((outlat[i] - lat[iin])/slat)**2
                                    + ((outlon[j] - lon[jin])/slon)**2
                                    + ((midTime   - vtime[kin])/stime)**2))
                           val = var[kin, iin, jin]
                           if VERBOSE > 1: print >>sys.stderr,  kin, iin, jin, vtime[kin], lat[iin], lon[jin], val, fac, val*fac

                           vinterp[i,j] = vinterp[i,j] + val * fac
                           vweight[i,j] = vweight[i,j] + fac

           if vweight[i,j] != 0.0:
               vinterp[i,j] = vinterp[i,j] / vweight[i,j]
          else:
               vinterp[i,j] = missingValue

    return (vinterp, vweight, status)


cdef int clamp(int i, int n, int m):
    if i < n: return n
    if i > m: return m
    return i
