#
# spatialFilter routine -- Apply a fixed spatial filter (smoother) in lat/lon and then average over times/grids
#
# Calls into optimized routine in Fortran (spatialFilter_f.f).
#

import numpy as N, time
from spatialFilter_f import spatialfilter_f


def spatialFilter(var,                      # bundle of input arrays: masked variable, coordinates
                  varNames,                 # list of names in order: primary variable, coordinates in order lat, lon, time
                  spatialFilter,            # 3x3 filter numpy array of integers
                  normalization,            # normalization factor for filter (integer)
                  missingValue=-9999.,      # value to mark missing values in interp result
                  verbose=1,                # integer to set verbosity level
                  optimization='fortran'):  # Mode of optimization, using 'fortran' or 'cython'
    '''Apply a fixed spatial filter (smoother) in lat/lon and then average over times/grids.
    '''
    # Prepare numpy arrays
    v = var[varNames[0]][:]                     # real*4 in Fortran code, is v.dtype correct?
    vmask = N.ma.getmask(v).astype('int8')[:]   # integer*1, convert bool mask to one-byte integer for Fortran
    vtime  = var[varNames[1]][:]                # integer*4 in Fortran
    lat = var[varNames[2]][:]                   # real*4
    lon = var[varNames[3]][:]                   # real*4

    if optimization == 'fortran':
        vinterp, vcount, status = \
             spatialfilter_f(v, vmask, vtime, lat, lon,
                             spatialFilter, normalization, missingValue, verbose)
    else:
        pass

    vinterp = N.ma.masked_where(vcount == 0, vinterp)
    return (vinterp, vcount, status)

