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
"""
climatology3Spark.py

Compute a multi-epoch (multi-day) climatology from daily SST Level-3 grids.

Simple code to be run on Spark cluster, or using multi-core parallelism on single machine.

"""

import sys, os, urlparse, urllib, re, time
import numpy as N
import matplotlib
matplotlib.use('Agg')
import matplotlib.pylab as M

from variables import getVariables, close
from cache import retrieveFile, CachePath
from split import fixedSplit, splitByNDays
from netCDF4 import Dataset, default_fillvals
from plotlib import imageMap, makeMovie

from spatialFilter import spatialFilter
from gaussInterp import gaussInterp         # calls into Fortran version gaussInterp_f.so
#from gaussInterp_slow import gaussInterp_slow as gaussInterp       # pure python, slow debuggable version

VERBOSE = 1

# Possible execution modes
ExecutionModes = ['multicore', 'spark']

# SST L3m 4.6km Metadata
# SST calues are scaled integers in degrees Celsius, lon/lat is 8640 x 4320
# Variable = 'sst', Mask = 'qual_sst', Coordinates = ['lon', 'lat']

# Generate algorithmic name for N-day Climatology product
SSTClimatologyTemplate = 'SST.L3.Global.Clim.%(period)s.%(date)s.%(version)s.nc'  #??

# Simple mask and average functions to get us started, then add gaussian interpolation.
# MODIS L3 SST product, qual_sst is [-1, 2] - =0 is best data, can add =1 for better coverage
#def qcMask(var, mask): return N.ma.array(var, mask=N.ma.make_mask(mask == 0))

def qcMask(var, mask): return N.ma.masked_where(mask != 0, var)
#def qcMask(var, mask): return N.ma.masked_where(mask < 0, var)

def splitModisSst(seq, n):
    for chunk in splitByNDays(seq, n, re.compile(r'(...).L3m')):
        yield chunk

AveragingFunctions = {'pixelMean': mean, 'gaussInterp': gaussInterp, 'spatialFilter': spatialFilter}

PixelMeanConfig = {'name': 'pixelMean'}

GaussInterpConfig = {'name': 'gaussInterp',
                     'latGrid': None, 'lonGrid': None,         # None means use input lat/lon grid
                     'wlat': 3, 'wlon': 3,
                     'slat': 0.15, 'slon': 0.15, 'stime': 1,
                     'vfactor': -0.6931, 'missingValue': default_fillvals['f4']}

GaussInterpConfig1a = {'name': 'gaussInterp',
                     'latGrid': None, 'lonGrid': None,         # None means use input lat/lon grid
                     'wlat': 0.30, 'wlon': 0.30,
                     'slat': 0.15, 'slon': 0.15, 'stime': 1,
                     'vfactor': -0.6931, 'missingValue': default_fillvals['f4']}

GaussInterpConfig1b = {'name': 'gaussInterp',
                     'latGrid': None, 'lonGrid': None,         # None means use input lat/lon grid
                     'wlat': 0.08, 'wlon': 0.08,
                     'slat': 0.15, 'slon': 0.15, 'stime': 1,
                     'vfactor': -0.6931, 'missingValue': default_fillvals['f4']}

GaussInterpConfig2 = {'name': 'gaussInterp',
                     'latGrid': (89.5, -89.5, -0.25), 'lonGrid': (-180., 179., 0.25),
                     'wlat': 2., 'wlon': 2.,
                     'slat': 0.15, 'slon': 0.15, 'stime': 1,
                     'vfactor': -0.6931, 'missingValue': default_fillvals['f4']}

FilterGaussian = [[1, 2, 1], [2, 4, 2], [1, 2, 1]]    # divide by 16
FilterLowPass = [[1, 1, 1], [1, 1, 1], [1, 1, 1]]     # divide by 9

SpatialFilterConfig1 = {'name': 'spatialFilter', 'normalization': 16., 
                        'spatialFilter': N.array(FilterGaussian, dtype=N.int32),
                        'missingValue': default_fillvals['f4']}

SpatialFilterConfig2 = {'name': 'spatialFilter', 'normalization': 9.,
                        'spatialFilter': N.array(FilterLowPass, dtype=N.int32),
                        'missingValue': default_fillvals['f4']}

# Directory to cache retrieved files in
CachePath = '~/cache'



def climByAveragingPeriods(urls,              # list of (daily) granule URLs for a long time period (e.g. a year)
                    nEpochs,                  # compute a climatology for every N epochs (days) by 'averaging'
                    nWindow,                  # number of epochs in window needed for averaging
                    variable,                 # name of primary variable in file
                    mask,                     # name of mask variable
                    coordinates,              # names of coordinate arrays to read and pass on (e.g. 'lat' and 'lon')
                    splitFn=splitModisSst,    # split function to use to partition the input URL list
                    maskFn=qcMask,            # mask function to compute mask from mask variable
                    averager='pixelMean',     # averaging function to use, one of ['pixelMean', 'gaussInterp', 'spatialFilter']
                    averagingConfig={},       # dict of parameters to control the averaging function (e.g. gaussInterp)
                    optimization='fortran',   # optimization mode (fortran or cython)
                    mode='multicore',         # Map across time periods of N-days for concurrent work, executed by:
                                              # 'multicore' using pysparkling or 'spark' using Spark/Mesos cluster
                    numNodes=1,               # number of cluster nodes to use
                    nWorkers=4,               # number of parallel workers per node
                    averagingFunctions=AveragingFunctions,    # dict of possible averaging functions
                    legalModes=ExecutionModes,  # list of possible execution modes
                    cachePath=CachePath       # directory to cache retrieved files in
                   ):
    '''Compute a climatology every N days by applying a mask and averaging function.
Writes the averaged variable grid, attributes of the primary variable, and the coordinate arrays in a dictionary.
***Assumption:  This routine assumes that the N grids will fit in memory.***
    '''
    if averagingConfig['name'] == 'gaussInterp':
        averagingConfig['wlat'] = nNeighbors
        averagingConfig['wlon'] = nNeighbors
    try:
        averageFn = averagingFunctions[averager]
    except:
        print >>sys.stderr, 'climatology: Error, Averaging function must be one of: %s' % str(averagingFunctions)
        sys.exit(1)

    urlSplits = [s for s in splitFn(urls, nEpochs)]

    if mode == 'multicore':
        pass
    elif mode == 'spark':
        pass

    def climsContoured(urls, plot=None, fillValue=default_fillvals['f4'], format='NETCDF4', cachePath=cachePath):
        n = len(urls)
        if VERBOSE: print >>sys.stderr, urls

        var = climByAveraging(urls, variable, mask, coordinates, maskFn, averageFn, averagingConfig, optimization, cachePath)

        fn = os.path.split(urls[0])[1]
        inFile = os.path.join(cachePath, fn)
        method = averagingConfig['name']
        fn = os.path.splitext(fn)[0]
        day = fn[5:8]
        nDays = int(var['time'][0])

        if 'wlat' in averagingConfig:
            wlat = averagingConfig['wlat']
        else:
            wlat = 1
        if int(wlat) == wlat:
            outFile = 'A%s.L3m_%dday_clim_sst_4km_%s_%dnbrs.nc' % (day, nDays, method, int(wlat))    # mark each file with first day in period
        else:
            outFile = 'A%s.L3m_%dday_clim_sst_4km_%s_%4.2fnbrs.nc' % (day, nDays, method, wlat)    # mark each file with first day in period

        outFile = writeOutNetcdfVars(var, variable, mask, coordinates, inFile, outFile, fillValue, format)

        if plot == 'contour':
            figFile = contourMap(var, variable, coordinates, n, outFile)
        elif plot == 'histogram':
#            figFile = histogram(var, variable, n, outFile)
            figFile = None
        else:
            figFile = None
        return (outFile, figFile)


    return results


def accumulateClim(urls,                    # list of granule URLs for a time period
                   variable,                # name of primary variable in file
                   mask,                    # name of mask variable
                   coordinates,             # names of coordinate arrays to read and pass on (e.g. 'lat' and 'lon')
                   maskFn=qcMask,           # mask function to compute mask from mask variable
                   averageFn=mean,          # averaging function to use
                   averagingConfig={},      # parameters to control averaging function (e.g. gaussInterp)
                   optimization='fortran',  # optimization mode (fortran or cython)
                   cachePath=CachePath
                  ):
    '''Compute a climatology over N arrays by applying a mask and averaging function.
Returns the averaged variable grid, attributes of the primary variable, and the coordinate arrays in a dictionary.
***Assumption:  This routine assumes that the N grids will fit in memory.***
    '''
    print >>sys.stderr, 'accumulateClim: Doing %s ...' % averagingConfig['name']
    varList = [variable, mask]
    for i, url in enumerate(urls):
        try:
            path = retrieveFile(url, cachePath)
            fn = os.path.split(path)[1]
            vtime = int(fn[5:8])     # KLUDGE: extract DOY from filename
        except:
            print >>sys.stderr, 'climByAveraging: Error, continuing without file %s' % url
            continue
        if path is None: continue
        try:
            print >>sys.stderr, 'Reading file %s ...' % path
            var, fh = getVariables(path, varList, arrayOnly=True, order='F', set_auto_mask=False)   # return dict of variable objects by name
        except:
            print >>sys.stderr, 'climByAveraging: Error, cannot read file %s' % path
            continue
        if i == 0:
            dtype = var[variable].dtype
            if 'int' in dtype.name: dtype = N.float32
            shape = var[variable].shape
            vsum = N.ma.empty(shape, dtype, order='F')
            vcount = N.ma.empty(shape, dtype, order='F')
            emptyVar = N.array(N.ma.masked_all(var[variable].shape, dtype), order='F')  # totally masked variable array for missing or bad file reads

            print >>sys.stderr, 'Read coordinates ...'
            var, fh = getVariables(path, coordinates, var, arrayOnly=True, order='F')   # read coordinate arrays and add to dict

        var[variable] = maskFn(var[variable], var[mask])     # apply quality mask variable to get numpy MA, turned off masking done by netCDF4 library

        # Echo variable range for sanity check
        vals = var[variable].compressed()
        print >>sys.stderr, 'Variable Range: min, max:', vals.min(), vals.max()

        if averagingConfig['name'] == 'pixelMean':
            vsum += var[variable]                        # update accumulators
            vcount += ~var[mask]

        elif averagingConfig['name'] == 'gaussInterp':
            var[variable] = accum
            c = averagingConfig
            latGrid = c['latGrid']; lonGrid = c['lonGrid']
            if latGrid is not None and lonGrid is not None:
                outlat = N.arange(latGrid[0], latGrid[1]+latGrid[2], latGrid[2], dtype=N.float32, order='F')
                outlon = N.arange(lonGrid[0], lonGrid[1]+lonGrid[2], lonGrid[2], dtype=N.float32, order='F')
            else:
                outlat = N.array(var[coordinates[1]], dtype=N.float32, order='F')
                outlon = N.array(var[coordinates[2]], dtype=N.float32, order='F')
            varNames = [variable] + coordinates
            start = time.time()
            avg, weight, status = \
                gaussInterp(var, varNames, outlat, outlon, c['wlat'], c['wlon'],
                            c['slat'], c['slon'], c['stime'], c['vfactor'], c['missingValue'],
                            VERBOSE, optimization)
            end = time.time()
            vcount = weight.astype(N.float32)
            vsum = avg
            print >>sys.stderr, 'gaussInterp execution time:', (end - start)
        elif averagingConfig['name'] == 'spatialFilter':
            var[variable] = accum
            c = averagingConfig
            varNames = [variable] + coordinates
            start = time.time()
            avg, vcount, status = \
                spatialFilter(var, varNames, c['spatialFilter'], c['normalization'], 
                              c['missingValue'], VERBOSE, optimization)
            vsum = avg
            end = time.time()
            print >>sys.stderr, 'spatialFilter execution time:', (end - start)
        close(fh)

    return (vcount, vsum)


def writeOutNetcdfVars(var, variable, mask, coordinates, inFile, outFile, fillValue=None, format='NETCDF4'):
    '''Construct output bundle of arrays with NetCDF dimensions and attributes.
Output variables and attributes will have same names as the input file.
    '''
    din = Dataset(inFile, 'r')
    dout = Dataset(outFile, 'w', format=format)
    print >>sys.stderr, 'Writing %s ...' % outFile

    # Transfer global attributes from input file
    for a in din.ncattrs():
        dout.setncattr(a, din.getncattr(a))

    # Add dimensions and variables, copying data
    coordDim = [dout.createDimension(coord, var['out'+coord].shape[0]) for coord in coordinates]    # here output lon, lat, etc.
    for coord in coordinates:
        v = dout.createVariable(coord, var['out'+coord].dtype, (coord,))
        v[:] = var['out'+coord][:]
    primaryVar = dout.createVariable(variable, var['out'+variable].dtype, coordinates, fill_value=fillValue)
    primaryVar[:] = var['out'+variable][:]          # transfer array
    maskVar = dout.createVariable(mask, 'i1', coordinates)
    maskVar[:] = var['out'+mask].astype('i1')[:]

    # Transfer variable attributes from input file
    for k,v in dout.variables.iteritems():
        for a in din.variables[k].ncattrs():
            if a == 'scale_factor' or a == 'add_offset' or a == '_FillValue': continue
            v.setncattr(a, din.variables[k].getncattr(a))
        if k == variable:
            try:
#                if fillValue == None: fillValue = din.variables[k].getncattr('_FillValue')        # total kludge
                if fillValue == None: fillValue = default_fillvals['f4']
#                print >>sys.stderr, default_fillvals
#                v.setncattr('_FillValue', fillValue)         # set proper _FillValue for climatology array
                v.setncattr('missing_value', fillValue)
                print >>sys.stderr, 'Setting missing_value for primary variable %s to %f' % (variable, fillValue)
            except:
                print >>sys.stderr, 'writeOutNetcdfVars: Warning, for variable %s no fill value specified or derivable from inputs.' % variable
    din.close()
    dout.close()
    return outFile
    

def contourMap(var, variable, coordinates, n, outFile):
    figFile = os.path.splitext(outFile)[0] + '_hist.png'
    # TODO: Downscale variable array (SST) before contouring, matplotlib is TOO slow on large arrays
    vals = var[variable][:]

    # Fixed color scale, write file, turn off auto borders, set title, reverse lat direction so monotonically increasing??
    imageMap(var[coordinates[1]][:], var[coordinates[0]][:], var[variable][:],
             vmin=-2., vmax=45., outFile=figFile, autoBorders=False,
             title='%s %d-day Mean from %s' % (variable.upper(), n, outFile))
    print >>sys.stderr, 'Writing contour plot to %s' % figFile
    return figFile


def histogram(vals, variable, n, outFile):
    figFile = os.path.splitext(outFile)[0] + '_hist.png'
    M.clf()
#    M.hist(vals, 47, (-2., 45.))
    M.hist(vals, 94)
    M.xlim(-5, 45)
    M.xlabel('SST in degrees Celsius')
    M.ylim(0, 300000)
    M.ylabel('Count')
    M.title('Histogram of %s %d-day Mean from %s' % (variable.upper(), n, outFile))
    M.show()
    print >>sys.stderr, 'Writing histogram plot to %s' % figFile
    M.savefig(figFile)
    return figFile


def dailyFile2date(path, offset=1):
    '''Convert YYYYDOY string in filename to date.'''
    fn = os.path.split(path)[1]
    year = int(fn[offset:offset+4])
    doy = int(fn[offset+5:offset+8])
    return fn[5:15].replace('.', '/')


def formatRegion(r):
    """Format lat/lon region specifier as string suitable for file name."""
    if isinstance(r, str):
        return r
    else:
        strs = [str(i).replace('-', 'm') for i in r]
        return 'region-%s-%sby%s-%s' % tuple(strs)


def formatGrid(r):
    """Format lat/lon grid resolution specifier as string suitable for file name."""
    if isinstance(r, str):
        return r
    else:
        return str(r[0]) + 'by' + str(r[1])


def main(args):
    nEpochs = int(args[0])
    nWindow = int(args[1])
    nNeighbors = int(args[2])
    averager = args[3]
    optimization = args[4]
    mode = args[5]
    nWorkers = int(args[6])
    urlFile = args[7]
    urls = [s.strip() for s in open(urlFile, 'r')]
    if averager == 'gaussInterp':
        results = climByAveragingPeriods(urls, nEpochs, nWindow, nNeighbors, 'sst', 'qual_sst', ['lat', 'lon'],
                             averager=averager, optimization=optimization, averagingConfig=GaussInterpConfig,
                             mode=mode, nWorkers=nWorkers)
    elif averager == 'spatialFilter':
        results = climByAveragingPeriods(urls, nEpochs, nWindow, nNeighbors, 'sst', 'qual_sst', ['lat', 'lon'],
                             averager=averager, optimization=optimization, averagingConfig=SpatialFilterConfig1,
                             mode=mode, nWorkers=nWorkers)
    elif averager == 'pixelMean':
        results = climByAveragingPeriods(urls, nEpochs, nWindow, nNeighbors, 'sst', 'qual_sst', ['lat', 'lon'],
                             averager=averager, optimization=optimization, averagingConfig=PixelMeanConfig,
                             mode=mode, nWorkers=nWorkers)
    else:
        print >>sys.stderr, 'climatology2: Error, averager must be one of', AveragingFunctions.keys()
        sys.exit(1)
    
    if results[0][1] is not None:
        makeMovie([r[1] for r in results], 'clim.mpg')    
    return results

    
if __name__ == '__main__':
    print main(sys.argv[1:])


# Old Tests:
# python climatology2.py 5 5 0 pixelMean   fortran sequential 1 urls_sst_10days.txt
# python climatology2.py 5 5 3 gaussInterp fortran sequential 1 urls_sst_10days.txt

# python climatology2.py 5 5 0 pixelMean   fortran sequential 1 urls_sst_40days.txt
# python climatology2.py 5 5 0 pixelMean   fortran multicore  8 urls_sst_40days.txt
# python climatology2.py 5 5 3 gaussInterp fortran multicore  8 urls_sst_40days.txt

# Old Production:
# python climatology2.py 5 5 0 pixelMean     fortran multicore  16 urls_sst_2015.txt  >& log &
# python climatology2.py 10 10 0 pixelMean     fortran multicore  16 urls_sst_2015.txt  >& log &
# python climatology2.py 5 5 3 gaussInterp   fortran multicore  16 urls_sst_2015.txt  >& log &


# Tests:
# python climatology2.py 5 5 0 pixelMean   fortran sequential 1 urls_sst_daynight_5days_sorted.txt
# python climatology2.py 5 5 0 pixelMean   fortran multicore  4 urls_sst_daynight_5days_sorted.txt
# python climatology2.py 5 5 3 gaussInterp fortran sequential 1 urls_sst_daynight_5days_sorted.txt
# python climatology2.py 5 5 3 gaussInterp fortran multicore  4 urls_sst_daynight_5days_sorted.txt
# python climatology2.py 5 7 1 spatialFilter fortran sequential 1 urls_sst_daynight_5days_sorted.txt

# Test number of neighbors needed:
# python climatology2.py 5 7 3 gaussInterp fortran multicore  4 urls_sst_daynight_20days_sorted.txt


# Production:
# python climatology2.py 5 7 0 pixelMean fortran multicore  4 urls_sst_daynight_2003_2015_sorted.txt
# python climatology2.py 5 7 3 gaussInterp fortran sequential 1 urls_sst_daynight_2003_2015_sorted.txt
# python climatology2.py 5 7 3 gaussInterp fortran multicore  4 urls_sst_daynight_2003_2015_sorted.txt
# python climatology2.py 5 7 1 spatialFilter fortran multicore  4 urls_sst_daynight_2003_2015_sorted.txt

