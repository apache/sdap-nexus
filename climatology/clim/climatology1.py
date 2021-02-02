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
climatology.py

Compute a multi-epoch (multi-day) climatology from daily SST Level-3 grids.

Simple code to be run on Spark cluster, or using multi-core parallelism on single machine.

"""

import sys, os, calendar, urllib.parse, urllib.request, urllib.parse, urllib.error
from datetime import datetime
import numpy as N
from .variables import getVariables, close
#from timePartitions import partitionFilesByKey
from .split import fixedSplit
#from stats import Stats
from pathos.multiprocessing import ProcessingPool as Pool
from .plotlib import imageMap, makeMovie

#from gaussInterp import gaussInterp

VERBOSE = 1

# Possible execution modes
# Multicore & cluster modes use pathos pool.map(); Spark mode uses PySpark cluster.
ExecutionModes = ['sequential', 'multicore', 'cluster', 'spark']

# SST L3m 4.6km Metadata
# SST calues are scaled integers in degrees Celsius, lat/lon is 4320 x 8640
# Variable = 'sst', Mask = 'qual_sst', Coordinates = ['lat', 'lon']

# Generate algorithmic name for N-day Climatology product
SSTClimatologyTemplate = 'SST.L3.Global.Clim.%(period)s.%(date)s.%(version)s.nc'  #??

# Simple mask and average functions to get us started, then add gaussian interpolation.
# MODIS L3 SST product, qual_sst is [-1, 2] - =0 is best data, can add =1 for better coverage
def qcMask(var, mask): return N.ma.array(var, mask=N.ma.make_mask(mask))
#def qcMask(var, mask): return N.ma.masked_where(mask != 0, var)

def average(a): return N.ma.mean(a, axis=0)
#AveragingFunctions = {'pixelAverage': average, 'gaussInterp': gaussInterp}
AveragingFunctions = {'pixelAverage': average, 'gaussInterp': average}


def climByAveragingPeriods(urls,              # list of (daily) granule URLs for a long time period (e.g. a year)
                    nEpochs,                  # compute a climatology for every N epochs (days) by 'averaging'
                    nWindow,                  # number of epochs in window needed for averaging
                    variable,                 # name of primary variable in file
                    mask,                     # name of mask variable
                    coordinates,              # names of coordinate arrays to read and pass on (e.g. 'lat' and 'lon')
                    maskFn=qcMask,            # mask function to compute mask from mask variable
                    averager='pixelAverage',  # averaging function to use, one of ['pixelAverage', 'gaussInterp']
                    mode='sequential',        # Map across time periods of N-days for concurrent work, executed by:
                                              # 'sequential' map, 'multicore' using pool.map(), 'cluster' using pathos pool.map(),
                                              # or 'spark' using PySpark
                    numNodes=1,               # number of cluster nodes to use
                    nWorkers=4,               # number of parallel workers per node
                    averagingFunctions=AveragingFunctions,    # dict of possible averaging functions
                    legalModes=ExecutionModes  # list of possiblel execution modes
                   ):
    '''Compute a climatology every N days by applying a mask and averaging function.
Writes the averaged variable grid, attributes of the primary variable, and the coordinate arrays in a dictionary.
***Assumption:  This routine assumes that the N grids will fit in memory.***
    '''
    try:
        averageFn = averagingFunctions[averager]
    except :
        averageFn = average
        print('climatology: Error, Averaging function must be one of: %s' % str(averagingFunctions), file=sys.stderr)

    urlSplits = [s for s in fixedSplit(urls, nEpochs)]
    if VERBOSE: print(urlSplits, file=sys.stderr)

    def climsContoured(urls):
        n = len(urls)
        var = climByAveraging(urls, variable, mask, coordinates, maskFn, averageFn)
        return contourMap(var, variable, coordinates, n, urls[0])

    if mode == 'sequential':
        plots = list(map(climsContoured, urlSplits))
    elif mode == 'multicore':
        pool = Pool(nWorkers)
        plots = pool.map(climsContoured, urlSplits)        
    elif mode == 'cluster':
        pass
    elif mode == 'spark':
        pass

    plots = list(map(climsContoured, urlSplits))
    print(plots)
    return plots
#    return makeMovie(plots, 'clim.mpg')    


def climByAveraging(urls,                 # list of granule URLs for a time period
                    variable,             # name of primary variable in file
                    mask,                 # name of mask variable
                    coordinates,          # names of coordinate arrays to read and pass on (e.g. 'lat' and 'lon')
                    maskFn=qcMask,        # mask function to compute mask from mask variable
                    averageFn=average     # averaging function to use
                   ):
    '''Compute a climatology over N arrays by applying a mask and averaging function.
Returns the averaged variable grid, attributes of the primary variable, and the coordinate arrays in a dictionary.
***Assumption:  This routine assumes that the N grids will fit in memory.***
    '''
    n = len(urls)
    varList = [variable, mask]
    for i, url in enumerate(urls):
        fn = retrieveFile(url, '~/cache')
        if VERBOSE: print('Read variables and mask ...', file=sys.stderr)
        var, fh = getVariables(fn, varList)            # return dict of variable objects by name
        if i == 0:
            dtype = var[variable].dtype
            shape = (n,) + var[variable].shape
            accum = N.ma.empty(shape, dtype)
        
        v = maskFn(var[variable], var[mask])           # apply quality mask variable to get numpy MA
#        v = var[variable][:]
        accum[i] = v                                   # accumulate N arrays for 'averaging'
        if i+1 != len(urls):                           # keep var dictionary from last file to grab metadata
            close(fh)                                  # REMEMBER:  closing fh loses in-memory data structures

    if VERBOSE: print('Averaging ...', file=sys.stderr)
    coord, fh = getVariables(fn, coordinates)          # read coordinate arrays and add to dict
    for c in coordinates: var[c] = coord[c][:]

    if averageFn == average:
        avg = averageFn(accum)                         # call averaging function
    else:
        var[variable] = accum
        if averageFn == gaussInterp:
            varNames = variable + coordinates
            avg, vweight, status = \
                gaussInterp(var, varNames, latGrid, lonGrid, wlat, wlon, slat, slon, stime, vfactor, missingValue)
        
    var['attributes'] = var[variable].__dict__         # save attributes of primary variable
    var[variable] = avg                                # return primary variable & mask arrays in dict
    var[mask] = N.ma.getmask(avg)

#    close(fh)                                         # Can't close, lose netCDF4.Variable objects, leaking two fh
    return var


def contourMap(var, variable, coordinates, n, url):
    p = urllib.parse.urlparse(url)
    filename = os.path.split(p.path)[1]
    return filename
    outFile = filename + '.png'
    # Downscale variable array (SST) before contouring, matplotlib is TOO slow on large arrays
    vals = var[variable][:]

    # Fixed color scale, write file, turn off auto borders, set title, reverse lat direction so monotonically increasing??
    imageMap(var[coordinates[1]][:], var[coordinates[0]][:], var[variable][:],
             vmin=-2., vmax=45., outFile=outFile, autoBorders=False,
             title='%s %d-day Mean from %s' % (variable.upper(), n, filename))
    print('Writing contour plot to %s' % outFile, file=sys.stderr)
    return outFile


def isLocalFile(url):
    '''Check if URL is a local path.'''
    u = urllib.parse.urlparse(url)
    if u.scheme == '' or u.scheme == 'file':
        if not path.exists(u.path):
            print('isLocalFile: File at local path does not exist: %s' % u.path, file=sys.stderr)
        return (True, u.path)
    else:
        return (False, u.path)


def retrieveFile(url, dir=None):
    '''Retrieve a file from a URL, or if it is a local path then verify it exists.'''
    if dir is None: dir = './'
    ok, path = isLocalFile(url)
    fn = os.path.split(path)[1]
    outPath = os.path.join(dir, fn)
    if not ok:
        if os.path.exists(outPath):
            print('retrieveFile: Using cached file: %s' % outPath, file=sys.stderr)
        else:
            try:
                print('retrieveFile: Retrieving (URL) %s to %s' % (url, outPath), file=sys.stderr)
                urllib.request.urlretrieve(url, outPath)
            except:
                print('retrieveFile: Cannot retrieve file at URL: %s' % url, file=sys.stderr)
                return None
    return outPath    


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
    averager = args[2]
    mode = args[3]
    nWorkers = int(args[4])
    urlFile = args[5]
    urls = [s.strip() for s in open(urlFile, 'r')]
    return climByAveragingPeriods(urls, nEpochs, nWindow, 'sst', 'qual_sst', ['lat', 'lon'],
                                  averager=averager, mode=mode, nWorkers=nWorkers)

    
if __name__ == '__main__':
    print(main(sys.argv[1:]))


# python climatology.py 5 5 pixelAverage sequential 1 urls_sst_10days.txt
# python climatology.py 5 5 gaussianInterp multicore 8 urls_sst_40days.txt

