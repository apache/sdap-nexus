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
pixelStats.py

Compute a multi-epoch (multi-day) statistics for each lat/lon pixel read from daily Level-3 grids.

Also do statistics roll-ups from daily to monthly, monthly to seasonal, seasonal to yearly, 
yearly to multi-year, and multi-year to total N-year period.

Simple code to be run using Spark or Dpark.

"""

import sys, os, urllib.request, urllib.parse, urllib.error, re, time
import numpy as N
import matplotlib
from functools import reduce
matplotlib.use('Agg')
import matplotlib.pylab as M
from netCDF4 import Dataset, default_fillvals

from .variables import getVariables, close
from .split import splitByMonth
from .cache import retrieveFile, CachePath

#from pyspark import SparkContext    # both imported below when needed
#import dpark

Modes = ['sequential', 'dpark', 'spark']

Accumulators = ['count', 'sum', 'sumsq', 'min', 'max']
Stats = ['count', 'mean', 'stddev', 'min', 'max']

GroupByKeys = ['month', 'season', 'year', '3-year', 'total']

TimeFromFilenameDOY = {'get': ('year', 'doy'), 'regex': re.compile(r'\/A(....)(...)')}


def pixelStats(urls, variable, nPartitions, timeFromFilename=TimeFromFilenameDOY, groupByKeys=GroupByKeys, accumulators=Accumulators,
               cachePath=CachePath, mode='dpark', modes=Modes):
    '''Compute a global (or regional) pixel mean field in parallel, given a list of URL's pointing to netCDF files.'''
    baseKey = groupByKeys[0]
    if baseKey == 'month':
        urlsByKey = splitByMonth(urls, timeFromFilename)
    else:
        print('pixelStats: Unrecognized groupByKey "%s".  Must be in %s' % (baseKey, str(groupByKeys)), file=sys.stderr)
        sys.exit(1)

    if mode == 'sequential':
        accum = [accumulate(u, variable, accumulators) for u in urlsByKey]
        merged = reduce(combine, accum)
        stats = statsFromAccumulators(merged)

    elif mode == 'dpark':
        import dpark
        urls = dpark.parallelize(urlsByKey, nPartitions)                          # returns RDD of URL lists
        accum = urls.map(lambda urls: accumulate(urls, variable, accumulators))   # returns RDD of stats accumulators
        merged = accum.reduce(combine)                                            # merged accumulators on head node
        stats = statsFromAccumulators(merged)                                     # compute final stats from accumulators

    elif mode == 'spark':
        from pyspark import SparkContext
        sc = SparkContext(appName="PixelStats")
        urls = sc.parallelize(urlsByKey, nPartitions)                             # returns RDD of URL lists
        accum = urls.map(lambda urls: accumulate(urls, variable, accumulators))   # returns RDD of stats accumulators
        merged = accum.reduce(combine)                                            # merged accumulators on head node
        stats = statsFromAccumulators(merged)                                     # compute final stats from accumulators

    else:
        stats = None
        if mode not in modes:
            print('pixelStats: Unrecognized mode  "%s".  Must be in %s' % (mode, str(modes)), file=sys.stderr)
            sys.exit(1)
    return stats


def accumulate(urls, variable, accumulators, cachePath=CachePath):
    '''Accumulate data into statistics accumulators like count, sum, sumsq, min, max, M3, M4, etc.'''
    keys, urls = urls
    accum = {}
    for i, url in enumerate(urls):
        try:
            path = retrieveFile(url, cachePath)
            fn = os.path.split(path)[1]
        except:
            print('accumulate: Error, continuing without file %s' % url, file=sys.stderr)
            continue

        try:
            var, fh = getVariables(path, [variable], arrayOnly=True, set_auto_mask=True)   # return dict of variable objects by name
            v = var[variable]   # masked array
            close(fh)
        except:
            print('accumulate: Error, cannot read variable %s from file %s' % (variable, path), file=sys.stderr)
            continue

        if i == 0:
            for k in accumulators:
                if k == 'min':     accum[k] = default_fillvals['f8'] * N.ones(v.shape, dtype=N.float64)
                elif k == 'max':   accum[k] = -default_fillvals['f8'] * N.ones(v.shape, dtype=N.float64)
                elif k == 'count': accum[k] = N.zeros(v.shape, dtype=N.int64)
                else:
                    accum[k] = N.zeros(v.shape, dtype=N.float64)

        if 'count' in accumulators:
            accum['count'] += ~v.mask
        if 'min' in accumulators:
            accum['min'] = N.ma.minimum(accum['min'], v)
        if 'max' in accumulators:
            accum['max'] = N.ma.maximum(accum['max'], v)

        v = N.ma.filled(v, 0.)
        if 'sum' in accumulators:
            accum['sum'] += v
        if 'sumsq' in accumulators:
            accum['sumsq'] += v*v
    return (keys, accum)


def combine(a, b):
    '''Combine accumulators by summing.'''
    keys, a = a
    b = b[1]
    for k in list(a.keys()):
        if k != 'min' and k != 'max':
            a[k] += b[k]
    if 'min' in accumulators:
        a['min'] = N.ma.minimum(a['min'], b['min'])
    if 'max' in accumulators:
        a['max'] = N.ma.maximum(a['max'], b['max'])
    return (('total',), a)


def statsFromAccumulators(accum):
    '''Compute final statistics from accumulators.'''
    keys, accum = accum
    # Mask all of the accumulator arrays
    accum['count'] = N.ma.masked_equal(accum['count'], 0, copy=False)
    mask = accum['count'].mask
    for k in accum:
        if k != 'count':
            accum[k] = N.ma.array(accum[k], copy=False, mask=mask)

    # Compute stats (masked)
    stats = {}
    if 'count' in accum:
        stats['count'] = accum['count']
    if 'min' in accum:
        stats['min'] = accum['min']
    if 'max' in accum:
        stats['max'] = accum['max']
    if 'sum' in accum:
        stats['mean'] = accum['sum'] / accum['count']
    if 'sumsq' in accum:
        stats['stddev'] = N.sqrt(accum['sumsq'] / (accum['count'].astype(N.float32) - 1))
    return (keys, stats)


def writeStats(urls, variable, stats, outFile, copyToHdfsPath=None, format='NETCDF4', cachePath=CachePath):
    '''Write out stats arrays to netCDF with some attributes.
    '''
    keys, stats = stats
    dout = Dataset(outFile, 'w', format=format)
    print('Writing %s ...' % outFile, file=sys.stderr)
    dout.setncattr('variable', variable)
    dout.setncattr('urls', str(urls))
    dout.setncattr('level', str(keys))

    inFile = retrieveFile(urls[0], cachePath)
    din = Dataset(inFile, 'r')
    try:
        coordinates = din.variables[variable].getncattr('coordinates')
        coordinates = coordinates.split()
    except:
        coordinates = ('lat', 'lon')     # kludge: FIX ME
    
    # Add dimensions and variables, copying data
    coordDim = [dout.createDimension(coord, din.variables[coord].shape[0]) for coord in coordinates]     # here lat, lon, alt, etc.
    for coord in coordinates:
        var = dout.createVariable(coord, din.variables[coord].dtype, (coord,))
        var[:] = din.variables[coord][:]

    # Add stats variables
    for k,v in list(stats.items()):
        var = dout.createVariable(k, stats[k].dtype, coordinates)
        var[:] = v[:]

    din.close()
    dout.close()
    return outFile
    

def totalStats(args):
    urlFile = args[0]
    with open(urlFile, 'r') as f:
        urls = [line.strip() for line in f]
    variable = args[1]
    mode = args[2]
    nPartitions = int(args[3])
    outFile = args[4]
    stats = pixelStats(urls, variable, nPartitions, mode=mode)
    outFile = writeStats(urls, variable, stats, outFile)
    return outFile

def main(args):
    return totalStats(args)

if __name__ == '__main__':
    print(main(sys.argv[1:]))


# python pixelStats.py urls_sst_daynight_2003_3days.txt sst sequential 1 modis_sst_stats_test.nc
# python pixelStats.py urls_sst_daynight_2003_4months.txt sst sequential 1 modis_sst_stats_test.nc
# python pixelStats.py urls_sst_daynight_2003_4months.txt sst dpark 4 modis_sst_stats_test.nc
# python pixelStats.py urls_sst_daynight_2003_4months.txt sst spark 4 modis_sst_stats_test.nc

# python pixelStats.py urls_sst_daynight_2003_2015.txt sst dpark 16 modis_sst_stats.nc
# python pixelStats.py urls_sst_daynight_2003_2015.txt sst spark 16 modis_sst_stats.nc

