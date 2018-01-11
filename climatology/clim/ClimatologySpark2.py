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
ClimatologySpark2.py

Compute a multi-epoch (multi-day) climatology from daily SST Level-3 grids.

Simple code to be run on Spark cluster, or using multi-core parallelism on single machine via pysparkling.

"""

import sys, os, urlparse, urllib, re, time, glob
import numpy as N
import matplotlib

matplotlib.use('Agg')
import matplotlib.pylab as M

from clim.variables import getVariables, close
from clim.cache import retrieveFile, CachePath, hdfsCopyFromLocal
from netCDF4 import Dataset, default_fillvals
from clim.plotlib import imageMap, makeMovie

from clim.spatialFilter import spatialFilter
from clim.gaussInterp import gaussInterp  # calls into Fortran version gaussInterp_f.so
# from clim.gaussInterp_slow import gaussInterp_slow as gaussInterp       # pure python, slow debuggable version

from clim.datasets import DatasetList, ModisSst, ModisChlor, MeasuresSsh
from clim.sort import sortByKeys
from clim.split import groupByKeys, splitByNDaysKeyed

from pyspark import SparkContext, SparkConf

DaskClientEndpoint = "daskclient:8786"

VERBOSE = 0
CollectAndTime = 0

# Possible execution modes
# Multicore & cluster modes use pathos pool.map(); Spark mode uses PySpark cluster.
ExecutionModes = ['spark', 'mesos', 'multicore']


# SST L3m 4.6km Metadata
# SST calues are scaled integers in degrees Celsius, lon/lat is 8640 x 4320
# Variable = 'sst', Mask = 'qual_sst', Coordinates = ['lon', 'lat']

# Simple mask and average functions to get us started, then add gaussian interpolation.
# MODIS L3 SST product, qual_sst is [-1, 2] - =0 is best data, can add =1 for better coverage

def qcMask(var, mask): return N.ma.masked_where(mask != 0, var)


# Example Configurations
PixelMeanConfig = {'name': 'pixelMean', 'accumulators': ['count', 'mean', 'M2']}

GaussInterpConfig = {'name': 'gaussInterp',
                     'latGrid': None, 'lonGrid': None,  # None means use input lat/lon grid
                     'wlat': 3, 'wlon': 3,
                     'slat': 0.15, 'slon': 0.15, 'stime': 1,
                     'vfactor': -0.6931, 'missingValue': default_fillvals['f4'],
                     'optimization': 'fortran'}

GaussInterpConfig1a = {'name': 'gaussInterp',
                       'latGrid': None, 'lonGrid': None,  # None means use input lat/lon grid
                       'wlat': 0.30, 'wlon': 0.30,
                       'slat': 0.15, 'slon': 0.15, 'stime': 1,
                       'vfactor': -0.6931, 'missingValue': default_fillvals['f4'],
                       'optimization': 'fortran'}

GaussInterpConfig1b = {'name': 'gaussInterp',
                       'latGrid': None, 'lonGrid': None,  # None means use input lat/lon grid
                       'wlat': 0.08, 'wlon': 0.08,
                       'slat': 0.15, 'slon': 0.15, 'stime': 1,
                       'vfactor': -0.6931, 'missingValue': default_fillvals['f4'],
                       'optimization': 'fortran'}

GaussInterpConfig2 = {'name': 'gaussInterp',
                      'latGrid': (89.5, -89.5, -0.25), 'lonGrid': (-180., 179., 0.25),
                      'wlat': 2., 'wlon': 2.,
                      'slat': 0.15, 'slon': 0.15, 'stime': 1,
                      'vfactor': -0.6931, 'missingValue': default_fillvals['f4'],
                      'optimization': 'fortran'}

FilterGaussian = [[1, 2, 1], [2, 4, 2], [1, 2, 1]]  # divide by 16
FilterLowPass = [[1, 1, 1], [1, 1, 1], [1, 1, 1]]  # divide by 9

SpatialFilterConfig1 = {'name': 'spatialFilter', 'normalization': 16.,
                        'spatialFilter': N.array(FilterGaussian, dtype=N.int32),
                        'missingValue': default_fillvals['f4']}

SpatialFilterConfig2 = {'name': 'spatialFilter', 'normalization': 9.,
                        'spatialFilter': N.array(FilterLowPass, dtype=N.int32),
                        'missingValue': default_fillvals['f4']}

# Three kinds of averaging supported
AveragingFunctions = {'pixelMean': None, 'gaussInterp': gaussInterp, 'spatialFilter': spatialFilter}
AveragingConfigs = {'pixelMean': PixelMeanConfig, 'gaussInterp': GaussInterpConfig1a,
                    'spatialFilter': SpatialFilterConfig1}


def climByAveragingPeriods(urls,  # list of (daily) granule URLs for a long time period (e.g. 15 years), *SORTED* by DOY
                           datasetInfo,  # class holding dataset metadata
                           nEpochs,  # compute a climatology for every N epochs (days) by 'averaging'
                           nWindow,  # number of epochs in window needed for averaging
                           variable,  # name of primary variable in file
                           mask,  # name of mask variable
                           coordinates,  # names of coordinate arrays to read and pass on (e.g. 'lat' and 'lon')
                           reader,  # reader function that reads the variable from the file into a numpy masked array
                           outHdfsPath,
                           # HDFS path to write outputs to (i.e. netCDF file containing stats and optional contour plot)
                           splitter,  # split function to use to partition the input URL list
                           masker=qcMask,  # mask function to compute mask from mask variable
                           averager='pixelMean',
                           # averaging function to use, one of ['pixelMean', 'gaussInterp', 'spatialFilter']
                           averagingConfig={},
                           # dict of parameters to control the averaging function (e.g. gaussInterp)
                           sparkConfig='mesos,8,16',
                           # Map across time periods of N-days for concurrent work, executed by:
                           # 'spark to use PySpark or 'multicore' using pysparkling
                           # 'spark,n,m' means use N executors and M partitions
                           averagingFunctions=AveragingFunctions,  # dict of possible averaging functions
                           legalModes=ExecutionModes,  # list of possible execution modes
                           cachePath=CachePath  # directory to cache retrieved files in
                           ):
    '''Compute a climatology every N days by applying a mask and averaging function.
Writes the climatology average, attributes of the primary variable, and the coordinate arrays to a series of netCDF files.
Optionally generates a contour plot of each N-day climatology for verification.
    '''
    try:
        averageFn = averagingFunctions[averager]
    except:
        print >> sys.stderr, 'climatology: Error, Averaging function must be one of: %s' % str(averagingFunctions)
        sys.exit(1)
    if 'accumulators' in averagingConfig:
        accumulators = averagingConfig['accumulators']

    # Split daily files into N-day periods for parallel tasks (e.g. 5-day clim means 73 periods)
    # Keyed by integer DOY
    urls = sortByKeys(urls, datasetInfo.getKeys)
    urlSplits = splitter(urls, nEpochs)
    urlSplits = groupByKeys(urlSplits)
    print >> sys.stderr, 'Number of URL splits = ', len(urlSplits)
    if VERBOSE: print >> sys.stderr, urlSplits
    if len(urlSplits) == 0: sys.exit(1)

    # Compute per-pixel statistics in parallel
    with Timer("Parallel Stats"):
        if sparkConfig.startswith('dask'):
            outputFiles = parallelStatsDaskSimple(urlSplits, datasetInfo, nEpochs, variable, mask, coordinates, reader,
                                                  outHdfsPath,
                                                  averagingConfig, sparkConfig, accumulators)
        else:
            outputFiles = parallelStatsSparkSimple(urlSplits, datasetInfo, nEpochs, variable, mask, coordinates, reader,
                                                   outHdfsPath,
                                                   averagingConfig, sparkConfig, accumulators)
    return outputFiles


def parallelStatsSparkSimple(urlSplits, ds, nEpochs, variable, mask, coordinates, reader, outHdfsPath, averagingConfig,
                             sparkConfig,
                             accumulators=['count', 'mean', 'M2', 'min', 'max']):
    '''Compute N-day climatology statistics in parallel using PySpark or pysparkling.'''
    with Timer("Configure Spark"):
        sparkContext, numExecutors, numPartitions = configureSpark(sparkConfig, appName='parallelClimStats')

    urlsRDD = sparkContext.parallelize(urlSplits, numPartitions)  # partition N-day periods into tasks

    with CollectTimer("Map-reduce over %d partitions" % numPartitions):  # map to update accumulators in parallel
        accum = urlsRDD.map(lambda urls: accumulate(urls, variable, mask, coordinates, reader, accumulators))

    with CollectTimer("Compute merged statistics"):
        stats = accum.map(statsFromAccumulators)  # compute final statistics from accumulators

    with Timer("Write out climatology and optionally plot"):  # write stats to netCDF file
        outputFiles = stats.map(
            lambda s: writeAndPlot(s, ds, variable, coordinates, nEpochs, averagingConfig, outHdfsPath, plot=False)) \
            .collect()
    return outputFiles


def parallelStatsSpark(urlSplits, ds, nEpochs, variable, mask, coordinates, reader, outHdfsPath, averagingConfig,
                       sparkConfig,
                       accumulators=['count', 'mean', 'M2', 'min', 'max']):
    '''Compute N-day climatology statistics in parallel using PySpark or pysparkling.'''
    with Timer("Configure Spark"):
        sparkContext, numExecutors, numPartitions = configureSpark(sparkConfig, appName='parallelClimStats')

    urlsRDD = sparkContext.parallelize(urlSplits, numPartitions)  # partition N-day periods into tasks

    with CollectTimer("Map-reduce over %d partitions" % numPartitions):
        merged = urlsRDD.map(lambda urls: accumulate(urls, variable, mask, coordinates, reader, accumulators)) \
            .reduceByKey(combine)  # map-reduce to update accumulators in parallel

    with CollectTimer("Compute merged statistics"):
        stats = merged.map(statsFromAccumulators)  # compute final statistics from merged accumulators

    with Timer("Write out climatology and optionally plot"):
        outputFiles = stats.map(
            lambda s: writeAndPlot(s, ds, variable, coordinates, nEpochs, averagingConfig, outHdfsPath, plot=False)) \
            .collect()
    return outputFiles


def parallelStatsPipeline(urls, ds, nEpochs, variable, mask, coordinates, reader, averagingConfig, outHdfsPath,
                          accumulators=['count', 'mean', 'M2', 'min', 'max']):
    '''Hide entire pipeline in a single function.'''
    outputFile = writeAndPlot(
        statsFromAccumulators(accumulate(urls, variable, mask, coordinates, reader, accumulators)),
        ds, variable, coordinates, nEpochs, averagingConfig, outHdfsPath, plot=False)
    return outputFile


def parallelStatsDaskSimple(urlSplits, ds, nEpochs, variable, mask, coordinates, reader, outHdfsPath, averagingConfig,
                            sparkConfig,
                            accumulators=['count', 'mean', 'M2', 'min', 'max']):
    '''Compute N-day climatology statistics in parallel using PySpark or pysparkling.'''
    if not sparkConfig.startswith('dask,'):
        print >> sys.stderr, "dask: configuration must be of form 'dask,n'"
        sys.exit(1)
    numPartitions = int(sparkConfig.split(',')[1])

    with Timer("Configure Dask distributed"):
        from distributed import Client, as_completed
        client = Client(DaskClientEndpoint)

    print >> sys.stderr, 'Starting parallel Stats using Dask . . .'
    start = time.time()
    futures = client.map(
        lambda urls: parallelStatsPipeline(urls, ds, nEpochs, variable, mask, coordinates, reader, averagingConfig,
                                           outHdfsPath, accumulators), urlSplits)

    outputFiles = []
    for future in as_completed(futures):
        outputFile = future.result()
        outputFiles.append(outputFile)
        end = time.time()
        print >> sys.stderr, "parallelStats: Completed %s in %0.3f seconds." % (outputFile, (end - start))
    return outputFiles


def writeAndPlot(stats, ds, variable, coordinates, nEpochs, averagingConfig, outHdfsPath, plot=False):
    '''Write statistics to a netCDF4 file and optionally make a contour plot.'''
    key, stats = stats
    doy = int(key)
    urls = stats['_meta']['urls']
    #    outFile = 'A%03d.L3m_%s_%dday_clim_%s.nc' % (doy, variable, nEpochs, averagingConfig['name'])    # mark each file with first day in period
    outFile = ds.genOutputName(doy, variable, nEpochs, averagingConfig)
    firstInputFile = retrieveFile(urls[0])

    outFile = writeStats(stats, firstInputFile, variable, coordinates, outFile)  # write to netCDF4 file
    outHdfsFile = hdfsCopyFromLocal(outFile, outHdfsPath)

    if plot:
        coords = readCoordinates(firstInputFile, coordinates)
        plotFile = contourMap(stats['mean'], variable, coords, nEpochs, outFile + '.png')
        plotHdfsFile = hdfsCopyFromLocal(plotFile, outHdfsPath)
        return (outHdfsFile, plotHdfsFile)
    else:
        return outHdfsFile


def configureSpark(sparkConfig, appName, memoryPerExecutor='4G', coresPerExecutor=1):
    mode, numExecutors, numPartitions = sparkConfig.split(',')
    numExecutors = int(numExecutors)
    print >> sys.stderr, 'numExecutors = ', numExecutors
    numPartitions = int(numPartitions)
    print >> sys.stderr, 'numPartitions = ', numPartitions
    if mode == 'multicore':
        print >> sys.stderr, 'Using pysparkling'
        import pysparkling
        sc = pysparkling.Context()
    else:
        print >> sys.stderr, 'Using PySpark'
        sparkMaster = mode
        spConf = SparkConf()
        spConf.setAppName(appName)
        spConf.set("spark.executorEnv.HOME",
                   os.path.join(os.getenv('HOME'), 'spark_exec_home'))
        spConf.set("spark.executorEnv.PYTHONPATH", os.getcwd())
        spConf.set("spark.executor.memory", memoryPerExecutor)
        print >> sys.stderr, 'memoryPerExecutor = ', memoryPerExecutor
        try:
            sparkMaster = SparkMasterOverride
        except:
            pass
        if sparkMaster[:5] == "mesos":
            spConf.set("spark.cores.max", numExecutors)
        else:
            # Spark master is YARN or local[N]
            spConf.set("spark.executor.instances", numExecutors)
            spConf.set("spark.executor.cores", coresPerExecutor)
            spConf.setMaster(sparkMaster)
        sc = SparkContext(conf=spConf)
    return sc, numExecutors, numPartitions


def readAndMask(url, variable, mask=None, cachePath=CachePath, hdfsPath=None):
    '''Read a variable from a netCDF or HDF file and return a numpy masked array.
If the URL is remote or HDFS, first retrieve the file into a cache directory.
    '''
    v = None
    if mask:
        variables = [variable, mask]
    else:
        variables = [variable]
    try:
        path = retrieveFile(url, cachePath, hdfsPath)
    except:
        print >> sys.stderr, 'readAndMask: Error, continuing without file %s' % url
        return v

    try:
        print >> sys.stderr, 'Reading variable %s from %s' % (variable, path)
        var, fh = getVariables(path, variables, arrayOnly=True,
                               set_auto_mask=True)  # return dict of variable objects by name
        v = var[
            variable]  # could be masked array
        if v.shape[0] == 1: v = v[0]  # throw away trivial time dimension for CF-style files
        if VERBOSE: print >> sys.stderr, 'Variable range: %fs to %f' % (v.min(), v.max())
        close(fh)
    except:
        print >> sys.stderr, 'readAndMask: Error, cannot read variable %s from file %s' % (variable, path)

    return v


def readCoordinates(path, coordinates=['lat', 'lon']):
    '''Read coordinate arrays from local netCDF file.'''
    var, fh = getVariables(path, coordinates, arrayOnly=True, set_auto_mask=True)
    close(fh)
    return [var[k] for k in coordinates]


def accumulate(urls, variable, maskVar, coordinates, reader=readAndMask,
               accumulators=['count', 'mean', 'M2', 'min', 'max'], cachePath=CachePath, hdfsPath=None):
    '''Accumulate data into statistics accumulators like count, sum, sumsq, min, max, M3, M4, etc.'''
    keys, urls = urls
    print >> sys.stderr, 'Updating accumulators %s for key %s' % (str(accumulators), str(keys))
    accum = {}
    accum['_meta'] = {'urls': urls, 'coordinates': coordinates}
    for i, url in enumerate(urls):
        v = reader(url, variable, maskVar, cachePath, hdfsPath)
        if v is None: continue

        if i == 0:
            for k in accumulators:
                if k[0] == '_': continue
                if k == 'min':
                    accum[k] = default_fillvals['f8'] * N.ones(v.shape, dtype=N.float64)
                elif k == 'max':
                    accum[k] = -default_fillvals['f8'] * N.ones(v.shape, dtype=N.float64)
                elif k == 'count':
                    accum[k] = N.zeros(v.shape, dtype=N.int64)
                else:
                    accum[k] = N.zeros(v.shape, dtype=N.float64)

        if N.ma.isMaskedArray(v):
            if 'count' in accumulators:
                accum['count'] += ~v.mask
            if 'min' in accumulators:
                accum['min'] = N.ma.minimum(accum['min'], v)
            if 'max' in accumulators:
                accum['max'] = N.ma.maximum(accum['max'], v)

            v = N.ma.filled(v, 0.)
        else:
            if 'count' in accumulators:
                accum['count'] += 1
            if 'min' in accumulators:
                accum['min'] = N.minimum(accum['min'], v)
            if 'max' in accumulators:
                accum['max'] = N.maximum(accum['max'], v)

        if 'mean' in accumulators:
            n = accum['count']
            #            mask = N.not_equal(n, 0)
            # subtract running mean from new values, eliminate roundoff errors
            #            delta = N.choose(mask, (0, v - accum['mean']))
            delta = v - accum['mean']
            #            delta_n = N.choose(mask, (0, delta/n))
            delta_n = delta / n
            delta_n = N.nan_to_num(delta_n)  # set to zero if n=0 caused a NaN
            accum['mean'] += delta_n
        if 'M2' in accumulators:
            term = delta * delta_n * (n - 1)
            accum['M2'] += term
    return (keys, accum)


def combine(a, b):
    '''Combine accumulators by summing.'''
    print >> sys.stderr, 'Combining accumulators . . .'
    if 'urls' in a['_meta'] and 'urls' in b['_meta']:
        a['_meta']['urls'].extend(b['_meta']['urls'])
    if 'mean' in a:
        ntotal = a['count'] + b['count']
        #        mask = N.not_equal(ntotal, 0)
        #        a['mean'] = N.choose(mask, (0, (a['mean'] * a['count'] + b['mean'] * b['count']) / ntotal))
        a['mean'] = (a['mean'] * a['count'] + b['mean'] * b['count']) / ntotal
    if 'min' in a:
        if N.ma.isMaskedArray(a):
            a['min'] = N.ma.minimum(a['min'], b['min'])
        else:
            a['min'] = N.minimum(a['min'], b['min'])
    if 'max' in a:
        if N.ma.isMaskedArray(a):
            a['max'] = N.ma.maximum(a['max'], b['max'])
        else:
            a['max'] = N.maximum(a['max'], b['max'])
    for k in a.keys():
        if k[0] == '_': continue
        if k != 'mean' and k != 'min' and k != 'max':
            a[k] += b[k]  # just sum count and other moments
    return a


def statsFromAccumulators(accum):
    '''Compute final statistics from accumulators.'''
    print >> sys.stderr, 'Computing statistics from accumulators . . .'
    keys, accum = accum

    # Mask all of the accumulator arrays for zero counts
    if 'count' in accum:
        accum['count'] = N.ma.masked_less_equal(accum['count'], 0, copy=False)
        mask = accum['count'].mask
        for k in accum:
            if k[0] == '_': continue
            if k != 'count':
                accum[k] = N.ma.array(accum[k], copy=False, mask=mask)

    # Compute stats
    stats = {}
    if '_meta' in accum:
        stats['_meta'] = accum['_meta']
    if 'count' in accum:
        stats['count'] = accum['count']
    if 'min' in accum:
        stats['min'] = accum['min']
    if 'max' in accum:
        stats['max'] = accum['max']
    if 'mean' in accum:
        stats['mean'] = accum['mean']
    if 'M2' in accum:
        stats['stddev'] = N.sqrt(accum['M2'] / (accum['count'] - 1))

        # Convert stats arrays to masked arrays, keeping only cells with count > 0
    #    mask = stats['count'] <= 0
    #    for k in stats:
    #        if k[0] == '_': continue
    #        stats[k] = N.ma.masked_where(mask, stats[k], copy=False)
    return (keys, stats)


def writeStats(stats, inputFile, variable, coordinates, outFile, format='NETCDF4', cachePath='cache'):
    '''Write out stats arrays to netCDF with some attributes.'''
    if os.path.exists(outFile): os.unlink(outFile)
    dout = Dataset(outFile, 'w', format=format)
    print >> sys.stderr, 'Writing stats for variable %s to %s ...' % (variable, outFile)
    print >> sys.stderr, 'Shape:', stats['mean'].shape
    dout.setncattr('variable', variable)
    dout.setncattr('urls', str(stats['_meta']['urls']))

    din = Dataset(inputFile, 'r')
    # Transfer global attributes from input file
    #    for a in din.ncattrs():
    #        dout.setncattr(a, din.getncattr(a))

    print >> sys.stderr, 'Using coordinates & attributes from %s' % inputFile
    coordinatesSave = coordinates
    try:
        coordinatesFromFile = din.variables[variable].getncattr('coordinates')
        if 'lat' in coorindatesFromFile.lower() and 'lon' in coordinatesFromFile.lower():
            coordinates = coordinatesFromFile.split()
            if coordinates[0].lower() == 'time':   coordinates = coordinates[
                                                                 1:]  # discard trivial time dimension for CF-style files
        else:
            coordinates = coordinatesSave  # use input coordinates
    except:
        if coordinates is None or len(coordinates) == 0:
            coordinates = ('lat', 'lon')  # kludge: another possibility

    # Add dimensions and variables, copying data                                                                          
    coordDim = [dout.createDimension(coord, din.variables[coord].shape[0]) for coord in
                coordinates]  # here lat, lon, alt, etc.
    for coord in coordinates:
        var = dout.createVariable(coord, din.variables[coord].dtype, (coord,))
        var[:] = din.variables[coord][:]

    # Add stats variables                                                                                                 
    for k, v in stats.items():
        if k[0] == '_': continue
        var = dout.createVariable(k, stats[k].dtype, coordinates)
        fillVal = default_fillvals[v.dtype.str.strip("<>")]  # remove endian part of dtype to do lookup
        #        print >>sys.stderr, "Setting _FillValue attribute for %s to %s" % (k, fillVal)
        var.setncattr('_FillValue', fillVal)
        var[:] = v[:]

        # Add attributes from variable in input file (does this make sense?)
        if k == 'count': continue
        try:
            vin = din.variables[variable]
            for a in vin.ncattrs():
                if a == 'scale_factor' or a == 'add_offset' or a == '_FillValue': continue
                var.setncattr(a, vin.getncattr(a))
        except KeyError:
            pass

    din.close()
    dout.close()
    return outFile


# CollectTimer context manager
class CollectTimer(object):
    '''Automatically collect Spark result and do timing.'''

    def __init__(self, name):
        self.name = name
        self._collect = CollectAndTime

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, ty, val, tb):
        if not self._collect:
            return False
        else:
            # what goes here??
            end = time.time()
            print >> sys.stderr, "timer: " + self.name + ": %0.3f seconds" % (end - self.start)
            sys.stdout.flush()
            return False


# Timer context manager
class Timer(object):
    '''Automatically collect Spark result and do timing.'''

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, ty, val, tb):
        end = time.time()
        print >> sys.stderr, "timer: " + self.name + ": %0.3f seconds" % (end - self.start)
        sys.stdout.flush()
        return False


def contourMap(var, variable, coordinates, n, plotFile):
    '''Make contour plot for the var array using coordinates vector for lat/lon coordinates.'''
    # TODO: Downscale variable array (SST) before contouring, matplotlib is TOO slow on large arrays
    # Fixed color scale, write file, turn off auto borders, set title, etc.
    lats = coordinates[0][:]
    lons = coordinates[1][:]
    if lats[1] < lats[0]:  # if latitudes decreasing, reverse
        lats = N.flipud(lats)
        var = N.flipud(var[:])

    imageMap(lons, lats, var,
             vmin=-2., vmax=45., outFile=plotFile, autoBorders=False,
             title='%s %d-day Mean from %s' % (variable.upper(), n, os.path.splitext(plotFile)[0]))
    print >> sys.stderr, 'Writing contour plot to %s' % plotFile
    return plotFile


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
    print >> sys.stderr, 'Writing histogram plot to %s' % figFile
    M.savefig(figFile)
    return figFile


def computeClimatology(datasetName, nEpochs, nWindow, averager, outHdfsPath, sparkConfig):
    '''Compute an N-day climatology for the specified dataset and write the files to HDFS.'''
    if averager not in AveragingFunctions:
        print >> sys.stderr, 'computeClimatology: Error, averager %s must be in set %s' % (
            averager, str(AveragingFunctions.keys()))
        sys.exit(1)
    try:
        ds = DatasetList[datasetName]  # get dataset metadata class
    except:
        print >> sys.stderr, 'computeClimatology: Error, %s not in dataset list %s.' % (datasetName, str(DatasetList))
        sys.exit(1)
    urls = glob.glob(ds.UrlsPath)
    with Timer("climByAveragingPeriods"):
        results = climByAveragingPeriods(urls, ds, nEpochs, nWindow, ds.Variable, ds.Mask, ds.Coordinates,
                                         getattr(ds, "readAndMask", readAndMask), outHdfsPath, ds.split,
                                         averager=averager, averagingConfig=AveragingConfigs[averager],
                                         sparkConfig=sparkConfig)
    return results


def main(args):
    dsName = args[0]
    nEpochs = int(args[1])
    nWindow = int(args[2])
    averager = args[3]
    sparkConfig = args[4]
    outHdfsPath = args[5]

    results = computeClimatology(dsName, nEpochs, nWindow, averager, outHdfsPath, sparkConfig)

    if isinstance(results[0], tuple):
        makeMovie([r[1] for r in results], 'clim.mpg')
    return results


if __name__ == '__main__':
    print main(sys.argv[1:])


# Debug Tests:
# python ClimatologySpark2.py ModisSst    5 5 pixelMean   multicore,1,1   cache/clim
# python ClimatologySpark2.py ModisChlor  5 5 pixelMean   multicore,1,1   cache/clim
# python ClimatologySpark2.py MeasuresSsh 5 5 pixelMean   multicore,1,1   cache/clim
# python ClimatologySpark2.py ModisSst    5 5 pixelMean   multicore,2,2  cache/clim

# Production:
# time python ClimatologySpark2.py ModisSst    5 5 pixelMean mesos,37,37 cache/clim
# time python ClimatologySpark2.py ModisChlor  5 5 pixelMean mesos,37,37 cache/clim
# time python ClimatologySpark2.py MeasuresSsh 5 5 pixelMean mesos,37,37 cache/clim

# time python ClimatologySpark2.py ModisSst    5 5 pixelMean dask,37  cache/clim
# time python ClimatologySpark2.py ModisChlor  5 5 pixelMean dask,37  cache/clim
# time python ClimatologySpark2.py MeasuresSsh 5 5 pixelMean dask,37  cache/clim
