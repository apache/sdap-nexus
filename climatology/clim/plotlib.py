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
#!/bin/env python

import sys, os, math, types, time, datetime
from urllib.request import urlopen
from urllib.request import urlretrieve
from urllib.parse import urlparse
#import Numeric as N
import numpy as N
import numpy.ma as MA
#import matplotlib.numerix.ma as MA
import matplotlib
matplotlib.use('Agg')
import matplotlib.pylab as M
from mpl_toolkits.basemap import Basemap
#import hdfeos

def echo2(*s): sys.stderr.write(' '.join(map(str, s)) + '\n')
def warn(*s):  echo2('plotlib:', *s)
def die(*s):   warn('Error,',  *s); sys.exit()

CmdOptions = {'MCommand':  ['title', 'xlabel', 'ylabel',  'xlim', 'ylim', 'show'],
              'plot':      ['label', 'linewidth', 'legend', 'axis'],
              'map.plot':  ['label', 'linewidth', 'axis'],
              'map.scatter':  ['norm', 'alpha', 'linewidths', 'faceted', 'hold'],
              'savefig':   ['dpi', 'orientation']
              }

def imageMap(lons, lats, vals, vmin=None, vmax=None, 
             imageWidth=None, imageHeight=None, outFile=None,
             projection='cyl', cmap=M.cm.jet, makeFigure=False,
             borders=[0., -90., 360., 90.], autoBorders=True, borderSlop=10.,
             meridians=[0, 360, 60], parallels=[-60, 90, 30],
             **options
             ):
    lons = normalizeLons(lons)
    if vmin == 'auto': vmin = None
    if vmax == 'auto': vmax = None
    if imageWidth is not None: makeFigure = True
    if projection is None or projection == '': projection = 'cyl'
    if cmap is None or cmap == '': cmap = M.cm.jet
    if isinstance(cmap, bytes) and cmap != '':
        try:
            cmap = eval('M.cm.' + cmap)
        except:
            cmap = M.cm.jet

#    ensureItems(options, {'xlabel': 'Longitude (deg)', 'ylabel': 'Latitude (deg)', \
    ensureItems(options, { \
                     'title': 'An Image Map', 'dpi': 100,
                     'imageWidth': imageWidth or 1024, 'imageHeight': imageHeight or 768})
    if autoBorders:
        borders = [min(lons), min(lats), max(lons), max(lats)]
        borders = roundBorders(borders, borderSlop)

    m = Basemap(borders[0], borders[1], borders[2], borders[3], \
                projection=projection, lon_0=N.average([lons[0], lons[-1]]))

    if makeFigure:
        dpi = float(options['dpi'])
        width = float(imageWidth) / dpi
        if imageHeight is None:
            height = width * m.aspect
        else:
            height = float(imageHeight) / dpi
        f = M.figure(figsize=(width,height)).add_axes([0.1,0.1,0.8,0.8], frameon=True)

    if vmin is not None or vmax is not None: 
        if vmin is None:
            vmin = min(vals)
        else:
            vmin = float(vmin)
        if vmax is None:
            vmax = max(vals)
        else:
            vmax = float(vmax)
        vrange = (vmax - vmin) / 255.
        levels = N.arange(vmin, vmax, vrange/30.)
    else:
        levels = 30

    x, y = m(*M.meshgrid(lons,lats))
    c = m.contourf(x, y, vals, levels, cmap=cmap, colors=None)

    m.drawcoastlines()
    m.drawmeridians(list(range(meridians[0], meridians[1], meridians[2])), labels=[0,0,0,1])
    m.drawparallels(list(range(parallels[0], parallels[1], parallels[2])), labels=[1,1,1,1])
    M.colorbar(c)
    evalKeywordCmds(options)
    if outFile:
        M.savefig(outFile, **validCmdOptions(options, 'savefig'))


# all of these calls below are handled by the evalKeywordCmds line above
#    M.xlim(0,360)
#    M.xlabel(xlabel)
#    M.ylabel(ylabel)
#    M.title(title)
#    M.show()


def imageMap2(lons, lats, vals, vmin=None, vmax=None, 
             imageWidth=None, imageHeight=None, outFile=None,
             projection='cyl', cmap=M.cm.jet, makeFigure=False,
             borders=[0., -90., 360., 90.], autoBorders=True, borderSlop=10.,
             meridians=[0, 360, 60], parallels=[-60, 90, 30],
             **options
             ):
#    lons = normalizeLons(lons)
    if vmin == 'auto': vmin = None
    if vmax == 'auto': vmax = None
    if imageWidth is not None: makeFigure = True
    if projection is None or projection == '': projection = 'cyl'
    if cmap is None or cmap == '': cmap = M.cm.jet
    if isinstance(cmap, bytes) and cmap != '':
        try:
            cmap = eval('M.cm.' + cmap)
        except:
            cmap = M.cm.jet

#    ensureItems(options, {'xlabel': 'Longitude (deg)', 'ylabel': 'Latitude (deg)', \
    ensureItems(options, { \
                     'title': 'An Image Map', 'dpi': 100,
                     'imageWidth': imageWidth or 1024, 'imageHeight': imageHeight or 768})
    if autoBorders:
        borders = [min(min(lons)), min(min(lats)), max(max(lons)), max(max(lats))]
        borders = roundBorders(borders, borderSlop)

    m = Basemap(borders[0], borders[1], borders[2], borders[3], \
                projection=projection, lon_0=N.average([lons.flat[0], lons.flat[-1]]))

    if makeFigure:
        dpi = float(options['dpi'])
        width = float(imageWidth) / dpi
        if imageHeight is None:
            height = width * m.aspect
        else:
            height = float(imageHeight) / dpi
        f = M.figure(figsize=(width,height)).add_axes([0.1,0.1,0.8,0.8], frameon=True)

    if vmin is not None or vmax is not None: 
        if vmin is None:
            vmin = min(min(vals))
        else:
            vmin = float(vmin)
        if vmax is None:
            vmax = max(max(vals))
        else:
            vmax = float(vmax)
        vrange = vmax - vmin
        levels = N.arange(vmin, vmax, vrange/30.)
    else:
        levels = 30

    c = m.contourf(lons, lats, vals, levels, cmap=cmap, colors=None)
    m.drawcoastlines()
    m.drawmeridians(list(range(meridians[0], meridians[1], meridians[2])), labels=[0,0,0,1])
    m.drawparallels(list(range(parallels[0], parallels[1], parallels[2])), labels=[1,1,1,1])
    M.colorbar(c, orientation='horizontal')
    evalKeywordCmds(options)
    if outFile: M.savefig(outFile, **validCmdOptions(options, 'savefig'))


def image2(vals, vmin=None, vmax=None, outFile=None,
           imageWidth=None, imageHeight=None, upOrDown='upper',
           cmap=M.cm.jet, makeFigure=False, **options
          ):
    M.clf()
    M.axes([0, 0, 1, 1])
    if vmin == 'auto': vmin = None
    if vmax == 'auto': vmax = None
    if imageWidth is not None: makeFigure = True
    if cmap is None or cmap == '': cmap = M.cm.jet
    if isinstance(cmap, bytes) and cmap != '':
        try:
            cmap = eval('M.cm.' + cmap)
        except:
            cmap = M.cm.jet

    if makeFigure:
        dpi = float(options['dpi'])
        width = float(imageWidth) / dpi
        height = float(imageHeight) / dpi
        f = M.figure(figsize=(width,height)).add_axes([0.1,0.1,0.8,0.8], frameon=True)

    if vmin is not None or vmax is not None: 
        if vmin is None:
            vmin = min(min(vals))
        else:
            vmin = float(vmin)
        if vmax is None:
            vmax = max(max(vals))
        else:
            vmax = float(vmax)
        vrange = vmax - vmin
        levels = N.arange(vmin, vmax, vrange/30.)
    else:
        levels = 30

    M.contourf(vals, levels, cmap=cmap, origin=upOrDown)
    evalKeywordCmds(options)
    if outFile: M.savefig(outFile, **validCmdOptions(options, 'savefig'))


def marksOnMap(lons, lats, vals=None, vmin=None, vmax=None,
               imageWidth=None, imageHeight=None, outFile=None,
               projection='cyl', cmap=M.cm.jet,
               sizes=80, colors='b', marker='o', makeFigure=False,
               times=None, timeDelta=None,
               borders=[0., -90., 360., 90.], autoBorders=True, borderSlop=10.,
               meridians=[0, 360, 60], parallels=[-60, 90, 30],
               **options
               ):
    lons = normalizeLons(lons)
    if imageWidth is not None: makeFigure = True
    if projection is None: projection = 'cyl'

#    ensureItems(options, {'xlabel': 'Longitude (deg)', 'ylabel': 'Latitude (deg)',
    ensureItems(options, { \
                          'title': 'Markers on a Map', 'dpi': 100,
                          'imageWidth': imageWidth or 1024, 'imageHeight': imageHeight or 768})
    if autoBorders:
        borders = [min(lons), min(lats), max(lons), max(lats)]
        borders = roundBorders(borders, borderSlop)

    m = Basemap(borders[0], borders[1], borders[2], borders[3], \
                projection=projection, lon_0=N.average([lons[0], lons[-1]]))

    if makeFigure:
        dpi = float(options['dpi'])
        width = float(imageWidth) / dpi
        if imageHeight is None:
            height = width * m.aspect
        else:
            height = float(imageHeight) / dpi
        f = M.figure(figsize=(width,height)).add_axes([0.1,0.1,0.8,0.8],frameon=True)

    if vals is not None: 
        if vmin is None or vmin == 'auto':
            vmin = min(vals)
            warn('auto scale min is %f' % vmin)
        else:
            vmin = float(vmin)
        if vmax is None or vmax == 'auto':
	    vmax = max(vals)
        else:
            vmax = float(vmax)
            warn('auto scale max is %f' % vmax)
        vrange = (vmax - vmin) / 255.
        colors = N.array([(val - vmin)/vrange for val in vals])

    if timeDelta is not None:
        if times is not None:
            times = list(map(mkTime, times))
            timeDelta = float(timeDelta) * 60.
            start = roundTime(times[0], 'down', timeDelta)
            end = roundTime(times[-1], 'up', timeDelta)
            plotTimes = arange(start, end+timeDelta, timeDelta)
        else:
            timeDelta = None
            

    g = m.scatter(lons, lats, s=sizes, c=colors, marker=marker, cmap=cmap,
                  vmin=vmin, vmax=vmax, **validCmdOptions(options, 'map.scatter'))

    m.drawcoastlines()
    m.drawmeridians(list(range(meridians[0], meridians[1], meridians[2])), labels=[0,0,0,1])
    m.drawparallels(list(range(parallels[0], parallels[1], parallels[2])), labels=[1,1,1,1])
    M.colorbar(g)
    evalKeywordCmds(options)
    if outFile: M.savefig(outFile, **validCmdOptions(options, 'savefig'))


def plotSwathVar(granules, variable, scaleFactor, title, outFile, filterMin=None, filterMax=None,
                 scaleMin=None, scaleMax=None, imageWidth=None, imageHeight=None,
                 plotType='map', projection='cyl', markerSize=10, **options):
    if filterMin == 'auto': filterMin = None
    if filterMax == 'auto': filterMax = None
#    files = [localize(url) for url in granules if url != 'None']
    files = granules
    imageFiles = []

    for i, file in enumerate(files):
        print('plotSwathVar: Reading %s: %s' % (file, variable))
        localFile = localize(file, retrieve=False)
        if i == 0:
            swath = hdfeos.swaths(file)[0]
#            geoFields = hdfeos.swath_geo_fields(file, swath)
        lat = hdfeos.swath_field_read(file, swath, 'Latitude')
        lon = hdfeos.swath_field_read(file, swath, 'Longitude')
###        time = hdfeos.swath_field_read(file, swath, 'Time')
###        pressure = hdfeos.swath_field_read(file, swath, '???')

        if N.minimum.reduce(lon.flat) < -360. or N.minimum.reduce(lat.flat) < -90.:
            useImageMap = False   # have missing values in lat/lon coord variables
        else:
            useImageMap = True

        dataFields = hdfeos.swath_data_fields(file, swath)
        if '[' not in variable:
            varName = variable
        else:
            varName, slice = variable.split('[')
        if varName not in dataFields:
            die('%s not a variable in %s' % (variable, file))
        if '[' not in variable:
            var = hdfeos.swath_field_read(file, swath, variable) * float(scaleFactor)
        else:
            vals = hdfeos.swath_field_read(file, swath, varName)
            var = eval('['.join(('vals', slice)))
            var = var * float(scaleFactor)

        print('plotSwathVar: Variable range: %f -> %f' % (min(min(var)), max(max(var))))
        if plotType != 'map' or not useImageMap:
            lat = lat.flat; lon = lon.flat; var = var.flat

        if filterMin is not None or filterMax is not None:
            if filterMin is not None and filterMax is None:
                cond = N.greater(var, float(filterMin))
            elif filterMin is None and filterMax is not None:
                cond = N.less(var, float(filterMax))
            else:
                cond = N.logical_and(N.greater(var, float(filterMin)),
                                     N.less(var, float(filterMax)))
            if plotType == 'map' and useImageMap:
                lat = MA.masked_where(cond, lat, copy=0)
                lon = MA.masked_where(cond, lon, copy=0)
                var = MA.masked_where(cond, var, copy=0)
            else:
                lat = N.compress(cond, lat.flat)
                lon = N.compress(cond, lon.flat)
                var = N.compress(cond, var.flat)

        if plotType == 'map':
            imageFile = localFile + '_map.png'
            if useImageMap:
                imageMap2(lon, lat, var, scaleMin, scaleMax, imageWidth, imageHeight,
                          imageFile, projection, autoBorders=False, title=title+' '+file,
                          **options)
            else:
                marksOnMap(lon, lat, var, scaleMin, scaleMax,
                           imageWidth, imageHeight, imageFile, projection,
                           autoBorders=True, title=title+' '+file,
                           sizes=markerSize*markerSize, **options)
        elif plotType == 'hist':
            imageFile = localFile + '_aot_hist.png'
            hist(var, 50, imageFile)
        else:
            die("plotSwathVar: plotType must be 'map' or 'hist'")

        imageFiles.append(imageFile)
    return makeMovie(imageFiles, outFile)


def imageSwathVar(granules, variable, scaleFactor, title, outFile, filterMin=None, filterMax=None,
                 scaleMin=None, scaleMax=None, imageWidth=None, imageHeight=None,
                 plotType='map', projection='cyl', markerSize=10, **options):
    if filterMin == 'auto': filterMin = None
    if filterMax == 'auto': filterMax = None
#    files = [localize(url) for url in granules if url != 'None']
    files = granules
    imageFiles = []; lonLatBounds = []

    for i, file in enumerate(files):
        print('imageSwathVar: Reading %s: %s' % (file, variable))
        localFile = localize(file, retrieve=False)
        if i == 0:
            swath = hdfeos.swaths(file)[0]
#            geoFields = hdfeos.swath_geo_fields(file, swath)
        lat = hdfeos.swath_field_read(file, swath, 'Latitude')
        lon = hdfeos.swath_field_read(file, swath, 'Longitude')
###        time = hdfeos.swath_field_read(file, swath, 'Time')
###        pressure = hdfeos.swath_field_read(file, swath, '???')

        if N.minimum.reduce(lon.flat) < -360. or N.minimum.reduce(lat.flat) < -90.:
            useImageMap = False   # have missing values in lat/lon coord variables
        else:
            useImageMap = True

        dataFields = hdfeos.swath_data_fields(file, swath)
        if '[' not in variable:
            varName = variable
        else:
            varName, slice = variable.split('[')
        if varName not in dataFields:
            die('%s not a variable in %s' % (variable, file))
        if '[' not in variable:
            var = hdfeos.swath_field_read(file, swath, variable) * float(scaleFactor)
        else:
            vals = hdfeos.swath_field_read(file, swath, varName)
            var = eval('['.join(('vals', slice)))
            var = var * float(scaleFactor)

        print('imageSwathVar: Variable range: %f -> %f' % (min(min(var)), max(max(var))))
        if plotType != 'map' or not useImageMap:
            lat = lat.flat; lon = lon.flat; var = var.flat

        if filterMin is not None or filterMax is not None:
            if filterMin is not None and filterMax is None:
                cond = N.greater(var, float(filterMin))
            elif filterMin is None and filterMax is not None:
                cond = N.less(var, float(filterMax))
            else:
                cond = N.logical_and(N.greater(var, float(filterMin)),
                                     N.less(var, float(filterMax)))
            if plotType == 'map' and useImageMap:
                lat = MA.masked_where(cond, lat, copy=0)
                lon = MA.masked_where(cond, lon, copy=0)
                var = MA.masked_where(cond, var, copy=0)
            else:
                lat = N.compress(cond, lat.flat)
                lon = N.compress(cond, lon.flat)
                var = N.compress(cond, var.flat)

        lonLatBound = (min(min(lon)), min(min(lat)), max(max(lon)), max(max(lat)))
        lonLatBounds.append(lonLatBound)

        if plotType == 'map':
            imageFile = localFile + '_image.png'
            if useImageMap:
                upOrDown = 'upper'
                if lat[0, 0] < lat[-1, 0]: upOrDown = 'lower'
                if lon[0, 0] > lon[0, -1]: var = fliplr(var)
                image2(var, scaleMin, scaleMax, imageFile, upOrDown=upOrDown, **options)
#                plainImage2(var, imageFile)
            else:
                marksOnMap(lon, lat, var, scaleMin, scaleMax,
                           imageWidth, imageHeight, imageFile, projection,
                           autoBorders=True, title=title+' '+file,
                           sizes=markerSize*markerSize, **options)
        elif plotType == 'hist':
            imageFile = localFile + '_aot_hist.png'
            hist(var, 50, imageFile)
        else:
            die("plotSwathVar: plotType must be 'map' or 'hist'")

        imageFiles.append(imageFile)
    print("imageSwathVar results:", imageFiles)
    return (imageFiles, lonLatBounds)

def fliplr(a):
    b = N.array(a)
    for i in range(b.shape[0]):
        b[i,:] = a[i,::-1]
    return b

def plainImage2(var, imageFile):
    M.clf()
    M.figimage(var)
    M.savefig(imageFile)


def maskNcVar(inNcFile, outNcFile, outVars=[], writeMode='include',
              conditionVar=None, filterMin=None, filterMax=None,
              varToMask=None, maskValue=-9999.):
    nc = NC(inNcFile)
    if conditionVar is not None:
        try:
            condVar = nc.variables[conditionVar]
        except:
            die('maskNcVar: Variable %s not in ncFile %s' % (conditionVar, inNcFile))
        if varToMask is None: die('maskNcVar: Must specify variable to mask with condition.')
        try:
            var = nc.variables[varToMask]
        except:
            die('maskNcVar: Variable %s not in ncFile %s' % (varToMask, inNcFile))

        if filterMin is not None or filterMax is not None:
            if filterMin is not None and filterMax is None:
                cond = N.greater(var, float(filterMin))
            elif filterMin is None and filterMax is not None:
                cond = N.less(var, float(filterMax))
            else:
                cond = N.logical_and(N.greater(var, float(filterMin)),
                                     N.less(var, float(filterMax)))
            var = N.putmask(var, cond, float(maskValue))
        outVars = list( set(outVars).add(conditionVar).add(varToMask) )
    return subsetNcFile(inNcFile, outVars, outNcFile)

def subsetNcFile(inNcFile, outNcFile, outVars, writeMode='include'):
    if writeMode == '' or writeMode == 'auto': writeMode = 'include'
    inf = NC(inNcFile)
    outf = NC(outNcFile, 'w')
    return outNcFile


def hist(x, bins, outFile=None, **options):
    if outFile: M.clf()
    M.hist(x, bins, **options)
    if outFile: M.savefig(outFile, **validCmdOptions(options, 'savefig'))

def localize(url, retrieve=True):
    scheme, netloc, path, params, query, frag = urlparse(url)
    dir, file = os.path.split(path)
    if retrieve: urlretrieve(url, file)
    return file

def makeMovie(files, outFile):
    if len(files) > 1:
        outMovieFile = os.path.splitext(outFile)[0] + '.mpg'
        cmd = 'convert ' + ' '.join(files) + ' ' + outMovieFile
        os.system(cmd)
        warn('Wrote movie ' + outMovieFile)
        return outMovieFile
    else:
        os.rename(files[0], outFile)
        return outFile

def mkTime(timeStr):
    """Make a time object from a date/time string YYYY-MM-DD HH:MM:SS"""
    from time import mktime, strptime
    return mktime( strptime(timeStr, '%Y %m %d %H %M %S') )

def roundTime(time, resolution, direction):
    pass

def roundBorders(borders, borderSlop=10.):
    b0 = roundBorder(borders[0], 'down', borderSlop,   0.)
    b1 = roundBorder(borders[1], 'down', borderSlop, -90.)
    b2 = roundBorder(borders[2],   'up', borderSlop, 360.)
    b3 = roundBorder(borders[3],   'up', borderSlop,  90.)
    return [b0, b1, b2, b3]

def roundBorder(val, direction, step, end):
    if direction == 'up':
        rounder = math.ceil
        slop = step
    else:
        rounder = math.floor
        slop = -step
###    v = rounder(val/step) * step + slop
    v = rounder(val/step) * step
    if abs(v - end) < step+1.: v = end
    return v

def normalizeLon(lon):
    if lon < 0.: return lon + 360.
    if lon > 360.: return lon - 360.
    return lon

def normalizeLons(lons):
    return N.array([normalizeLon(lon) for lon in lons])


def plotColumns(specs, groupBy=None, outFile=None, rmsDiffFrom=None, floatFormat=None,
                colors='bgrcmyk', markers='+x^svD<4>3', **options):
    if groupBy:
        plotColumnsGrouped(specs, groupBy, outFile, rmsDiffFrom, floatFormat,
                           colors, markers, **options)
    else:
        plotColumnsSimple(specs, outFile, rmsDiffFrom, floatFormat,
                          colors, markers, **options)


def plotColumnsSimple(specs, outFile=None, rmsDiffFrom=None, floatFormat=None,
                colors='bgrcmyk', markers='+x^svD<4>3', **options):
    """Plot olumns of numbers from one or more data files.
    Each plot spec. contains a filename and a list of labelled columns:
      e.g., ('file1', 'xlabel:1,ylabel1:4,ylabel2:2,ylabel3:13)
    Bug:  For the moment, only have 7 different colors and 10 different markers.
    """
    ensureItems(options, {'legend': True})
    ydataMaster = None
    for spec in specs:
        file, columns = spec          # each spec is a (file, columnList) pair
        columns = columns.split(',')  # each columnList is a comma-separated list of named columns
        # Each named column is a colon-separated pair or triple 'label:integer[:style]'
        # Column indices are one-based.
        # Styles are concatenated one-char flags like 'go' for green circles or
        # 'kx-' for black X's with a line.
        fields = N.array([list(map(floatOrMiss, line.split())) for line in open(file, 'r')])
        xcol = columns.pop(0)  # first column in list is the x axis
        xlabel, xcol, xstyle = splitColumnSpec(xcol)
        xdata = fields[:,xcol-1]
        markIndex = 0
        for ycol in columns:
            ylabel, ycol, ystyle = splitColumnSpec(ycol)
            if ystyle is None: ystyle = colors[markIndex] + markers[markIndex]            
            ydata = fields[:,ycol-1]  # all other columns are multiple y plots
            if rmsDiffFrom:
                if ydataMaster is None:
                    ydataMaster = ydata    # kludge: must be first ycol in first file
                    ylabelMaster = ylabel
                else:
                    s = diffStats(ylabelMaster, ydataMaster, ylabel, ydata)
                    print(s.format(floatFormat), file=sys.stderr)
                    n, mean, sigma, min, max, rms = s.calc()
                    ylabel = ylabel + ' ' + floatFormat % rms
            M.plot(xdata, ydata, ystyle, label=ylabel)
            markIndex += 1
    evalKeywordCmds(options)
    if outFile: M.savefig(outFile)    


def plotColumnsGrouped(specs, groupBy, outFile=None, rmsDiffFrom=None, floatFormat=None,
                colors='bgrcmyk', markers='+x^svD<4>3', **options):
    """Plot olumns of numbers from one or more data files.
    Each plot spec. contains a filename and a list of labelled columns:
      e.g., ('file1', 'xlabel:1,ylabel1:4,ylabel2:2,ylabel3:13)
    Bug:  For the moment, only have 7 different colors and 10 different markers.
    """
    ensureItems(options, {'legend': True})
    ydataMaster = None
    for spec in specs:
        file, columns = spec          # each spec is a (file, columnList) pair
        columns = columns.split(',')  # each columnList is a comma-separated list of named columns
        # Each named column is a colon-separated pair or triple 'label:integer[:style]'
        # Column indices are one-based.
        # Styles are concatenated one-char flags like 'go' for green circles or
        # 'kx-' for black X's with a line.
        fields = N.array([list(map(floatOrMiss, line.split())) for line in open(file, 'r')])
        xcol = columns.pop(0)  # first column in list is the x axis
        xlabel, xcol, xstyle = splitColumnSpec(xcol)
        xdata = fields[:,xcol-1]
        markIndex = 0
        for ycol in columns:
            ylabel, ycol, ystyle = splitColumnSpec(ycol)
            if ystyle is None: ystyle = colors[markIndex] + markers[markIndex]            
            ydata = fields[:,ycol-1]  # all other columns are multiple y plots
            if rmsDiffFrom:
                if ydataMaster is None:
                    ydataMaster = ydata    # kludge: must be first ycol in first file
                    ylabelMaster = ylabel
                else:
                    s = diffStats(ylabelMaster, ydataMaster, ylabel, ydata)
                    print(s.format(floatFormat), file=sys.stderr)
                    n, mean, sigma, min, max, rms = s.calc()
                    ylabel = ylabel + ' ' + floatFormat % rms
            M.plot(xdata, ydata, ystyle, label=ylabel)
            markIndex += 1
    evalKeywordCmds(options)
    if outFile: M.savefig(outFile)    


def plotTllv(inFile, markerType='kx', outFile=None, groupBy=None, **options):
    """Plot the lat/lon locations of points from a time/lat/lon/value file."""
    fields = N.array([list(map(float, line.split())) for line in open(inFile, 'r')])
    lons = fields[:,2]; lats = fields[:,1]
    marksOnMap(lons, lats, markerType, outFile, \
               title='Lat/lon plot of '+inFile, **options)


def plotVtecAndJasonTracks(gtcFiles, outFile=None, names=None, makeFigure=True, show=False, **options):
    """Plot GAIM climate and assim VTEC versus JASON using at least two 'gc' files.
    First file is usually climate file, and rest are assim files.
    """
    ensureItems(options, {'title': 'GAIM vs. JASON for '+gtcFiles[0], \
                          'xlabel': 'Geographic Latitude (deg)', 'ylabel': 'VTEC (TECU)'})
    if 'show' in options:
        show = True
        del options['show']
    M.subplot(211)
    gtcFile = gtcFiles.pop(0)
    name = 'clim_'
    if names: name = names.pop(0)
    specs = [(gtcFile, 'latitude:2,jason:6,gim__:8,%s:13,iri__:10' % name)]
    name = 'assim'
    for i, gtcFile in enumerate(gtcFiles):
        label = name
        if len(gtcFiles) > 1: label += str(i+1)
        specs.append( (gtcFile, 'latitude:2,%s:13' % label) )
    plotColumns(specs, rmsDiffFrom='jason', floatFormat='%5.1f', **options)
    M.legend()
    
    M.subplot(212)
    options.update({'title': 'JASON Track Plot', 'xlabel': 'Longitude (deg)', 'ylabel': 'Latitude (deg)'})
    fields = N.array([list(map(floatOrMiss, line.split())) for line in open(gtcFiles[0], 'r')])
    lons = fields[:,2]; lats = fields[:,1]
    marksOnMap(lons, lats, show=show, **options)
    if outFile: M.savefig(outFile)


def diffStats(name1, vals1, name2, vals2):
    """Compute RMS difference between two Numeric vectors."""
    from Stats import Stats
    label = name2 + ' - ' + name1
    diff = vals2 - vals1
    return Stats().label(label).addm(diff)

def ensureItems(d1, d2):
    for key in list(d2.keys()):
        if key not in d1: d1[key] = d2[key]

def splitColumnSpec(s):
    """Split column spec 'label:integer[:style]' into its 2 or 3 parts."""
    items = s.split(':')
    n = len(items)
    if n < 2:
        die('plotlib: Bad column spec. %s' % s)
    elif n == 2:
        items.append(None)
    items[1] = int(items[1])
    return items

def floatOrMiss(val, missingValue=-999.):
    try: val = float(val)
    except: val = missingValue
    return val

def floatOrStr(val):
    try: val = float(val)
    except: pass
    return val

def evalKeywordCmds(options, cmdOptions=CmdOptions):
    for option in options:
        if option in cmdOptions['MCommand']:
            args = options[option]
            if args:
                if args is True:
                    args = ''
                else:
                    args = "'" + args + "'"
                if option in cmdOptions:
                    args += dict2kwargs( validCmdOptions(options, cmdOptions[option]) )
                try:
                    eval('M.' + option + '(%s)' % args)
                except:
                    die('failed eval of keyword command option failed: %s=%s' % (option, args))
#        else:
#            warn('Invalid keyword option specified" %s=%s' % (option, args))

def validCmdOptions(options, cmd, possibleOptions=CmdOptions):
    return dict([(option, options[option]) for option in list(options.keys())
                    if option in possibleOptions[cmd]])

def dict2kwargs(d):
    args = [',%s=%s' % (kw, d[kw]) for kw in d]
    return ', '.join(args)

def csv2columns(csvFile, columns):
    """Given a CSV file and a comma-separated list of desired column names and
    types (name:type), return an array of vectors containing type-converted data.

    Type should be the name of string-to-val type-conversion function such as float, int, or str.
    If type is missing, then float conversion is assumed.
    """
    import csv
    names = []; types = []; cols = []
    for column in columns.split(','):
        if column.find(':') > 0:
            name, type = column.split(':')
        else:
            name = column; type = 'float'
        names.append(name.strip())
        types.append( eval(type.strip()) )  # get type conversion function from type string
        cols.append([])

    print(csvFile)
    for fields in csv.DictReader(urlopen(csvFile).readlines(), skipinitialspace=True):
        tmpColVals = []
        try:
            for i, type in enumerate(types): tmpColVals.append( type(fields[names[i]]) )
        except Exception as e:
            print("Got exception coercing values: %s" % e)
            continue
        for i in range(len(types)): cols[i].append(tmpColVals[i])
    return [N.array(col) for col in cols]

def csv2marksOnMap(csvFile, columns, title, vmin=None, vmax=None,
                   imageWidth=None, imageHeight=None, mapFile=None, projection='cyl'):
    if mapFile is None: mapFile = csvFile + '.map.png'
    lons, lats, vals = csv2columns(csvFile, columns)
    marksOnMap(lons, lats, vals, vmin, vmax, imageWidth, imageHeight,
               mapFile, projection, title=title)
    return mapFile

def imageSlice(dataFile, lonSlice, latSlice, varSlice, title, vmin=None, vmax=None,
               imageWidth=None, imageHeight=None, imageFile=None,
               projection='cyl', colormap=M.cm.jet):
#    if dataFile == []: dataFile = 'http://laits.gmu.edu/NWGISS_Temp_Data/WCSfCays5.hdf'
    if imageFile is None: imageFile = dataFile + '.image.png'

    print(dataFile)
    print(imageFile)
    tmpFile, headers = urlretrieve(dataFile)
#    tmpFile = 'WCSfCays5.hdf'
    try:
        from Scientific.IO.NetCDF import NetCDFFile as NC
        nc = NC(tmpFile)
        fileType = 'nc'
    except:
        try:
            import hdfeos
            grids = hdfeos.grids(tmpFile)
            fileType = 'hdf_grid'
            grid = grids[0]
        except:
            try:
                swaths = hdfeos.swaths(tmpFile)
                fileType = 'hdf_swath'
                swath = swaths[0]
            except:
                raise RuntimeError('imageSlice: Can only slice & image netCDF or HDF grid/swath files.')
    if fileType == 'nc':
        lons = evalSlice(nc, lonSlice)
        lats = evalSlice(nc, latSlice)
        vals = evalSlice(nc, varSlice)
    elif fileType == 'hdf_grid':
        print(grids)
        fields = hdfeos.grid_fields(tmpFile, grid)
        print(fields)
        field = fields[0]
        dims = hdfeos.grid_field_dims(tmpFile, grid, field)
        print(dims)
        lons = hdfeos.grid_field_read(tmpFile, grid, lonSlice)
        lats = hdfeos.grid_field_read(tmpFile, grid, latSlice)
        vals = hdfeos.grid_field_read(tmpFile, grid, field)
    elif fileType == 'hdf_swath':
        print(swaths)
        print(hdfeos.swath_geo_fields(tmpFile, swath))
        print(hdfeos.swath_data_fields(tmpFile, swath))
        lons = hdfeos.get_swath_field(tmpFile, swath, 'Longitude')  # assume HDFEOS conventions
        lats = hdfeos.get_swath_field(tmpFile, swath, 'Latitude')
        vals = hdfeos.get_swath_field(tmpFile, swath, varSlice)

    imageMap(lons, lats, vals, vmin, vmax, imageWidth, imageHeight, imageFile,
             title=title, projection=projection, cmap=colormap)
    return imageFile

def evalSlice(nc, varSlice):
    if varSlice.find('[') > 0:
        varName, slice = varSlice.split('[')
        vals = nc.variables[varName]
        vals = eval('['.join(('vals', slice)))
    else:
        vals = nc.variables[varSlice][:]
    return vals
        

if __name__ == '__main__':
    from sys import argv
#    lons = N.arange(0, 361, 2, N.Float)
#    lats = N.arange(-90, 91, 1, N.Float)
#    data = N.fromfunction( lambda x,y: x+y, (len(lats), len(lons)))

    outFile = 'out.png'
#    imageMap(lons, lats, data, outFile)
#    marksOnMap(lons, lats, 'bx', outFile)
#    plotVtecAndJasonTracks([argv[1], argv[2]], outFile, show=True, legend=True)

    me = argv.pop(0)
    args = list(map(floatOrStr, argv))
    imageSlice(*args)
