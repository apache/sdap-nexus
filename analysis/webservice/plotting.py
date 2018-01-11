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
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import pyximport;

pyximport.install()

import os
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser
import matplotlib.dates as mdates
from matplotlib import cm
import pickle
from cStringIO import StringIO
import datetime
from matplotlib.ticker import FuncFormatter
from matplotlib.colors import LightSource
from mpl_toolkits.basemap import Basemap
import mpld3


# TODO needs to be updated to use nexustiles

def __writeAndShowImageData(data, delete=True):
    f = open("foo.png", "wb")
    f.write(data)
    f.close()

    plt.clf()
    image = plt.imread("foo.png")
    plt.imshow(image)
    plt.axis('off')  # clear x- and y-axes
    plt.show()
    if delete:
        os.unlink("foo.png")


def createLatLonTimeAverageMapGlobe(res, meta, startTime=None, endTime=None):
    latSeries = [m[0]['lat'] for m in res]
    lonSeries = [m['lon'] for m in res[0]][:]
    data = np.zeros((len(lonSeries), len(latSeries)))
    for t in range(0, len(latSeries)):
        latSet = res[t]
        for l in range(0, len(lonSeries)):
            data[l][t] = latSet[l]['avg']
    data[data == 0.0] = np.nan
    # data = np.rot90(data, 3)
    lats, lons = np.meshgrid(latSeries, lonSeries)
    masked_array = np.ma.array(data, mask=np.isnan(data))
    z = masked_array

    fig = plt.figure()
    fig.set_size_inches(11.0, 8.5)
    ax = fig.add_axes([0.05, 0.05, 0.9, 0.9])

    m = Basemap(projection='ortho', lat_0=20, lon_0=-100, resolution='l')
    # m.drawmapboundary(fill_color='0.3')
    im1 = m.pcolormesh(lons, lats, z, shading='flat', cmap=plt.cm.jet, latlon=True)
    m.drawparallels(np.arange(-90., 99., 30.))
    m.drawmeridians(np.arange(-180., 180., 60.))
    # m.drawcoastlines()
    # m.drawcountries()
    cb = m.colorbar(im1, "bottom", size="5%", pad="2%")

    title = meta['title']
    source = meta['source']
    if startTime is not None and endTime is not None:
        if type(startTime) is not datetime.datetime:
            startTime = datetime.datetime.fromtimestamp(startTime / 1000)
        if type(endTime) is not datetime.datetime:
            endTime = datetime.datetime.fromtimestamp(endTime / 1000)
        dateRange = "%s - %s" % (startTime.strftime('%b %Y'), endTime.strftime('%b %Y'))
    else:
        dateRange = ""

    ax.set_title("%s\n%s\n%s" % (title, source, dateRange))
    ax.set_ylabel('Latitude')
    ax.set_xlabel('Longitude')

    sio = StringIO()
    plt.savefig(sio, format='png')
    return sio.getvalue()


def createLatLonTimeAverageMapProjected(res, meta, startTime=None, endTime=None):
    latSeries = [m[0]['lat'] for m in res]
    lonSeries = [m['lon'] for m in res[0]][:]
    data = np.zeros((len(lonSeries), len(latSeries)))
    for t in range(0, len(latSeries)):
        latSet = res[t]
        for l in range(0, len(lonSeries)):
            data[l][t] = latSet[l]['avg']
    data[data == 0.0] = np.nan
    # data = np.rot90(data, 3)
    lats, lons = np.meshgrid(latSeries, lonSeries)
    masked_array = np.ma.array(data, mask=np.isnan(data))
    z = masked_array

    fig = plt.figure()
    fig.set_size_inches(11.0, 8.5)
    ax = fig.add_axes([0.05, 0.05, 0.9, 0.9])

    m = Basemap(projection='kav7', lon_0=0, resolution=None)
    # m.drawmapboundary(fill_color='0.3')
    im1 = m.pcolormesh(lons, lats, z, shading='gouraud', cmap=plt.cm.jet, latlon=True)
    m.drawparallels(np.arange(-90., 99., 30.))
    m.drawmeridians(np.arange(-180., 180., 60.))
    # m.drawcoastlines()
    # m.drawcountries()
    cb = m.colorbar(im1, "bottom", size="5%", pad="2%")

    title = meta['title']
    source = meta['source']
    if startTime is not None and endTime is not None:
        if type(startTime) is not datetime.datetime:
            startTime = datetime.datetime.fromtimestamp(startTime / 1000)
        if type(endTime) is not datetime.datetime:
            endTime = datetime.datetime.fromtimestamp(endTime / 1000)
        dateRange = "%s - %s" % (startTime.strftime('%b %Y'), endTime.strftime('%b %Y'))
    else:
        dateRange = ""

    ax.set_title("%s\n%s\n%s" % (title, source, dateRange))
    ax.set_ylabel('Latitude')
    ax.set_xlabel('Longitude')

    sio = StringIO()
    plt.savefig(sio, format='png')
    return sio.getvalue()


def createLatLonTimeAverageMap3d(res, meta, startTime=None, endTime=None):
    latSeries = [m[0]['lat'] for m in res][::-1]
    lonSeries = [m['lon'] for m in res[0]]
    data = np.zeros((len(latSeries), len(lonSeries)))
    for t in range(0, len(latSeries)):
        latSet = res[t]
        for l in range(0, len(lonSeries)):
            data[len(latSeries) - t - 1][l] = latSet[l]['avg']
    data[data == 0.0] = np.nan

    x, y = np.meshgrid(latSeries, lonSeries)
    z = data

    region = np.s_[0:178, 0:178]
    x, y, z = x[region], y[region], z[region]

    fig, ax = plt.subplots(subplot_kw=dict(projection='3d'))

    ls = LightSource(270, 45)
    masked_array = np.ma.array(z, mask=np.isnan(z))
    rgb = ls.shade(masked_array, cmap=cm.gist_earth)  # , vert_exag=0.1, blend_mode='soft')
    surf = ax.plot_surface(x, y, masked_array, rstride=1, cstride=1, facecolors=rgb,
                           linewidth=0, antialiased=False, shade=False)
    sio = StringIO()
    plt.savefig(sio, format='png')
    return sio.getvalue()


def createLatLonTimeAverageMap(res, meta, startTime=None, endTime=None):
    latSeries = [m[0]['lat'] for m in res][::-1]
    lonSeries = [m['lon'] for m in res[0]]

    data = np.zeros((len(latSeries), len(lonSeries)))

    for t in range(0, len(latSeries)):
        latSet = res[t]
        for l in range(0, len(lonSeries)):
            data[len(latSeries) - t - 1][l] = latSet[l]['avg']

    def yFormatter(y, pos):
        if y < len(latSeries):
            return '%s $^\circ$' % (int(latSeries[int(y)] * 100.0) / 100.)
        else:
            return ""

    def xFormatter(x, pos):
        if x < len(lonSeries):
            return "%s $^\circ$" % (int(lonSeries[int(x)] * 100.0) / 100.)
        else:
            return ""

    data[data == 0.0] = np.nan
    fig, ax = plt.subplots()
    fig.set_size_inches(11.0, 8.5)

    cmap = cm.coolwarm
    ls = LightSource(315, 45)
    masked_array = np.ma.array(data, mask=np.isnan(data))
    rgb = ls.shade(masked_array, cmap)

    cax = ax.imshow(rgb, interpolation='nearest', cmap=cmap)

    ax.yaxis.set_major_formatter(FuncFormatter(yFormatter))
    ax.xaxis.set_major_formatter(FuncFormatter(xFormatter))

    title = meta['title']
    source = meta['source']
    if startTime is not None and endTime is not None:
        if type(startTime) is not datetime.datetime:
            startTime = datetime.datetime.fromtimestamp(startTime / 1000)
        if type(endTime) is not datetime.datetime:
            endTime = datetime.datetime.fromtimestamp(endTime / 1000)
        dateRange = "%s - %s" % (startTime.strftime('%b %Y'), endTime.strftime('%b %Y'))
    else:
        dateRange = ""

    ax.set_title("%s\n%s\n%s" % (title, source, dateRange))
    ax.set_ylabel('Latitude')
    ax.set_xlabel('Longitude')

    fig.colorbar(cax)
    fig.autofmt_xdate()

    sio = StringIO()
    plt.savefig(sio, format='png')
    return sio.getvalue()


def __createLatLonTimeAverageMapTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False,
                                     delete=True, projection='flat'):
    if create:
        config = ConfigParser.RawConfigParser()
        config.read("sci.ini")

        sci = Sci(config)

        res, meta = sci.getLongitudeLatitudeMapStatsForBox(minLat,
                                                           maxLat,
                                                           minLon,
                                                           maxLon,
                                                           ds,
                                                           startTime,
                                                           endTime,
                                                           mask)

        output = open('data_latlon_time_avg.pkl', 'wb')
        pickle.dump((res, meta), output)
        output.close()

    pkl_file = open('data_latlon_time_avg.pkl', 'rb')
    data1 = pickle.load(pkl_file)
    pkl_file.close()
    res, meta = data1
    if projection == 'flat':
        bytes = createLatLonTimeAverageMap(res, meta, startTime, endTime)
    elif projection == '3d':
        bytes = createLatLonTimeAverageMap3d(res, meta, startTime, endTime)
    elif projection == 'projected':
        bytes = createLatLonTimeAverageMapProjected(res, meta, startTime, endTime)
    elif projection == 'globe':
        bytes = createLatLonTimeAverageMapGlobe(res, meta, startTime, endTime)
    __writeAndShowImageData(bytes, delete=delete)


def createHoffmueller(data, coordSeries, timeSeries, coordName, title, interpolate='nearest'):
    cmap = cm.coolwarm
    # ls = LightSource(315, 45)
    # rgb = ls.shade(data, cmap)

    fig, ax = plt.subplots()
    fig.set_size_inches(11.0, 8.5)
    cax = ax.imshow(data, interpolation=interpolate, cmap=cmap)

    def yFormatter(y, pos):
        if y < len(coordSeries):
            return "%s $^\circ$" % (int(coordSeries[int(y)] * 100.0) / 100.)
        else:
            return ""

    def xFormatter(x, pos):
        if x < len(timeSeries):
            return timeSeries[int(x)].strftime('%b %Y')
        else:
            return ""

    ax.xaxis.set_major_formatter(FuncFormatter(xFormatter))
    ax.yaxis.set_major_formatter(FuncFormatter(yFormatter))

    ax.set_title(title)
    ax.set_ylabel(coordName)
    ax.set_xlabel('Date')

    fig.colorbar(cax)
    fig.autofmt_xdate()

    labels = ['point {0}'.format(i + 1) for i in range(len(data))]
    # plugins.connect(fig, plugins.MousePosition(fontsize=14))
    tooltip = mpld3.plugins.PointLabelTooltip(cax, labels=labels)
    mpld3.plugins.connect(fig, tooltip)
    mpld3.show()
    # sio = StringIO()
    # plt.savefig(sio, format='png')
    # return sio.getvalue()


def createLongitudeHoffmueller(res, meta):
    lonSeries = [m['longitude'] for m in res[0]['lons']]
    timeSeries = [datetime.datetime.fromtimestamp(m['time'] / 1000) for m in res]

    data = np.zeros((len(lonSeries), len(timeSeries)))

    for t in range(0, len(timeSeries)):
        timeSet = res[t]
        for l in range(0, len(lonSeries)):
            latSet = timeSet['lons'][l]
            value = latSet['avg']
            data[len(lonSeries) - l - 1][t] = value

    title = meta['title']
    source = meta['source']
    dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

    return createHoffmueller(data, lonSeries, timeSeries, "Longitude", "%s\n%s\n%s" % (title, source, dateRange),
                             interpolate='nearest')


def createLongitudeHoffmuellerTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False,
                                   delete=True):
    if create:
        config = ConfigParser.RawConfigParser()
        config.read("sci.ini")

        sci = Sci(config)

        res, meta = sci.getLongitudeTimeHofMoellerStatsForBox(minLat,
                                                              maxLat,
                                                              minLon,
                                                              maxLon,
                                                              ds,
                                                              startTime,
                                                              endTime,
                                                              mask)

        output = open('data_lon_hoff.pkl', 'wb')
        pickle.dump((res, meta), output)
        output.close()

    pkl_file = open('data_lon_hoff.pkl', 'rb')
    data1 = pickle.load(pkl_file)
    pkl_file.close()
    res, meta = data1
    bytes = createLongitudeHoffmueller(res, meta)
    __writeAndShowImageData(bytes, delete=delete)


def createLatitudeHoffmueller(res, meta):
    latSeries = [m['latitude'] for m in res[0]['lats']]
    timeSeries = [datetime.datetime.fromtimestamp(m['time'] / 1000) for m in res]

    data = np.zeros((len(latSeries), len(timeSeries)))

    for t in range(0, len(timeSeries)):
        timeSet = res[t]
        for l in range(0, len(latSeries)):
            latSet = timeSet['lats'][l]
            value = latSet['avg']
            data[len(latSeries) - l - 1][t] = value

    title = meta['title']
    source = meta['source']
    dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

    return createHoffmueller(data, latSeries, timeSeries, "Latitude", title="%s\n%s\n%s" % (title, source, dateRange),
                             interpolate='nearest')


def createLatitudeHoffmuellerTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False,
                                  delete=True):
    if create:
        config = ConfigParser.RawConfigParser()
        config.read("sci.ini")

        sci = Sci(config)

        res, meta = sci.getLatitudeTimeHofMoellerStatsForBox(minLat,
                                                             maxLat,
                                                             minLon,
                                                             maxLon,
                                                             ds,
                                                             startTime,
                                                             endTime,
                                                             mask)

        output = open('data_lat_hoff.pkl', 'wb')
        pickle.dump((res, meta), output)
        output.close()

    pkl_file = open('data_lat_hoff.pkl', 'rb')
    data1 = pickle.load(pkl_file)
    pkl_file.close()
    res, meta = data1
    bytes = createLatitudeHoffmueller(res, meta)
    # __writeAndShowImageData(bytes, delete=delete)


SERIES_COLORS = ['red', 'blue']


def createTimeSeries(res, meta, nseries=1):
    # maxSeries = [m[0]['maxFiltered'] for m in res]
    # minSeries = [m[0]['minFiltered'] for m in res]
    # mean = [m[0]["meanFiltered"] for m in res]
    # mean1 = [m[1]["meanFiltered"] for m in res]
    # stdSeries = [m[0]['std'] for m in res]

    timeSeries = [datetime.datetime.fromtimestamp(m[0]["time"] / 1000) for m in res]

    means = [[np.nan] * len(res) for n in range(0, nseries)]

    for n in range(0, len(res)):
        timeSlot = res[n]
        for seriesValues in timeSlot:
            means[seriesValues['ds']][n] = seriesValues['mean']

    x = timeSeries

    fig, axMain = plt.subplots()
    fig.set_size_inches(11.0, 8.5)
    fig.autofmt_xdate()

    title = ', '.join(set([m['title'] for m in meta]))
    sources = ', '.join(set([m['source'] for m in meta]))
    dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

    axMain.set_title("%s\n%s\n%s" % (title, sources, dateRange))
    axMain.set_xlabel('Date')
    axMain.grid(True)
    axMain.xaxis.set_major_locator(mdates.YearLocator())
    axMain.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    axMain.xaxis.set_minor_locator(mdates.MonthLocator())
    axMain.format_xdata = mdates.DateFormatter('%Y-%m-%d')

    plots = []

    for n in range(0, nseries):
        if n == 0:
            ax = axMain
        else:
            ax = ax.twinx()

        plots += ax.plot(x, means[n], color=SERIES_COLORS[n], zorder=10, linewidth=3, label=meta[n]['title'])
        ax.set_ylabel(meta[n]['units'])

    labs = [l.get_label() for l in plots]
    axMain.legend(plots, labs, loc=0)

    sio = StringIO()
    plt.savefig(sio, format='png')
    return sio.getvalue()


def createScatterPlot(res, meta):
    timeSeries = []
    series0 = []
    series1 = []

    for m in res:
        if len(m) == 2:
            timeSeries.append(datetime.datetime.fromtimestamp(m[0]["time"] / 1000))
            series0.append(m[0]["mean"])
            series1.append(m[1]["mean"])

    title = ', '.join(set([m['title'] for m in meta]))
    sources = ', '.join(set([m['source'] for m in meta]))
    dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

    print len(timeSeries), len(series0), len(series1)
    fig, ax = plt.subplots()
    fig.set_size_inches(11.0, 8.5)
    ax.scatter(series0, series1, alpha=0.5)
    ax.set_xlabel(meta[0]['units'])
    ax.set_ylabel(meta[1]['units'])
    ax.set_title("%s\n%s\n%s" % (title, sources, dateRange))

    par = np.polyfit(series0, series1, 1, full=True)
    slope = par[0][0]
    intercept = par[0][1]
    xl = [min(series0), max(series0)]
    yl = [slope * xx + intercept for xx in xl]
    plt.plot(xl, yl, '-r')

    ax.grid(True)
    fig.tight_layout()

    sio = StringIO()
    plt.savefig(sio, format='png')
    return sio.getvalue()


def createTimeSeriesTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False, delete=True):
    if type(ds) != list and type(ds) != tuple:
        ds = (ds,)

    if create:
        config = ConfigParser.RawConfigParser()
        config.read("sci.ini")
        sci = Sci(config)
        res, meta = sci.getTimeSeriesStatsForBox(minLat,
                                                 maxLat,
                                                 minLon,
                                                 maxLon,
                                                 ds,
                                                 startTime,
                                                 endTime,
                                                 mask,
                                                 True,
                                                 True)

        output = open('data_sc_lp.pkl', 'wb')
        pickle.dump((res, meta), output)
        output.close()

    pkl_file = open('data_sc_lp.pkl', 'rb')
    data1 = pickle.load(pkl_file)
    pkl_file.close()
    res, meta = data1
    bytes = createScatterPlot(res, meta)
    # bytes = createTimeSeries(res, meta, nseries=len(ds))
    __writeAndShowImageData(bytes, delete=delete)


if __name__ == "__main__":
    ds = "NCDC-L4LRblend-GLOB-AVHRR_OI"

    # minLat=21.53978071813801
    # minLon=-25.20013561141637
    # maxLat=48.57009377619356
    # maxLon=8.446972830642368

    minLat = -2.67487472970339
    minLon = -78.13449868344182
    maxLat = 65.46403943747828
    maxLon = 4.6458350568532865

    # minLat = -89
    # maxLat = 89
    # minLon = -179
    # maxLon = 179

    startTime = 1201939200000
    endTime = 1328169600000
    mask = 3

    ds = (ds, "SSH_alti_1deg_1mon")
    createTimeSeriesTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False, delete=False)

    # createLatitudeHoffmuellerTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False, delete=False)
    # createLongitudeHoffmuellerTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False, delete=False)
    # __createLatLonTimeAverageMapTest(ds, minLat, minLon, maxLat, maxLon, startTime, endTime, mask, create=False, delete=False, projection='projected')
