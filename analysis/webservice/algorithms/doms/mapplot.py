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

import string
from cStringIO import StringIO
from multiprocessing import Process, Manager

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap

import BaseDomsHandler
import ResultsStorage

if not matplotlib.get_backend():
    matplotlib.use('Agg')

PARAMETER_TO_FIELD = {
    "sst": "sea_water_temperature",
    "sss": "sea_water_salinity"
}

PARAMETER_TO_UNITS = {
    "sst": "($^\circ$ C)",
    "sss": "(g/L)"
}


def __square(minLon, maxLon, minLat, maxLat):
    if maxLat - minLat > maxLon - minLon:
        a = ((maxLat - minLat) - (maxLon - minLon)) / 2.0
        minLon -= a
        maxLon += a
    elif maxLon - minLon > maxLat - minLat:
        a = ((maxLon - minLon) - (maxLat - minLat)) / 2.0
        minLat -= a
        maxLat += a

    return minLon, maxLon, minLat, maxLat


def render(d, lats, lons, z, primary, secondary, parameter):
    fig = plt.figure()
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])

    ax.set_title(string.upper("%s vs. %s" % (primary, secondary)))
    # ax.set_ylabel('Latitude')
    # ax.set_xlabel('Longitude')

    minLatA = np.min(lats)
    maxLatA = np.max(lats)
    minLonA = np.min(lons)
    maxLonA = np.max(lons)

    minLat = minLatA - (abs(maxLatA - minLatA) * 0.1)
    maxLat = maxLatA + (abs(maxLatA - minLatA) * 0.1)

    minLon = minLonA - (abs(maxLonA - minLonA) * 0.1)
    maxLon = maxLonA + (abs(maxLonA - minLonA) * 0.1)

    minLon, maxLon, minLat, maxLat = __square(minLon, maxLon, minLat, maxLat)

    # m = Basemap(projection='mill', llcrnrlon=-180,llcrnrlat=-80,urcrnrlon=180,urcrnrlat=80,resolution='l')
    m = Basemap(projection='mill', llcrnrlon=minLon, llcrnrlat=minLat, urcrnrlon=maxLon, urcrnrlat=maxLat,
                resolution='l')

    m.drawparallels(np.arange(minLat, maxLat, (maxLat - minLat) / 5.0), labels=[1, 0, 0, 0], fontsize=10)
    m.drawmeridians(np.arange(minLon, maxLon, (maxLon - minLon) / 5.0), labels=[0, 0, 0, 1], fontsize=10)

    m.drawcoastlines()
    m.drawmapboundary(fill_color='#99ffff')
    m.fillcontinents(color='#cc9966', lake_color='#99ffff')

    # lats, lons = np.meshgrid(lats, lons)

    masked_array = np.ma.array(z, mask=np.isnan(z))
    z = masked_array

    values = np.zeros(len(z))
    for i in range(0, len(z)):
        values[i] = ((z[i] - np.min(z)) / (np.max(z) - np.min(z)) * 20.0) + 10

    x, y = m(lons, lats)

    im1 = m.scatter(x, y, values)

    im1.set_array(z)
    cb = m.colorbar(im1)

    units = PARAMETER_TO_UNITS[parameter] if parameter in PARAMETER_TO_UNITS else PARAMETER_TO_UNITS["sst"]
    cb.set_label("Difference %s" % units)

    sio = StringIO()
    plt.savefig(sio, format='png')
    plot = sio.getvalue()
    if d is not None:
        d['plot'] = plot
    return plot


class DomsMapPlotQueryResults(BaseDomsHandler.DomsQueryResults):
    def __init__(self, lats, lons, z, parameter, primary, secondary, args=None, bounds=None, count=None, details=None,
                 computeOptions=None, executionId=None, plot=None):
        BaseDomsHandler.DomsQueryResults.__init__(self, results={"lats": lats, "lons": lons, "values": z}, args=args,
                                                  details=details, bounds=bounds, count=count,
                                                  computeOptions=computeOptions, executionId=executionId)
        self.__lats = lats
        self.__lons = lons
        self.__z = np.array(z)
        self.__parameter = parameter
        self.__primary = primary
        self.__secondary = secondary
        self.__plot = plot

    def toImage(self):
        return self.__plot


def renderAsync(x, y, z, primary, secondary, parameter):
    manager = Manager()
    d = manager.dict()
    p = Process(target=render, args=(d, x, y, z, primary, secondary, parameter))
    p.start()
    p.join()
    return d['plot']


def createMapPlot(id, parameter):
    with ResultsStorage.ResultsRetrieval() as storage:
        params, stats, data = storage.retrieveResults(id)

    primary = params["primary"]
    secondary = params["matchup"][0]

    lats = []
    lons = []
    z = []

    field = PARAMETER_TO_FIELD[parameter] if parameter in PARAMETER_TO_FIELD else PARAMETER_TO_FIELD["sst"]

    for entry in data:
        for match in entry["matches"]:
            if match["source"] == secondary:

                if field in entry and field in match:
                    a = entry[field]
                    b = match[field]
                    z.append((a - b))
                    z.append((a - b))
                else:
                    z.append(1.0)
                    z.append(1.0)
                lats.append(entry["y"])
                lons.append(entry["x"])
                lats.append(match["y"])
                lons.append(match["x"])

    plot = renderAsync(lats, lons, z, primary, secondary, parameter)
    r = DomsMapPlotQueryResults(lats=lats, lons=lons, z=z, parameter=parameter, primary=primary, secondary=secondary,
                                args=params,
                                details=stats, bounds=None, count=None, computeOptions=None, executionId=id, plot=plot)
    return r
