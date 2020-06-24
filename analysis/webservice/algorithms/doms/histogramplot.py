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
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import numpy as np

import BaseDomsHandler
import ResultsStorage

if not matplotlib.get_backend():
    matplotlib.use('Agg')

PARAMETER_TO_FIELD = {
    "sst": "sea_water_temperature",
    "sss": "sea_water_salinity"
}

PARAMETER_TO_UNITS = {
    "sst": "($^\circ$C)",
    "sss": "(g/L)"
}


class DomsHistogramPlotQueryResults(BaseDomsHandler.DomsQueryResults):

    def __init__(self, x, parameter, primary, secondary, args=None, bounds=None, count=None, details=None,
                 computeOptions=None, executionId=None, plot=None):
        BaseDomsHandler.DomsQueryResults.__init__(self, results=x, args=args, details=details, bounds=bounds,
                                                  count=count, computeOptions=computeOptions, executionId=executionId)
        self.__primary = primary
        self.__secondary = secondary
        self.__x = x
        self.__parameter = parameter
        self.__plot = plot

    def toImage(self):
        return self.__plot


def render(d, x, primary, secondary, parameter, norm_and_curve=False):
    fig, ax = plt.subplots()
    fig.suptitle(string.upper("%s vs. %s" % (primary, secondary)), fontsize=14, fontweight='bold')

    n, bins, patches = plt.hist(x, 50, normed=norm_and_curve, facecolor='green', alpha=0.75)

    if norm_and_curve:
        mean = np.mean(x)
        variance = np.var(x)
        sigma = np.sqrt(variance)
        y = mlab.normpdf(bins, mean, sigma)
        l = plt.plot(bins, y, 'r--', linewidth=1)

    ax.set_title('n = %d' % len(x))

    units = PARAMETER_TO_UNITS[parameter] if parameter in PARAMETER_TO_UNITS else PARAMETER_TO_UNITS["sst"]
    ax.set_xlabel("%s - %s %s" % (primary, secondary, units))

    if norm_and_curve:
        ax.set_ylabel("Probability per unit difference")
    else:
        ax.set_ylabel("Frequency")

    plt.grid(True)

    sio = StringIO()
    plt.savefig(sio, format='png')
    d['plot'] = sio.getvalue()


def renderAsync(x, primary, secondary, parameter, norm_and_curve):
    manager = Manager()
    d = manager.dict()
    p = Process(target=render, args=(d, x, primary, secondary, parameter, norm_and_curve))
    p.start()
    p.join()
    return d['plot']


def createHistogramPlot(id, parameter, norm_and_curve=False):
    with ResultsStorage.ResultsRetrieval() as storage:
        params, stats, data = storage.retrieveResults(id)

    primary = params["primary"]
    secondary = params["matchup"][0]

    x = createHistTable(data, secondary, parameter)

    plot = renderAsync(x, primary, secondary, parameter, norm_and_curve)

    r = DomsHistogramPlotQueryResults(x=x, parameter=parameter, primary=primary, secondary=secondary,
                                      args=params, details=stats,
                                      bounds=None, count=None, computeOptions=None, executionId=id, plot=plot)
    return r


def createHistTable(results, secondary, parameter):
    x = []

    field = PARAMETER_TO_FIELD[parameter] if parameter in PARAMETER_TO_FIELD else PARAMETER_TO_FIELD["sst"]

    for entry in results:
        for match in entry["matches"]:
            if match["source"] == secondary:
                if field in entry and field in match:
                    a = entry[field]
                    b = match[field]
                    x.append((a - b))

    return x
