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


def render(d, x, y, z, primary, secondary, parameter):
    fig, ax = plt.subplots()

    ax.set_title(string.upper("%s vs. %s" % (primary, secondary)))

    units = PARAMETER_TO_UNITS[parameter] if parameter in PARAMETER_TO_UNITS else PARAMETER_TO_UNITS[
        "sst"]
    ax.set_ylabel("%s %s" % (secondary, units))
    ax.set_xlabel("%s %s" % (primary, units))

    ax.scatter(x, y)

    sio = StringIO()
    plt.savefig(sio, format='png')
    d['plot'] = sio.getvalue()


class DomsScatterPlotQueryResults(BaseDomsHandler.DomsQueryResults):

    def __init__(self, x, y, z, parameter, primary, secondary, args=None, bounds=None, count=None, details=None,
                 computeOptions=None, executionId=None, plot=None):
        BaseDomsHandler.DomsQueryResults.__init__(self, results=[x, y], args=args, details=details, bounds=bounds,
                                                  count=count, computeOptions=computeOptions, executionId=executionId)
        self.__primary = primary
        self.__secondary = secondary
        self.__x = x
        self.__y = y
        self.__z = z
        self.__parameter = parameter
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


def createScatterPlot(id, parameter):
    with ResultsStorage.ResultsRetrieval() as storage:
        params, stats, data = storage.retrieveResults(id)

    primary = params["primary"]
    secondary = params["matchup"][0]

    x, y, z = createScatterTable(data, secondary, parameter)

    plot = renderAsync(x, y, z, primary, secondary, parameter)

    r = DomsScatterPlotQueryResults(x=x, y=y, z=z, parameter=parameter, primary=primary, secondary=secondary,
                                    args=params, details=stats,
                                    bounds=None, count=None, computeOptions=None, executionId=id, plot=plot)
    return r


def createScatterTable(results, secondary, parameter):
    x = []
    y = []
    z = []

    field = PARAMETER_TO_FIELD[parameter] if parameter in PARAMETER_TO_FIELD else PARAMETER_TO_FIELD["sst"]

    for entry in results:
        for match in entry["matches"]:
            if match["source"] == secondary:
                if field in entry and field in match:
                    a = entry[field]
                    b = match[field]
                    x.append(a)
                    y.append(b)
                    z.append(a - b)

    return x, y, z
