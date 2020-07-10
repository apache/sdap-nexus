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


import logging
import traceback
from cStringIO import StringIO
from datetime import datetime
from multiprocessing.dummy import Pool, Manager

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from nexustiles.nexustiles import NexusTileService
from scipy import stats

from webservice import Filtering as filt
from webservice.NexusHandler import nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

SENTINEL = 'STOP'


@nexus_handler
class TimeSeriesCalcHandlerImpl(NexusCalcHandler):
    name = "Time Series Solr"
    path = "/statsSolr"
    description = "Computes a time series plot between one or more datasets given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        NexusCalcHandler.__init__(self, skipCassandra=True)
        self.log = logging.getLogger(__name__)

    def calc(self, computeOptions, **args):
        """

        :param computeOptions: StatsComputeOptions
        :param args: dict
        :return:
        """

        ds = computeOptions.get_dataset()

        if type(ds) != list and type(ds) != tuple:
            ds = (ds,)

        resultsRaw = []

        for shortName in ds:
            results, meta = self.getTimeSeriesStatsForBoxSingleDataSet(computeOptions.get_min_lat(),
                                                                       computeOptions.get_max_lat(),
                                                                       computeOptions.get_min_lon(),
                                                                       computeOptions.get_max_lon(),
                                                                       shortName,
                                                                       computeOptions.get_start_time(),
                                                                       computeOptions.get_end_time(),
                                                                       computeOptions.get_apply_seasonal_cycle_filter(),
                                                                       computeOptions.get_apply_low_pass_filter())
            resultsRaw.append([results, meta])

        results = self._mergeResults(resultsRaw)

        if len(ds) == 2:
            stats = self.calculateComparisonStats(results, suffix="")
            if computeOptions.get_apply_seasonal_cycle_filter():
                s = self.calculateComparisonStats(results, suffix="Seasonal")
                stats = self._mergeDicts(stats, s)
            if computeOptions.get_apply_low_pass_filter():
                s = self.calculateComparisonStats(results, suffix="LowPass")
                stats = self._mergeDicts(stats, s)
            if computeOptions.get_apply_seasonal_cycle_filter() and computeOptions.get_apply_low_pass_filter():
                s = self.calculateComparisonStats(results, suffix="SeasonalLowPass")
                stats = self._mergeDicts(stats, s)
        else:
            stats = {}

        meta = []
        for singleRes in resultsRaw:
            meta.append(singleRes[1])

        res = TimeSeriesResults(results=results, meta=meta, stats=stats, computeOptions=computeOptions)
        return res

    def getTimeSeriesStatsForBoxSingleDataSet(self, min_lat, max_lat, min_lon, max_lon, ds, start_time=0, end_time=-1,
                                              applySeasonalFilter=True, applyLowPass=True):

        daysinrange = self._get_tile_service().find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, ds, start_time,
                                                                end_time)

        if len(daysinrange) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        maxprocesses = int(self.algorithm_config.get("multiprocessing", "maxprocesses"))

        results = []
        if maxprocesses == 1:
            calculator = TimeSeriesCalculator()
            for dayinseconds in daysinrange:
                result = calculator.calc_average_on_day(min_lat, max_lat, min_lon, max_lon, ds, dayinseconds)
                results.append(result)
        else:
            # Create a task to calc average difference for each day
            manager = Manager()
            work_queue = manager.Queue()
            done_queue = manager.Queue()
            for dayinseconds in daysinrange:
                work_queue.put(
                    ('calc_average_on_day', min_lat, max_lat, min_lon, max_lon, ds, dayinseconds))
            [work_queue.put(SENTINEL) for _ in xrange(0, maxprocesses)]

            # Start new processes to handle the work
            pool = Pool(maxprocesses)
            [pool.apply_async(pool_worker, (work_queue, done_queue)) for _ in xrange(0, maxprocesses)]
            pool.close()

            # Collect the results as [(day (in ms), average difference for that day)]
            for i in xrange(0, len(daysinrange)):
                result = done_queue.get()
                try:
                    error_str = result['error']
                    self.log.error(error_str)
                    raise NexusProcessingException(reason="Error calculating average by day.")
                except KeyError:
                    pass

                results.append(result)

            pool.terminate()
            manager.shutdown()

        results = sorted(results, key=lambda entry: entry["time"])

        filt.applyAllFiltersOnField(results, 'mean', applySeasonal=applySeasonalFilter, applyLowPass=applyLowPass)
        filt.applyAllFiltersOnField(results, 'max', applySeasonal=applySeasonalFilter, applyLowPass=applyLowPass)
        filt.applyAllFiltersOnField(results, 'min', applySeasonal=applySeasonalFilter, applyLowPass=applyLowPass)

        return results, {}

    def calculateComparisonStats(self, results, suffix=""):

        xy = [[], []]

        for item in results:
            if len(item) == 2:
                xy[item[0]["ds"]].append(item[0]["mean%s" % suffix])
                xy[item[1]["ds"]].append(item[1]["mean%s" % suffix])

        slope, intercept, r_value, p_value, std_err = stats.linregress(xy[0], xy[1])
        comparisonStats = {
            "slope%s" % suffix: slope,
            "intercept%s" % suffix: intercept,
            "r%s" % suffix: r_value,
            "p%s" % suffix: p_value,
            "err%s" % suffix: std_err
        }

        return comparisonStats


class TimeSeriesResults(NexusResults):
    LINE_PLOT = "line"
    SCATTER_PLOT = "scatter"

    __SERIES_COLORS = ['red', 'blue']

    def __init__(self, results=None, meta=None, stats=None, computeOptions=None):
        NexusResults.__init__(self, results=results, meta=meta, stats=stats, computeOptions=computeOptions)

    def toImage(self):

        type = self.computeOptions().get_plot_type()

        if type == TimeSeriesResults.LINE_PLOT or type == "default":
            return self.createLinePlot()
        elif type == TimeSeriesResults.SCATTER_PLOT:
            return self.createScatterPlot()
        else:
            raise Exception("Invalid or unsupported time series plot specified")

    def createScatterPlot(self):
        timeSeries = []
        series0 = []
        series1 = []

        res = self.results()
        meta = self.meta()

        plotSeries = self.computeOptions().get_plot_series() if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "mean"

        for m in res:
            if len(m) == 2:
                timeSeries.append(datetime.fromtimestamp(m[0]["time"] / 1000))
                series0.append(m[0][plotSeries])
                series1.append(m[1][plotSeries])

        title = ', '.join(set([m['title'] for m in meta]))
        sources = ', '.join(set([m['source'] for m in meta]))
        dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

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

        # r = self.stats()["r"]
        # plt.text(0.5, 0.5, "r = foo")

        ax.grid(True)
        fig.tight_layout()

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()

    def createLinePlot(self):
        nseries = len(self.meta())
        res = self.results()
        meta = self.meta()

        timeSeries = [datetime.fromtimestamp(m[0]["time"] / 1000) for m in res]

        means = [[np.nan] * len(res) for n in range(0, nseries)]

        plotSeries = self.computeOptions().get_plot_series() if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "mean"

        for n in range(0, len(res)):
            timeSlot = res[n]
            for seriesValues in timeSlot:
                means[seriesValues['ds']][n] = seriesValues[plotSeries]

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

            plots += ax.plot(x, means[n], color=self.__SERIES_COLORS[n], zorder=10, linewidth=3, label=meta[n]['title'])
            ax.set_ylabel(meta[n]['units'])

        labs = [l.get_label() for l in plots]
        axMain.legend(plots, labs, loc=0)

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()


class TimeSeriesCalculator(object):
    def __init__(self):
        self.__tile_service = NexusTileService()

    def calc_average_on_day(self, min_lat, max_lat, min_lon, max_lon, dataset, timeinseconds):
        # Get stats using solr only
        ds1_nexus_tiles_stats = self.__tile_service.get_stats_within_box_at_time(min_lat, max_lat, min_lon, max_lon,
                                                                                 dataset,
                                                                                 timeinseconds)

        data_min_within = min([tile["tile_min_val_d"] for tile in ds1_nexus_tiles_stats])
        data_max_within = max([tile["tile_max_val_d"] for tile in ds1_nexus_tiles_stats])
        data_sum_within = sum([tile["product(tile_avg_val_d, tile_count_i)"] for tile in ds1_nexus_tiles_stats])
        data_count_within = sum([tile["tile_count_i"] for tile in ds1_nexus_tiles_stats])

        # Get boundary tiles and calculate stats
        ds1_nexus_tiles = self.__tile_service.get_boundary_tiles_at_time(min_lat, max_lat, min_lon, max_lon,
                                                                         dataset,
                                                                         timeinseconds)

        tile_data_agg = np.ma.array([tile.data for tile in ds1_nexus_tiles])
        data_min_boundary = np.ma.min(tile_data_agg)
        data_max_boundary = np.ma.max(tile_data_agg)
        # daily_mean = np.ma.mean(tile_data_agg).item()
        data_sum_boundary = np.ma.sum(tile_data_agg)
        data_count_boundary = np.ma.count(tile_data_agg).item()
        # data_std = np.ma.std(tile_data_agg)

        # Combine stats
        data_min = min(data_min_within, data_min_boundary)
        data_max = max(data_max_within, data_max_boundary)
        data_count = data_count_within + data_count_boundary
        daily_mean = (data_sum_within + data_sum_boundary) / data_count
        data_std = 0

        # Return Stats by day
        stat = {
            'min': data_min,
            'max': data_max,
            'mean': daily_mean,
            'cnt': data_count,
            'std': data_std,
            'time': int(timeinseconds)
        }
        return stat


def pool_worker(work_queue, done_queue):
    try:
        calculator = TimeSeriesCalculator()

        for work in iter(work_queue.get, SENTINEL):
            scifunction = work[0]
            args = work[1:]
            result = calculator.__getattribute__(scifunction)(*args)
            done_queue.put(result)
    except Exception as e:
        e_str = traceback.format_exc(e)
        done_queue.put({'error': e_str})
