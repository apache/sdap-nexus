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


import calendar
import logging
import traceback
from cStringIO import StringIO
from datetime import datetime
from multiprocessing.dummy import Pool, Manager

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pytz
import shapely.geometry
import shapely.wkt
from backports.functools_lru_cache import lru_cache
from nexustiles.nexustiles import NexusTileService
from pytz import timezone
from scipy import stats

from webservice import Filtering as filtering
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException, NoDataException

SENTINEL = 'STOP'
EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

logger = logging.getLogger(__name__)

@nexus_handler
class TimeSeriesCalcHandlerImpl(NexusCalcHandler):
    name = "Time Series"
    path = "/stats"
    description = "Computes a time series plot between one or more datasets given an arbitrary geographical area and time range"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "comma-delimited string",
            "description": "The dataset(s) Used to generate the Time Series. Required"
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required"
        },
        "seasonalFilter": {
            "name": "Compute Seasonal Cycle Filter",
            "type": "boolean",
            "description": "Flag used to specify if the seasonal averages should be computed during "
                           "Time Series computation. Optional (Default: True)"
        },
        "lowPassFilter": {
            "name": "Compute Low Pass Filter",
            "type": "boolean",
            "description": "Flag used to specify if a low pass filter should be computed during "
                           "Time Series computation. Optional (Default: True)"
        }
    }
    singleton = True

    def parse_arguments(self, request):
        # Parse input arguments

        try:
            ds = request.get_dataset()
            if type(ds) != list and type(ds) != tuple:
                ds = (ds,)
        except:
            raise NexusProcessingException(
                reason="'ds' argument is required. Must be comma-delimited string",
                code=400)

        # Do not allow time series on Climatology
        if next(iter([clim for clim in ds if 'CLIM' in clim]), False):
            raise NexusProcessingException(reason="Cannot compute time series on a climatology", code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
            request.get_min_lon = lambda: bounding_polygon.bounds[0]
            request.get_min_lat = lambda: bounding_polygon.bounds[1]
            request.get_max_lon = lambda: bounding_polygon.bounds[2]
            request.get_max_lat = lambda: bounding_polygon.bounds[3]
        except:
            try:
                west, south, east, north = request.get_min_lon(), request.get_min_lat(), \
                                           request.get_max_lon(), request.get_max_lat()
                bounding_polygon = shapely.geometry.Polygon(
                    [(west, south), (east, south), (east, north), (west, north), (west, south)])
            except:
                raise NexusProcessingException(
                    reason="'b' argument is required. Must be comma-delimited float formatted as "
                           "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                    code=400)

        try:
            start_time = request.get_start_datetime()
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or "
                       "string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or "
                       "string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        if start_time > end_time:
            raise NexusProcessingException(
                reason="The starting time must be before the ending time. Received startTime: %s, endTime: %s" % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        apply_seasonal_cycle_filter = request.get_apply_seasonal_cycle_filter()
        apply_low_pass_filter = request.get_apply_low_pass_filter()

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        return ds, bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch, \
               apply_seasonal_cycle_filter, apply_low_pass_filter

    def calc(self, request, **args):
        """

        :param request: StatsComputeOptions
        :param args: dict
        :return:
        """

        ds, bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch, \
        apply_seasonal_cycle_filter, apply_low_pass_filter = self.parse_arguments(request)

        resultsRaw = []

        for shortName in ds:
            results, meta = self.getTimeSeriesStatsForBoxSingleDataSet(bounding_polygon,
                                                                       shortName,
                                                                       start_seconds_from_epoch,
                                                                       end_seconds_from_epoch,
                                                                       apply_seasonal_cycle_filter=apply_seasonal_cycle_filter,
                                                                       apply_low_pass_filter=apply_low_pass_filter)
            resultsRaw.append([results, meta])

        the_time = datetime.now()
        results = self._mergeResults(resultsRaw)

        if len(ds) == 2:
            try:
                stats = TimeSeriesCalcHandlerImpl.calculate_comparison_stats(results)
            except Exception:
                stats = {}
                tb = traceback.format_exc()
                logger.warn("Error when calculating comparison stats:\n%s" % tb)
        else:
            stats = {}

        meta = []
        for singleRes in resultsRaw:
            meta.append(singleRes[1])

        res = TimeSeriesResults(results=results, meta=meta, stats=stats,
                                computeOptions=None, minLat=bounding_polygon.bounds[1],
                                maxLat=bounding_polygon.bounds[3], minLon=bounding_polygon.bounds[0],
                                maxLon=bounding_polygon.bounds[2], ds=ds, startTime=start_seconds_from_epoch,
                                endTime=end_seconds_from_epoch)

        logger.info("Merging results and calculating comparisons took %s" % (str(datetime.now() - the_time)))
        return res

    def getTimeSeriesStatsForBoxSingleDataSet(self, bounding_polygon, ds, start_seconds_from_epoch,
                                              end_seconds_from_epoch,
                                              apply_seasonal_cycle_filter=True, apply_low_pass_filter=True):

        the_time = datetime.now()
        daysinrange = self._get_tile_service().find_days_in_range_asc(bounding_polygon.bounds[1],
                                                                bounding_polygon.bounds[3],
                                                                bounding_polygon.bounds[0],
                                                                bounding_polygon.bounds[2],
                                                                ds,
                                                                start_seconds_from_epoch,
                                                                end_seconds_from_epoch)
        logger.info("Finding days in range took %s for dataset %s" % (str(datetime.now() - the_time), ds))

        if len(daysinrange) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        the_time = datetime.now()
        maxprocesses = int(self.algorithm_config.get("multiprocessing", "maxprocesses"))

        results = []
        if maxprocesses == 1:
            calculator = TimeSeriesCalculator()
            for dayinseconds in daysinrange:
                result = calculator.calc_average_on_day(bounding_polygon.wkt, ds, dayinseconds)
                results += [result] if result else []
        else:
            # Create a task to calc average difference for each day
            manager = Manager()
            work_queue = manager.Queue()
            done_queue = manager.Queue()
            for dayinseconds in daysinrange:
                work_queue.put(
                    ('calc_average_on_day', bounding_polygon.wkt, ds, dayinseconds))
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
                    logger.error(error_str)
                    raise NexusProcessingException(reason="Error calculating average by day.")
                except KeyError:
                    pass

                results += [result] if result else []

            pool.terminate()
            manager.shutdown()

        results = sorted(results, key=lambda entry: entry["time"])
        logger.info("Time series calculation took %s for dataset %s" % (str(datetime.now() - the_time), ds))

        if apply_seasonal_cycle_filter:
            the_time = datetime.now()
            for result in results:
                month = datetime.utcfromtimestamp(result['time']).month
                month_mean, month_max, month_min = self.calculate_monthly_average(month, bounding_polygon.wkt, ds)
                seasonal_mean = result['mean'] - month_mean
                seasonal_min = result['min'] - month_min
                seasonal_max = result['max'] - month_max
                result['meanSeasonal'] = seasonal_mean
                result['minSeasonal'] = seasonal_min
                result['maxSeasonal'] = seasonal_max
            logger.info(
                "Seasonal calculation took %s for dataset %s" % (str(datetime.now() - the_time), ds))

        the_time = datetime.now()
        filtering.applyAllFiltersOnField(results, 'mean', applySeasonal=False, applyLowPass=apply_low_pass_filter)
        filtering.applyAllFiltersOnField(results, 'max', applySeasonal=False, applyLowPass=apply_low_pass_filter)
        filtering.applyAllFiltersOnField(results, 'min', applySeasonal=False, applyLowPass=apply_low_pass_filter)

        if apply_seasonal_cycle_filter and apply_low_pass_filter:
            try:
                filtering.applyFiltersOnField(results, 'meanSeasonal', applySeasonal=False, applyLowPass=True,
                                              append="LowPass")
                filtering.applyFiltersOnField(results, 'minSeasonal', applySeasonal=False, applyLowPass=True,
                                              append="LowPass")
                filtering.applyFiltersOnField(results, 'maxSeasonal', applySeasonal=False, applyLowPass=True,
                                              append="LowPass")
            except Exception as e:
                # If it doesn't work log the error but ignore it
                tb = traceback.format_exc()
                logger.warn("Error calculating SeasonalLowPass filter:\n%s" % tb)

        logger.info(
            "LowPass filter calculation took %s for dataset %s" % (str(datetime.now() - the_time), ds))

        return results, {}

    @lru_cache()
    def calculate_monthly_average(self, month=None, bounding_polygon_wkt=None, ds=None):

        min_date, max_date = self.get_min_max_date(ds=ds)

        monthly_averages, monthly_counts = [], []
        monthly_mins, monthly_maxes = [], []
        bounding_polygon = shapely.wkt.loads(bounding_polygon_wkt)
        for year in range(min_date.year, max_date.year + 1):
            beginning_of_month = datetime(year, month, 1)
            end_of_month = datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)
            start = (pytz.UTC.localize(beginning_of_month) - EPOCH).total_seconds()
            end = (pytz.UTC.localize(end_of_month) - EPOCH).total_seconds()
            tile_stats = self._get_tile_service().find_tiles_in_polygon(bounding_polygon, ds, start, end,
                                                                  fl=('id,'
                                                                      'tile_avg_val_d,tile_count_i,'
                                                                      'tile_min_val_d,tile_max_val_d,'
                                                                      'tile_min_lat,tile_max_lat,'
                                                                      'tile_min_lon,tile_max_lon'),
                                                                  fetch_data=False)
            if len(tile_stats) == 0:
                continue

            # Split list into tiles on the border of the bounding box and tiles completely inside the bounding box.
            border_tiles, inner_tiles = [], []
            for tile in tile_stats:
                inner_tiles.append(tile) if bounding_polygon.contains(shapely.geometry.box(tile.bbox.min_lon,
                                                                                           tile.bbox.min_lat,
                                                                                           tile.bbox.max_lon,
                                                                                           tile.bbox.max_lat)) else border_tiles.append(
                    tile)

            # We can use the stats of the inner tiles directly
            tile_means = [tile.tile_stats.mean for tile in inner_tiles]
            tile_mins = [tile.tile_stats.min for tile in inner_tiles]
            tile_maxes = [tile.tile_stats.max for tile in inner_tiles]
            tile_counts = [tile.tile_stats.count for tile in inner_tiles]

            # Border tiles need have the data loaded, masked, and stats recalculated
            border_tiles = list(self._get_tile_service().fetch_data_for_tiles(*border_tiles))
            border_tiles = self._get_tile_service().mask_tiles_to_polygon(bounding_polygon, border_tiles)
            for tile in border_tiles:
                tile.update_stats()
                tile_means.append(tile.tile_stats.mean)
                tile_mins.append(tile.tile_stats.min)
                tile_maxes.append(tile.tile_stats.max)
                tile_counts.append(tile.tile_stats.count)

            tile_means = np.array(tile_means)
            tile_mins = np.array(tile_mins)
            tile_maxes = np.array(tile_maxes)
            tile_counts = np.array(tile_counts)

            sum_tile_counts = np.sum(tile_counts) * 1.0

            monthly_averages += [np.average(tile_means, None, tile_counts / sum_tile_counts).item()]
            monthly_mins += [np.average(tile_mins, None, tile_counts / sum_tile_counts).item()]
            monthly_maxes += [np.average(tile_maxes, None, tile_counts / sum_tile_counts).item()]
            monthly_counts += [sum_tile_counts]

        count_sum = np.sum(monthly_counts) * 1.0
        weights = np.array(monthly_counts) / count_sum

        return np.average(monthly_averages, None, weights).item(), \
               np.average(monthly_averages, None, weights).item(), \
               np.average(monthly_averages, None, weights).item()

    @lru_cache()
    def get_min_max_date(self, ds=None):
        min_date = pytz.timezone('UTC').localize(
            datetime.utcfromtimestamp(self._get_tile_service().get_min_time([], ds=ds)))
        max_date = pytz.timezone('UTC').localize(
            datetime.utcfromtimestamp(self._get_tile_service().get_max_time([], ds=ds)))

        return min_date.date(), max_date.date()

    @staticmethod
    def calculate_comparison_stats(results):
        xy = [[], []]

        for item in results:
            if len(item) == 2:
                xy[item[0]["ds"]].append(item[0]["mean"])
                xy[item[1]["ds"]].append(item[1]["mean"])

        slope, intercept, r_value, p_value, std_err = stats.linregress(xy[0], xy[1])
        comparisonStats = {
            "slope": slope,
            "intercept": intercept,
            "r": r_value,
            "p": p_value,
            "err": std_err
        }

        return comparisonStats


class TimeSeriesResults(NexusResults):
    LINE_PLOT = "line"
    SCATTER_PLOT = "scatter"

    __SERIES_COLORS = ['red', 'blue']

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

    def calc_average_on_day(self, bounding_polygon_wkt, dataset, timeinseconds):
        bounding_polygon = shapely.wkt.loads(bounding_polygon_wkt)
        ds1_nexus_tiles = self.__tile_service.get_tiles_bounded_by_polygon_at_time(bounding_polygon,
                                                                                   dataset,
                                                                                   timeinseconds)

        # If all data ends up getting masked, ds1_nexus_tiles will be empty
        if len(ds1_nexus_tiles) == 0:
            return {}

        tile_data_agg = np.ma.array([tile.data for tile in ds1_nexus_tiles])
        data_min = np.ma.min(tile_data_agg)
        data_max = np.ma.max(tile_data_agg)
        daily_mean = np.ma.mean(tile_data_agg).item()
        data_count = np.ma.count(tile_data_agg)
        try:
            data_count = data_count.item()
        except AttributeError:
            pass
        data_std = np.ma.std(tile_data_agg)

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
