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

import itertools
import logging
from cStringIO import StringIO
from datetime import datetime
from functools import partial

import matplotlib.pyplot as plt
import mpld3
import numpy as np
import shapely.geometry
from matplotlib import cm
from matplotlib.ticker import FuncFormatter
from nexustiles.nexustiles import NexusTileService
from pytz import timezone
from webservice.NexusHandler import nexus_handler
from webservice.algorithms_spark.NexusCalcSparkHandler import NexusCalcSparkHandler
from webservice.webmodel import NexusResults, NoDataException, NexusProcessingException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%SZ'

SENTINEL = 'STOP'
LATITUDE = 0
LONGITUDE = 1


class HofMoellerCalculator(object):
    @staticmethod
    def hofmoeller_stats(metrics_callback, tile_in_spark):

        (latlon, tile_id, index,
         min_lat, max_lat, min_lon, max_lon) = tile_in_spark

        tile_service = NexusTileService()
        try:
            # Load the dataset tile
            tile = tile_service.find_tile_by_id(tile_id, metrics_callback=metrics_callback)[0]
            calculation_start = datetime.now()
            # Mask it to the search domain
            tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat,
                                                   min_lon, max_lon, [tile])[0]
        except IndexError:
            # return None
            return []
        t = np.ma.min(tile.times)
        stats = []

        points = list(tile.nexus_point_generator())
        if latlon == 0:
            # Latitude-Time Map (Average over longitudes)
            data = sorted(points, key=lambda p: p.latitude)
            points_by_coord = itertools.groupby(data, key=lambda p: p.latitude)
        else:
            # Longitude-Time Map (Average over latitudes)
            data = sorted(points, key=lambda p: p.longitude)
            points_by_coord = itertools.groupby(data, key=lambda p: p.longitude)

        for coord, points_at_coord in points_by_coord:
            values_at_coord = np.array([[p.data_val,
                                         np.cos(np.radians(p.latitude))]
                                        for p in points_at_coord])
            vals = np.nan_to_num(values_at_coord[:, 0])
            weights = values_at_coord[:, 1]
            coord_cnt = len(values_at_coord)
            if latlon == 0:
                # Latitude-Time Map (Average over longitudes)
                # In this case there is no weighting by cos(lat)
                weighted_sum = np.sum(vals).item()
                sum_of_weights = coord_cnt
            else:
                # Longitude-Time Map (Average over latitudes)
                # In this case we need to weight by cos(lat)
                weighted_sum = np.dot(vals, weights)
                sum_of_weights = np.sum(weights).item()

            stats.append(((t, float(coord)), (t, index, float(coord),
                                              coord_cnt,
                                              weighted_sum,
                                              sum_of_weights,
                                              np.max(vals).item(),
                                              np.min(vals).item(),
                                              np.var(vals).item())))
        calculation_duration = (datetime.now() - calculation_start).total_seconds()
        metrics_callback(calculation=calculation_duration)

        return stats


class BaseHoffMoellerSparkHandlerImpl(NexusCalcSparkHandler):
    params = {
        "ds": {
            "name": "Dataset",
            "type": "comma-delimited string",
            "description": "The dataset(s) Used to generate the plot. Required"
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
        "spark": {
            "name": "Spark Configuration",
            "type": "comma-delimited value",
            "description": "Configuration used to launch in the Spark cluster. Value should be 3 elements separated by "
                           "commas. 1) Spark Master 2) Number of Spark Executors 3) Number of Spark Partitions. Only "
                           "Number of Spark Partitions is used by this function. Optional (Default: local,1,1)"
        }
    }

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")

        try:
            ds = request.get_dataset()
            if type(ds) == list or type(ds) == tuple:
                ds = next(iter(ds))
        except:
            raise NexusProcessingException(
                reason="'ds' argument is required. Must be a string",
                code=400)

        # Do not allow time series on Climatology
        if next(iter([clim for clim in ds if 'CLIM' in clim]), False):
            raise NexusProcessingException(
                reason="Cannot compute Latitude/Longitude Time Average plot on a climatology", code=400)

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

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        return ds, bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch

    def applyDeseasonToHofMoellerByField(self, results, pivot="lats", field="mean", append=True):
        shape = (len(results), len(results[0][pivot]))
        if shape[0] <= 12:
            return results
        for a in range(0, 12):
            values = []
            for b in range(a, len(results), 12):
                values.append(np.average([l[field] for l in results[b][pivot]]))
            avg = np.average(values)

            for b in range(a, len(results), 12):
                for l in results[b][pivot]:
                    l["%sSeasonal" % field] = l[field] - avg

        return results

    def applyDeseasonToHofMoeller(self, results, pivot="lats", append=True):
        results = self.applyDeseasonToHofMoellerByField(results, pivot, field="mean", append=append)
        results = self.applyDeseasonToHofMoellerByField(results, pivot, field="min", append=append)
        results = self.applyDeseasonToHofMoellerByField(results, pivot, field="max", append=append)
        return results


def determine_parllelism(num_tiles):
    """
    Try to stay at a maximum of 1500 tiles per partition; But don't go over 128 partitions.
    Also, don't go below the default of 8
    """
    num_partitions = max(min(num_tiles // 1500, 128), 8)
    return num_partitions


def parallel_variance(avg_a, count_a, var_a, avg_b, count_b, var_b):
    # Thanks Wikipedia https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    delta = avg_b - avg_a
    m_a = var_a * (count_a - 1)
    m_b = var_b * (count_b - 1)
    M2 = m_a + m_b + delta ** 2 * count_a * count_b / (count_a + count_b)
    return M2 / (count_a + count_b - 1)


def hof_tuple_time(t):
    return t[0]


def hof_tuple_combine(t1, t2):
    return (t1[0],  # Time
            t1[1],  # Sequence (index)
            t1[2],  # Coordinate on axis (latitude or longitude)
            t1[3] + t2[3],  # Number of values
            t1[4] + t2[4],  # Sum of values (weighted for lon-time maps)
            t1[5] + t2[5],  # Sum of weights (= # of values for lat-time maps)
            max(t1[6], t2[6]),  # Maximum value
            min(t1[7], t2[7]),  # Minimum value
            parallel_variance(t1[4] / t1[5], t1[3], t1[8],
                              t2[4] / t2[5], t2[3], t2[8]))  # Variance


def hof_tuple_to_dict(t, avg_var_name):
    return {avg_var_name: t[2],
            'cnt': t[3],
            'mean': t[4] / t[5],
            'std': np.sqrt(t[8]),
            'max': t[6],
            'min': t[7]}


def spark_driver(sc, latlon, nexus_tiles_spark, metrics_callback):
    # Parallelize list of tile ids
    rdd = sc.parallelize(nexus_tiles_spark, determine_parllelism(len(nexus_tiles_spark)))
    if latlon == 0:
        # Latitude-Time Map (Average over longitudes)
        avg_var_name = 'latitude'
        avg_var_name_collection = 'lats'
    else:
        # Longitude-Time Map (Average over latitudes)
        avg_var_name = 'longitude'
        avg_var_name_collection = 'lons'

    # Create a set of key-value pairs where the key is (time, lat|lon) and
    # the value is a tuple of intermediate statistics for the specified
    # coordinate within a single NEXUS tile.
    metrics_callback(partitions=rdd.getNumPartitions())
    results = rdd.flatMap(partial(HofMoellerCalculator.hofmoeller_stats, metrics_callback))

    # Combine tuples across tiles with input key = (time, lat|lon)
    # Output a key value pair with key = (time)
    reduce_start = datetime.now()
    results = results.combineByKey(lambda val: (hof_tuple_time(val), val),
                                   lambda x, val: (hof_tuple_time(x),
                                                   hof_tuple_combine(x[1],
                                                                     val)),
                                   lambda x, y: (hof_tuple_time(x),
                                                 hof_tuple_combine(x[1],
                                                                   y[1])))

    # Convert the tuples to dictionary entries and combine coordinates
    # with the same time stamp.  Here we have input key = (time)
    results = results.values(). \
        combineByKey(lambda val, avg_var_name=avg_var_name,
                            avg_var_name_collection=avg_var_name_collection: {
        'sequence': val[1],
        'time': val[0],
        'iso_time': datetime.utcfromtimestamp(val[0]).strftime(ISO_8601),
        avg_var_name_collection: [
            hof_tuple_to_dict(val, avg_var_name)]},
                     lambda x, val, avg_var_name=avg_var_name,
                            avg_var_name_collection=avg_var_name_collection: {
                         'sequence': x['sequence'],
                         'time': x['time'],
                         'iso_time': x['iso_time'],
                         avg_var_name_collection: (
                                 x[avg_var_name_collection] +
                                 [hof_tuple_to_dict(val, avg_var_name)])},
                     lambda x, y,
                            avg_var_name_collection=avg_var_name_collection:
                     {'sequence': x['sequence'],
                      'time': x['time'],
                      'iso_time': x['iso_time'],
                      avg_var_name_collection: (
                              x[avg_var_name_collection] +
                              y[avg_var_name_collection])}). \
        values(). \
        collect()

    reduce_duration = (datetime.now() - reduce_start).total_seconds()
    metrics_callback(reduce=reduce_duration)

    return results


@nexus_handler
class LatitudeTimeHoffMoellerSparkHandlerImpl(BaseHoffMoellerSparkHandlerImpl):
    name = "Latitude/Time HofMoeller Spark"
    path = "/latitudeTimeHofMoellerSpark"
    description = "Computes a latitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = BaseHoffMoellerSparkHandlerImpl.params
    singleton = True

    def __init__(self, **kwargs):
        self._latlon = 0  # 0 for latitude-time map, 1 for longitude-time map
        BaseHoffMoellerSparkHandlerImpl.__init__(self, **kwargs)

    def calc(self, compute_options, **args):
        ds, bbox, start_time, end_time = self.parse_arguments(compute_options)

        metrics_record = self._create_metrics_record()
        calculation_start = datetime.now()

        min_lon, min_lat, max_lon, max_lat = bbox.bounds

        nexus_tiles_spark = [(self._latlon, tile.tile_id, x, min_lat, max_lat, min_lon, max_lon) for x, tile in
                             enumerate(self._get_tile_service().find_tiles_in_box(min_lat, max_lat, min_lon, max_lon,
                                                                            ds, start_time, end_time,
                                                                            metrics_callback=metrics_record.record_metrics,
                                                                            fetch_data=False))]

        print ("Got {} tiles".format(len(nexus_tiles_spark)))
        if len(nexus_tiles_spark) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        results = spark_driver(self._sc, self._latlon, nexus_tiles_spark, metrics_record.record_metrics)
        results = filter(None, results)
        results = sorted(results, key=lambda entry: entry['time'])
        for i in range(len(results)):
            results[i]['lats'] = sorted(results[i]['lats'],
                                        key=lambda entry: entry['latitude'])

        # Deseason disabled. See SDAP-148
        # results = self.applyDeseasonToHofMoeller(results)

        result = HoffMoellerResults(results=results, compute_options=None, type=HoffMoellerResults.LATITUDE,
                                    minLat=min_lat, maxLat=max_lat, minLon=min_lon,
                                    maxLon=max_lon, ds=ds, startTime=start_time, endTime=end_time)

        duration = (datetime.now() - calculation_start).total_seconds()
        metrics_record.record_metrics(actual_time=duration)
        metrics_record.print_metrics(self.log)

        return result


@nexus_handler
class LongitudeTimeHoffMoellerSparkHandlerImpl(BaseHoffMoellerSparkHandlerImpl):
    name = "Longitude/Time HofMoeller Spark"
    path = "/longitudeTimeHofMoellerSpark"
    description = "Computes a longitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = BaseHoffMoellerSparkHandlerImpl.params
    singleton = True

    def __init__(self, **kwargs):
        self._latlon = 1  # 0 for latitude-time map; 1 for longitude-time map
        BaseHoffMoellerSparkHandlerImpl.__init__(self, **kwargs)

    def calc(self, compute_options, **args):
        ds, bbox, start_time, end_time = self.parse_arguments(compute_options)

        metrics_record = self._create_metrics_record()
        calculation_start = datetime.now()

        min_lon, min_lat, max_lon, max_lat = bbox.bounds

        nexus_tiles_spark = [(self._latlon, tile.tile_id, x, min_lat, max_lat, min_lon, max_lon) for x, tile in
                             enumerate(self._get_tile_service().find_tiles_in_box(min_lat, max_lat, min_lon, max_lon,
                                                                            ds, start_time, end_time,
                                                                            metrics_callback=metrics_record.record_metrics,
                                                                            fetch_data=False))]

        print ("Got {} tiles".format(len(nexus_tiles_spark)))
        if len(nexus_tiles_spark) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        results = spark_driver(self._sc, self._latlon, nexus_tiles_spark, metrics_record.record_metrics)

        results = filter(None, results)
        results = sorted(results, key=lambda entry: entry["time"])
        for i in range(len(results)):
            results[i]['lons'] = sorted(results[i]['lons'],
                                        key=lambda entry: entry['longitude'])

        # Deseason disabled. See SDAP-148
        # results = self.applyDeseasonToHofMoeller(results, pivot="lons")

        result = HoffMoellerResults(results=results, compute_options=None, type=HoffMoellerResults.LONGITUDE,
                                    minLat=min_lat, maxLat=max_lat, minLon=min_lon,
                                    maxLon=max_lon, ds=ds, startTime=start_time, endTime=end_time)

        duration = (datetime.now() - calculation_start).total_seconds()
        metrics_record.record_metrics(actual_time=duration)
        metrics_record.print_metrics(self.log)

        return result


class HoffMoellerResults(NexusResults):
    LATITUDE = 0
    LONGITUDE = 1

    def __init__(self, results=None, meta=None, stats=None, **kwargs):

        NexusResults.__init__(self, results=results, meta=meta, stats=stats, computeOptions=None, **kwargs)
        self.__type = kwargs['type']

    def createHoffmueller(self, data, coordSeries, timeSeries, coordName, title, interpolate='nearest'):
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

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()

    def createLongitudeHoffmueller(self, res, meta):
        lonSeries = [m['longitude'] for m in res[0]['lons']]
        timeSeries = [datetime.fromtimestamp(m['time'] / 1000) for m in res]

        data = np.zeros((len(lonSeries), len(timeSeries)))

        plotSeries = self.computeOptions().get_plot_series(default="avg") if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "avg"

        for t in range(0, len(timeSeries)):
            timeSet = res[t]
            for l in range(0, len(lonSeries)):
                latSet = timeSet['lons'][l]
                value = latSet[plotSeries]
                data[len(lonSeries) - l - 1][t] = value

        title = meta['title']
        source = meta['source']
        dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

        return self.createHoffmueller(data, lonSeries, timeSeries, "Longitude",
                                      "%s\n%s\n%s" % (title, source, dateRange), interpolate='nearest')

    def createLatitudeHoffmueller(self, res, meta):
        latSeries = [m['latitude'] for m in res[0]['lats']]
        timeSeries = [datetime.fromtimestamp(m['time'] / 1000) for m in res]

        data = np.zeros((len(latSeries), len(timeSeries)))

        plotSeries = self.computeOptions().get_plot_series(default="avg") if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "avg"

        for t in range(0, len(timeSeries)):
            timeSet = res[t]
            for l in range(0, len(latSeries)):
                latSet = timeSet['lats'][l]
                value = latSet[plotSeries]
                data[len(latSeries) - l - 1][t] = value

        title = meta['title']
        source = meta['source']
        dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

        return self.createHoffmueller(data, latSeries, timeSeries, "Latitude",
                                      title="%s\n%s\n%s" % (title, source, dateRange), interpolate='nearest')

    def toImage(self):
        res = self.results()
        meta = self.meta()

        if self.__type == HoffMoellerResults.LATITUDE:
            return self.createLatitudeHoffmueller(res, meta)
        elif self.__type == HoffMoellerResults.LONGITUDE:
            return self.createLongitudeHoffmueller(res, meta)
        else:
            raise Exception("Unsupported HoffMoeller Plot Type")
