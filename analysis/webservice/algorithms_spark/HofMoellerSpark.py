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

import sys
import itertools
import logging
import traceback
from cStringIO import StringIO
from datetime import datetime

import matplotlib.pyplot as plt
import mpld3
import numpy as np
from matplotlib import cm
from matplotlib.ticker import FuncFormatter
from nexustiles.nexustiles import NexusTileService

from webservice.NexusHandler import SparkHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusProcessingException, NexusResults

SENTINEL = 'STOP'
LATITUDE = 0
LONGITUDE = 1


class HofMoellerCalculator(object):
    @staticmethod
    def hofmoeller_stats(tile_in_spark):

        (latlon, tile_id, index,
         min_lat, max_lat, min_lon, max_lon) = tile_in_spark

        tile_service = NexusTileService()
        try:
            # Load the dataset tile
            tile = tile_service.find_tile_by_id(tile_id)[0]
            # Mask it to the search domain
            tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat,
                                                   min_lon, max_lon, [tile])[0]
        except IndexError:
            #return None
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
            vals = np.nan_to_num(values_at_coord[:,0])
            weights = values_at_coord[:,1]
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
        return stats


class BaseHoffMoellerHandlerImpl(SparkHandler):
    def __init__(self):
        SparkHandler.__init__(self)
        self.log = logging.getLogger(__name__)

    def applyDeseasonToHofMoellerByField(self, results, pivot="lats", field="avg", append=True):
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
        results = self.applyDeseasonToHofMoellerByField(results, pivot, field="avg", append=append)
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
    return (t1[0], # Time
            t1[1], # Sequence (index)
            t1[2], # Coordinate on axis (latitude or longitude)
            t1[3] + t2[3], # Number of values
            t1[4] + t2[4], # Sum of values (weighted for lon-time maps)
            t1[5] + t2[5], # Sum of weights (= # of values for lat-time maps)
            max(t1[6], t2[6]), # Maximum value
            min(t1[7], t2[7]), # Minimum value
            parallel_variance(t1[4]/t1[5], t1[3], t1[8], 
                              t2[4]/t2[5], t2[3], t2[8])) # Variance

def hof_tuple_to_dict(t, avg_var_name):
    return {avg_var_name: t[2],
            'cnt': t[3],
            'avg': t[4] / t[5],
            'std': np.sqrt(t[8]),
            'max': t[6],
            'min': t[7]}

def spark_driver(sc, latlon, nexus_tiles_spark):
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
    results = rdd.flatMap(HofMoellerCalculator.hofmoeller_stats)

    # Combine tuples across tiles with input key = (time, lat|lon)
    # Output a key value pair with key = (time)
    results = results.combineByKey(lambda val: (hof_tuple_time(val),val),
                                   lambda x, val: (hof_tuple_time(x),
                                                   hof_tuple_combine(x[1],
                                                                     val)),
                                   lambda x, y: (hof_tuple_time(x),
                                                 hof_tuple_combine(x[1],
                                                                   y[1])))

    # Convert the tuples to dictionary entries and combine coordinates
    # with the same time stamp.  Here we have input key = (time)
    results = results.values().\
              combineByKey(lambda val, avg_var_name=avg_var_name,
                           avg_var_name_collection=avg_var_name_collection: {
                               'sequence': val[1],
                               'time': val[0],
                               avg_var_name_collection: [
                                   hof_tuple_to_dict(val, avg_var_name)]},
                           lambda x, val, avg_var_name=avg_var_name,
                           avg_var_name_collection=avg_var_name_collection: {
                               'sequence': x['sequence'],
                               'time': x['time'],
                               avg_var_name_collection: (
                                   x[avg_var_name_collection] +
                                   [hof_tuple_to_dict(val, avg_var_name)])},
                           lambda x, y,
                             avg_var_name_collection=avg_var_name_collection:
                             {'sequence': x['sequence'],
                              'time': x['time'],
                              avg_var_name_collection: (
                                  x[avg_var_name_collection] +
                                  y[avg_var_name_collection])}).\
              values().\
              collect()

    return results


@nexus_handler
class LatitudeTimeHoffMoellerSparkHandlerImpl(BaseHoffMoellerHandlerImpl):
    name = "Latitude/Time HofMoeller Spark"
    path = "/latitudeTimeHofMoellerSpark"
    description = "Computes a latitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        self._latlon = 0 # 0 for latitude-time map, 1 for longitude-time map
        BaseHoffMoellerHandlerImpl.__init__(self)

    def calc(self, computeOptions, **args):
        nexus_tiles_spark = [(self._latlon, tile.tile_id, x,
                              computeOptions.get_min_lat(),
                              computeOptions.get_max_lat(),
                              computeOptions.get_min_lon(),
                              computeOptions.get_max_lon())
                             for x, tile in enumerate(self._tile_service.find_tiles_in_box(computeOptions.get_min_lat(), computeOptions.get_max_lat(), computeOptions.get_min_lon(), computeOptions.get_max_lon(), computeOptions.get_dataset()[0], computeOptions.get_start_time(), computeOptions.get_end_time(), fetch_data=False))]
        print ("Got {} tiles".format(len(nexus_tiles_spark)))
        if len(nexus_tiles_spark) == 0:
            raise NexusProcessingException.NoDataException(reason="No data found for selected timeframe")

        results = spark_driver (self._sc, self._latlon, nexus_tiles_spark)
                                                        
        results = filter(None, results)
        results = sorted(results, key=lambda entry: entry['time'])
        for i in range(len(results)):
            results[i]['lats'] = sorted(results[i]['lats'],
                                        key=lambda entry: entry['latitude'])
        results = self.applyDeseasonToHofMoeller(results)

        result = HoffMoellerResults(results=results, computeOptions=computeOptions, type=HoffMoellerResults.LATITUDE)
        return result


@nexus_handler
class LongitudeTimeHoffMoellerSparkHandlerImpl(BaseHoffMoellerHandlerImpl):
    name = "Longitude/Time HofMoeller Spark"
    path = "/longitudeTimeHofMoellerSpark"
    description = "Computes a longitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        self._latlon = 1 # 0 for latitude-time map; 1 for longitude-time map
        BaseHoffMoellerHandlerImpl.__init__(self)

    def calc(self, computeOptions, **args):
        nexus_tiles_spark = [(self._latlon, tile.tile_id, x,
                              computeOptions.get_min_lat(),
                              computeOptions.get_max_lat(),
                              computeOptions.get_min_lon(),
                              computeOptions.get_max_lon())
                             for x, tile in enumerate(self._tile_service.find_tiles_in_box(computeOptions.get_min_lat(), computeOptions.get_max_lat(), computeOptions.get_min_lon(), computeOptions.get_max_lon(), computeOptions.get_dataset()[0], computeOptions.get_start_time(), computeOptions.get_end_time(), fetch_data=False))]

        if len(nexus_tiles_spark) == 0:
            raise NexusProcessingException.NoDataException(reason="No data found for selected timeframe")

        results = spark_driver (self._sc, self._latlon, nexus_tiles_spark)

        results = filter(None, results)
        results = sorted(results, key=lambda entry: entry["time"])
        for i in range(len(results)):
            results[i]['lons'] = sorted(results[i]['lons'],
                                        key=lambda entry: entry['longitude'])

        results = self.applyDeseasonToHofMoeller(results, pivot="lons")

        result = HoffMoellerResults(results=results, computeOptions=computeOptions, type=HoffMoellerResults.LONGITUDE)
        return result


class HoffMoellerResults(NexusResults):
    LATITUDE = 0
    LONGITUDE = 1

    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, **args):
        NexusResults.__init__(self, results=results, meta=meta, stats=stats, computeOptions=computeOptions)
        self.__type = args['type']

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


def pool_worker(type, work_queue, done_queue):
    try:

        if type == LATITUDE:
            calculator = LatitudeHofMoellerCalculator()
        elif type == LONGITUDE:
            calculator = LongitudeHofMoellerCalculator()

        for work in iter(work_queue.get, SENTINEL):
            scifunction = work[0]
            args = work[1:]
            result = calculator.__getattribute__(scifunction)(*args)
            done_queue.put(result)

    except Exception as e:
        e_str = traceback.format_exc(e)
        done_queue.put({'error': e_str})
