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
import traceback
from cStringIO import StringIO
from datetime import datetime

import matplotlib.pyplot as plt
import mpld3
import numpy as np
import shapely.geometry
from matplotlib import cm
from matplotlib.ticker import FuncFormatter
from nexustiles.nexustiles import NexusTileService
from pytz import timezone

from webservice.NexusHandler import SparkHandler, nexus_handler
from webservice.webmodel import NexusResults, NoDataException, NexusProcessingException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%SZ'

SENTINEL = 'STOP'
LATITUDE = 0
LONGITUDE = 1


class LongitudeHofMoellerCalculator(object):
    @staticmethod
    def longitude_time_hofmoeller_stats(tile_in_spark):

        (tile_id, index, min_lat, max_lat, min_lon, max_lon) = tile_in_spark

        tile_service = NexusTileService()
        try:
            # Load the dataset tile
            tile = tile_service.find_tile_by_id(tile_id)[0]
            # Mask it to the search domain
            tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, [tile])[0]
        except IndexError:
            return None

        stat = {
            'sequence': index,
            'time': np.ma.min(tile.times),
            'iso_time': tile.min_time.strftime(ISO_8601),
            'lons': []
        }
        points = list(tile.nexus_point_generator())
        data = sorted(points, key=lambda p: p.longitude)
        points_by_lon = itertools.groupby(data, key=lambda p: p.longitude)

        for lon, points_at_lon in points_by_lon:
            values_at_lon = np.array([point.data_val for point in points_at_lon])
            stat['lons'].append({
                'longitude': float(lon),
                'cnt': len(values_at_lon),
                'mean': np.mean(values_at_lon).item(),
                'max': np.max(values_at_lon).item(),
                'min': np.min(values_at_lon).item(),
                'std': np.std(values_at_lon).item()
            })

        return stat


class LatitudeHofMoellerCalculator(object):
    @staticmethod
    def latitude_time_hofmoeller_stats(tile_in_spark):

        (tile_id, index, min_lat, max_lat, min_lon, max_lon) = tile_in_spark

        tile_service = NexusTileService()
        try:
            # Load the dataset tile
            tile = tile_service.find_tile_by_id(tile_id)[0]
            # Mask it to the search domain
            tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, [tile])[0]
        except IndexError:
            return None

        stat = {
            'sequence': index,
            'time': np.ma.min(tile.times),
            'iso_time': tile.min_time.strftime(ISO_8601),
            'lats': []
        }

        points = list(tile.nexus_point_generator())
        data = sorted(points, key=lambda p: p.latitude)
        points_by_lat = itertools.groupby(data, key=lambda p: p.latitude)

        for lat, points_at_lat in points_by_lat:
            values_at_lat = np.array([point.data_val for point in points_at_lat])

            stat['lats'].append({
                'latitude': float(lat),
                'cnt': len(values_at_lat),
                'mean': np.mean(values_at_lat).item(),
                'max': np.max(values_at_lat).item(),
                'min': np.min(values_at_lat).item(),
                'std': np.std(values_at_lat).item()
            })

        return stat


class BaseHoffMoellerHandlerImpl(SparkHandler):
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

    def __init__(self):
        SparkHandler.__init__(self)
        self.log = logging.getLogger(__name__)

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

        spark_master, spark_nexecs, spark_nparts = request.get_spark_cfg()

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        return ds, bounding_polygon, start_seconds_from_epoch, end_seconds_from_epoch, \
               spark_master, spark_nexecs, spark_nparts

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


@nexus_handler
class LatitudeTimeHoffMoellerSparkHandlerImpl(BaseHoffMoellerHandlerImpl):
    name = "Latitude/Time HofMoeller Spark"
    path = "/latitudeTimeHofMoellerSpark"
    description = "Computes a latitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = BaseHoffMoellerHandlerImpl.params
    singleton = True

    def __init__(self):
        BaseHoffMoellerHandlerImpl.__init__(self)

    def calc(self, compute_options, **args):
        ds, bbox, start_time, end_time, spark_master, spark_nexecs, spark_nparts = self.parse_arguments(compute_options)

        min_lon, min_lat, max_lon, max_lat = bbox.bounds

        nexus_tiles_spark = [(tile.tile_id, x, min_lat, max_lat, min_lon, max_lon) for x, tile in enumerate(
            self._tile_service.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon,
                                                 ds, start_time, end_time, fetch_data=False))]

        if len(nexus_tiles_spark) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        # Parallelize list of tile ids
        rdd = self._sc.parallelize(nexus_tiles_spark, determine_parllelism(len(nexus_tiles_spark)))
        results = rdd.map(LatitudeHofMoellerCalculator.latitude_time_hofmoeller_stats).collect()

        results = filter(None, results)
        results = sorted(results, key=lambda entry: entry["time"])

        results = self.applyDeseasonToHofMoeller(results)

        result = HoffMoellerResults(results=results, compute_options=None, type=HoffMoellerResults.LATITUDE,
                                    minLat=min_lat, maxLat=max_lat, minLon=min_lon,
                                    maxLon=max_lon, ds=ds, startTime=start_time, endTime=end_time)
        return result


@nexus_handler
class LongitudeTimeHoffMoellerSparkHandlerImpl(BaseHoffMoellerHandlerImpl):
    name = "Longitude/Time HofMoeller Spark"
    path = "/longitudeTimeHofMoellerSpark"
    description = "Computes a longitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = BaseHoffMoellerHandlerImpl.params
    singleton = True

    def __init__(self):
        BaseHoffMoellerHandlerImpl.__init__(self)

    def calc(self, compute_options, **args):
        ds, bbox, start_time, end_time, spark_master, spark_nexecs, spark_nparts = self.parse_arguments(compute_options)

        min_lon, min_lat, max_lon, max_lat = bbox.bounds

        nexus_tiles_spark = [(tile.tile_id, x, min_lat, max_lat, min_lon, max_lon) for x, tile in enumerate(
            self._tile_service.find_tiles_in_box(min_lat, max_lat, min_lon, max_lon,
                                                 ds, start_time, end_time, fetch_data=False))]

        if len(nexus_tiles_spark) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        # Parallelize list of tile ids
        rdd = self._sc.parallelize(nexus_tiles_spark, determine_parllelism(len(nexus_tiles_spark)))
        results = rdd.map(LongitudeHofMoellerCalculator.longitude_time_hofmoeller_stats).collect()

        results = filter(None, results)
        results = sorted(results, key=lambda entry: entry["time"])

        results = self.applyDeseasonToHofMoeller(results, pivot="lons")

        result = HoffMoellerResults(results=results, compute_options=None, type=HoffMoellerResults.LONGITUDE,
                                    minLat=min_lat, maxLat=max_lat, minLon=min_lon,
                                    maxLon=max_lon, ds=ds, startTime=start_time, endTime=end_time)
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
