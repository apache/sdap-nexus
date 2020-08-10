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
from datetime import datetime

import numpy as np
import pytz
from nexustiles.nexustiles import NexusTileService
from shapely import wkt
from shapely.geometry import Polygon

from webservice.NexusHandler import nexus_handler
from webservice.algorithms_spark.NexusCalcSparkHandler import NexusCalcSparkHandler
from webservice.webmodel import NexusResults, NexusProcessingException

SENTINEL = 'STOP'

EPOCH = pytz.timezone('UTC').localize(datetime(1970, 1, 1))


def iso_time_to_epoch(str_time):
    return (datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=pytz.UTC) - EPOCH).total_seconds()


@nexus_handler
class DailyDifferenceAverageNexusImplSpark(NexusCalcSparkHandler):
    name = "Daily Difference Average Spark"
    path = "/dailydifferenceaverage_spark"
    description = "Subtracts data in box in Dataset 1 from Dataset 2, then averages the difference per day."
    params = {
        "dataset": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation"
        },
        "climatology": {
            "name": "Climatology",
            "type": "string",
            "description": "The Climatology shortname to use in calculation"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude"
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ"
        }
    }
    singleton = True

    def __init__(self, **kwargs):
        NexusCalcSparkHandler.__init__(self, **kwargs)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")
        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            try:
                minLat = request.get_min_lat()
                maxLat = request.get_max_lat()
                minLon = request.get_min_lon()
                maxLon = request.get_max_lon()
                bounding_polygon = Polygon([(minLon, minLat),  # (west, south)
                                            (maxLon, minLat),  # (east, south)
                                            (maxLon, maxLat),  # (east, north)
                                            (minLon, maxLat),  # (west, north)
                                            (minLon, minLat)])  # (west, south)
            except:
                raise NexusProcessingException(
                    reason="'b' argument or 'minLon', 'minLat', 'maxLon', and 'maxLat' arguments are required. If 'b' is used, it must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                    code=400)
        dataset = request.get_argument('dataset', None)
        if dataset is None:
            dataset = request.get_argument('ds1', None)
        if dataset is None:
            raise NexusProcessingException(reason="'dataset' or 'ds1' argument is required", code=400)
        climatology = request.get_argument('climatology', None)
        if climatology is None:
            climatology = request.get_argument('ds2', None)
        if climatology is None:
            raise NexusProcessingException(reason="'climatology' or 'ds2' argument is required", code=400)

        try:
            start_time = request.get_start_datetime()
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        plot = request.get_boolean_arg("plot", default=False)

        return bounding_polygon, dataset, climatology, start_time, start_seconds_from_epoch, end_time, end_seconds_from_epoch, plot

    def calc(self, request, **args):
        bounding_polygon, dataset, climatology, start_time, start_seconds_from_epoch, end_time, end_seconds_from_epoch, plot = self.parse_arguments(
            request)

        self.log.debug("Querying for tiles in search domain")
        # Get tile ids in box
        tile_ids = [tile.tile_id for tile in
                    self._get_tile_service().find_tiles_in_polygon(bounding_polygon, dataset,
                                                                   start_seconds_from_epoch, end_seconds_from_epoch,
                                                                   fetch_data=False, fl='id',
                                                                   sort=['tile_min_time_dt asc', 'tile_min_lon asc',
                                                                         'tile_min_lat asc'], rows=5000)]

        # Call spark_matchup
        try:
            spark_result = spark_anomalies_driver(self._tile_service_factory,
                                                  tile_ids,
                                                  wkt.dumps(bounding_polygon),
                                                  dataset,
                                                  climatology,
                                                  sc=self._sc)
        except Exception as e:
            self.log.exception(e)
            raise NexusProcessingException(reason="An unknown error occurred while computing average differences",
                                           code=500)

        average_and_std_by_day = spark_result

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds
        result = DDAResult(
            results=[[{'time': dayms, 'mean': avg_std[0], 'std': avg_std[1], 'ds': 0}] for dayms, avg_std in
                     average_and_std_by_day],
            stats={}, meta=self.get_meta(dataset),
            computeOptions=None, minLat=min_lat,
            maxLat=max_lat, minLon=min_lon,
            maxLon=max_lon, ds=dataset, startTime=start_seconds_from_epoch,
            endTime=end_seconds_from_epoch)
        result.meta()['climatology'] = climatology
        return result

    def get_meta(self, dataset):

        # TODO yea this is broken
        if 'sst' in dataset.lower() or 'mur' in dataset.lower():
            meta = {
                "title": "Sea Surface Temperature (SST) Anomalies",
                "description": "SST anomalies are departures from the 5-day pixel mean",
                "units": u'\u00B0C',
                "label": u'Difference from 5-Day mean (\u00B0C)'
            }
        elif 'chl' in dataset.lower():
            meta = {
                "title": "Chlorophyll Concentration Anomalies",
                "description": "Chlorophyll Concentration anomalies are departures from the 5-day pixel mean",
                "units": u'mg m^-3',
                "label": u'Difference from 5-Day mean (mg m^-3)'
            }
        elif 'sla' in dataset.lower():
            meta = {
                "title": "Sea Level Anomaly Estimate (SLA) Anomalies",
                "description": "SLA anomalies are departures from the 5-day pixel mean",
                "units": u'm',
                "label": u'Difference from 5-Day mean (m)'
            }
        elif 'sss' in dataset.lower():
            meta = {
                "title": "Sea Surface Salinity (SSS) Anomalies",
                "description": "SSS anomalies are departures from the 5-day pixel mean",
                "units": u'psu',
                "label": u'Difference from 5-Day mean (psu)'
            }
        elif 'ccmp' in dataset.lower():
            meta = {
                "title": "Wind Speed Anomalies",
                "description": "Wind Speed anomalies are departures from the 1-day pixel mean",
                "units": u'm/s',
                "label": u'Difference from 1-Day mean (m/s)'
            }
        elif 'trmm' in dataset.lower():
            meta = {
                "title": "Precipitation Anomalies",
                "description": "Precipitation anomalies are departures from the 5-day pixel mean",
                "units": u'mm',
                "label": u'Difference from 5-Day mean (mm)'
            }
        else:
            meta = {
                "title": "Anomalies",
                "description": "Anomalies are departures from the 5-day pixel mean",
                "units": u'',
                "label": u'Difference from 5-Day mean'
            }
        return meta


class DDAResult(NexusResults):
    def toImage(self):
        from StringIO import StringIO
        import matplotlib.pyplot as plt
        from matplotlib.dates import date2num

        times = [date2num(datetime.fromtimestamp(dayavglistdict[0]['time'], pytz.utc).date()) for dayavglistdict in
                 self.results()]
        means = [dayavglistdict[0]['mean'] for dayavglistdict in self.results()]
        plt.plot_date(times, means, '|g-')

        plt.xlabel('Date')
        plt.xticks(rotation=70)
        plt.ylabel(u'Difference from 5-Day mean (\u00B0C)')
        plt.title('Sea Surface Temperature (SST) Anomalies')
        plt.grid(True)
        plt.tight_layout()

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()


from threading import Lock
from shapely.geometry import box

DRIVER_LOCK = Lock()


class NoClimatologyTile(Exception):
    pass


class NoDatasetTile(Exception):
    pass


def determine_parllelism(num_tiles):
    """
    Try to stay at a maximum of 1500 tiles per partition; But don't go over 128 partitions.
    Also, don't go below the default of 8
    """
    num_partitions = max(min(num_tiles // 1500, 128), 8)
    return num_partitions


def spark_anomalies_driver(tile_service_factory, tile_ids, bounding_wkt, dataset, climatology, sc=None):
    from functools import partial

    with DRIVER_LOCK:
        bounding_wkt_b = sc.broadcast(bounding_wkt)
        dataset_b = sc.broadcast(dataset)
        climatology_b = sc.broadcast(climatology)

        # Parallelize list of tile ids
        rdd = sc.parallelize(tile_ids, determine_parllelism(len(tile_ids)))

    def add_tuple_elements(tuple1, tuple2):
        cumulative_sum = tuple1[0] + tuple2[0]
        cumulative_count = tuple1[1] + tuple2[1]

        avg_1 = tuple1[0] / tuple1[1]
        avg_2 = tuple2[0] / tuple2[1]

        cumulative_var = parallel_variance(avg_1, tuple1[1], tuple1[2], avg_2, tuple2[1], tuple2[2])
        return cumulative_sum, cumulative_count, cumulative_var

    def parallel_variance(avg_a, count_a, var_a, avg_b, count_b, var_b):
        # Thanks Wikipedia https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
        delta = avg_b - avg_a
        m_a = var_a * (count_a - 1)
        m_b = var_b * (count_b - 1)
        M2 = m_a + m_b + delta ** 2 * count_a * count_b / (count_a + count_b)
        return M2 / (count_a + count_b - 1)

    def compute_avg_and_std(sum_cnt_var_tuple):
        return sum_cnt_var_tuple[0] / sum_cnt_var_tuple[1], np.sqrt(sum_cnt_var_tuple[2])

    result = rdd \
        .mapPartitions(partial(calculate_diff, tile_service_factory, bounding_wkt=bounding_wkt_b, dataset=dataset_b,
                               climatology=climatology_b)) \
        .reduceByKey(add_tuple_elements) \
        .mapValues(compute_avg_and_std) \
        .sortByKey() \
        .collect()

    return result


def calculate_diff(tile_service_factory, tile_ids, bounding_wkt, dataset, climatology):
    from itertools import chain

    # Construct a list of generators that yield (day, sum, count, variance)
    diff_generators = []

    tile_ids = list(tile_ids)
    if len(tile_ids) == 0:
        return []
    tile_service = tile_service_factory()

    for tile_id in tile_ids:
        # Get the dataset tile
        try:
            dataset_tile = get_dataset_tile(tile_service, wkt.loads(bounding_wkt.value), tile_id)
        except NoDatasetTile:
            # This should only happen if all measurements in a tile become masked after applying the bounding polygon
            continue

        tile_day_of_year = dataset_tile.min_time.timetuple().tm_yday

        # Get the climatology tile
        try:
            climatology_tile = get_climatology_tile(tile_service, wkt.loads(bounding_wkt.value),
                                                    box(dataset_tile.bbox.min_lon,
                                                        dataset_tile.bbox.min_lat,
                                                        dataset_tile.bbox.max_lon,
                                                        dataset_tile.bbox.max_lat),
                                                    climatology.value,
                                                    tile_day_of_year)
        except NoClimatologyTile:
            continue

        diff_generators.append(generate_diff(dataset_tile, climatology_tile))

    return chain(*diff_generators)


def get_dataset_tile(tile_service, search_bounding_shape, tile_id):
    the_time = datetime.now()

    try:
        # Load the dataset tile
        dataset_tile = tile_service.find_tile_by_id(tile_id)[0]
        # Mask it to the search domain
        dataset_tile = tile_service.mask_tiles_to_polygon(search_bounding_shape, [dataset_tile])[0]
    except IndexError:
        raise NoDatasetTile()

    print "%s Time to load dataset tile %s" % (str(datetime.now() - the_time), dataset_tile.tile_id)
    return dataset_tile


def get_climatology_tile(tile_service, search_bounding_shape, tile_bounding_shape, climatology_dataset,
                         tile_day_of_year):
    the_time = datetime.now()
    try:
        # Load the tile
        climatology_tile = tile_service.find_tile_by_polygon_and_most_recent_day_of_year(
            tile_bounding_shape,
            climatology_dataset,
            tile_day_of_year)[0]

    except IndexError:
        raise NoClimatologyTile()

    if search_bounding_shape.contains(tile_bounding_shape):
        # The tile is totally contained in the search area, we don't need to mask it.
        pass
    else:
        # The tile is not totally contained in the search area,
        #     we need to mask the data to the search domain.
        try:
            # Mask it to the search domain
            climatology_tile = tile_service.mask_tiles_to_polygon(search_bounding_shape, [climatology_tile])[0]
        except IndexError:
            raise NoClimatologyTile()

    print "%s Time to load climatology tile %s" % (str(datetime.now() - the_time), climatology_tile.tile_id)
    return climatology_tile


def generate_diff(data_tile, climatology_tile):
    the_time = datetime.now()

    diff = np.subtract(data_tile.data, climatology_tile.data)
    diff_sum = np.nansum(diff)
    diff_var = np.nanvar(diff)
    diff_ct = np.ma.count(diff)

    date_in_seconds = int((datetime.combine(data_tile.min_time.date(), datetime.min.time()).replace(
        tzinfo=pytz.UTC) - EPOCH).total_seconds())

    print "%s Time to generate diff between %s and %s" % (
        str(datetime.now() - the_time), data_tile.tile_id, climatology_tile.tile_id)

    yield (date_in_seconds, (diff_sum, diff_ct, diff_var))
