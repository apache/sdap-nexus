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
import traceback
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool, Manager

import numpy as np
import pytz
from nexustiles.nexustiles import NexusTileService, NexusTileServiceException
from shapely.geometry import box

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException

SENTINEL = 'STOP'


@nexus_handler
class DailyDifferenceAverageImpl(NexusCalcHandler):
    name = "Daily Difference Average"
    path = "/dailydifferenceaverage"
    description = "Subtracts data in box in Dataset 1 from Dataset 2, then averages the difference per day."
    params = {
        "ds1": {
            "name": "Dataset 1",
            "type": "string",
            "description": "The first Dataset shortname to use in calculation"
        },
        "ds2": {
            "name": "Dataset 2",
            "type": "string",
            "description": "The second Dataset shortname to use in calculation"
        },
        "minLon": {
            "name": "Minimum Longitude",
            "type": "float",
            "description": "Minimum (Western) bounding box Longitude"
        },
        "minLat": {
            "name": "Minimum Latitude",
            "type": "float",
            "description": "Minimum (Southern) bounding box Latitude"
        },
        "maxLon": {
            "name": "Maximum Longitude",
            "type": "float",
            "description": "Maximum (Eastern) bounding box Longitude"
        },
        "maxLat": {
            "name": "Maximum Latitude",
            "type": "float",
            "description": "Maximum (Northern) bounding box Latitude"
        },
        "startTime": {
            "name": "Start Time",
            "type": "long integer",
            "description": "Starting time in milliseconds since midnight Jan. 1st, 1970 UTC"
        },
        "endTime": {
            "name": "End Time",
            "type": "long integer",
            "description": "Ending time in milliseconds since midnight Jan. 1st, 1970 UTC"
        }
    }
    singleton = True

    def __init__(self):
        NexusCalcHandler.__init__(self, skipCassandra=True)

    def calc(self, request, **args):
        min_lat, max_lat, min_lon, max_lon = request.get_min_lat(), request.get_max_lat(), request.get_min_lon(), request.get_max_lon()
        dataset1 = request.get_argument("ds1", None)
        dataset2 = request.get_argument("ds2", None)
        start_time = request.get_start_time()
        end_time = request.get_end_time()

        simple = request.get_argument("simple", None) is not None

        averagebyday = self.get_daily_difference_average_for_box(min_lat, max_lat, min_lon, max_lon, dataset1, dataset2,
                                                                 start_time, end_time)

        averagebyday = sorted(averagebyday, key=lambda dayavg: dayavg[0])

        if simple:

            import matplotlib.pyplot as plt
            from matplotlib.dates import date2num

            times = [date2num(self.date_from_ms(dayavg[0])) for dayavg in averagebyday]
            means = [dayavg[1] for dayavg in averagebyday]
            plt.plot_date(times, means, ls='solid')

            plt.xlabel('Date')
            plt.xticks(rotation=70)
            plt.ylabel(u'Difference from 5-Day mean (\u00B0C)')
            plt.title('Sea Surface Temperature (SST) Anomalies')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig("test.png")

            return averagebyday, None, None
        else:

            result = NexusResults(
                results=[[{'time': dayms, 'mean': avg, 'ds': 0}] for dayms, avg in averagebyday],
                stats={},
                meta=self.get_meta())

            result.extendMeta(min_lat, max_lat, min_lon, max_lon, "", start_time, end_time)
            result.meta()['label'] = u'Difference from 5-Day mean (\u00B0C)'

            return result

    def date_from_ms(self, dayms):
        base_datetime = datetime(1970, 1, 1)
        delta = timedelta(0, 0, 0, dayms)
        return base_datetime + delta

    def get_meta(self):
        meta = {
            "title": "Sea Surface Temperature (SST) Anomalies",
            "description": "SST anomolies are departures from the 5-day pixel mean",
            "units": u'\u00B0C',
        }
        return meta

    def get_daily_difference_average_for_box(self, min_lat, max_lat, min_lon, max_lon, dataset1, dataset2,
                                             start_time,
                                             end_time):

        daysinrange = self._get_tile_service().find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, dataset1,
                                                                start_time, end_time)

        maxprocesses = int(self.algorithm_config.get("multiprocessing", "maxprocesses"))

        if maxprocesses == 1:
            calculator = DailyDifferenceAverageCalculator()
            averagebyday = []
            for dayinseconds in daysinrange:
                result = calculator.calc_average_diff_on_day(min_lat, max_lat, min_lon, max_lon, dataset1, dataset2,
                                                             dayinseconds)
                averagebyday.append((result[0], result[1]))
        else:
            # Create a task to calc average difference for each day
            manager = Manager()
            work_queue = manager.Queue()
            done_queue = manager.Queue()
            for dayinseconds in daysinrange:
                work_queue.put(
                    ('calc_average_diff_on_day', min_lat, max_lat, min_lon, max_lon, dataset1, dataset2, dayinseconds))
            [work_queue.put(SENTINEL) for _ in xrange(0, maxprocesses)]

            # Start new processes to handle the work
            pool = Pool(maxprocesses)
            [pool.apply_async(pool_worker, (work_queue, done_queue)) for _ in xrange(0, maxprocesses)]
            pool.close()

            # Collect the results as [(day (in ms), average difference for that day)]
            averagebyday = []
            for i in xrange(0, len(daysinrange)):
                result = done_queue.get()
                if result[0] == 'error':
                    print >> sys.stderr, result[1]
                    raise NexusProcessingException(reason="Error calculating average by day.")
                rdata = result
                averagebyday.append((rdata[0], rdata[1]))

            pool.terminate()
            manager.shutdown()

        return averagebyday


class DailyDifferenceAverageCalculator(object):
    def __init__(self):
        self.__tile_service = NexusTileService()

    def calc_average_diff_on_day(self, min_lat, max_lat, min_lon, max_lon, dataset1, dataset2, timeinseconds):

        day_of_year = datetime.fromtimestamp(timeinseconds, pytz.utc).timetuple().tm_yday

        ds1_nexus_tiles = self.__tile_service.find_all_tiles_in_box_at_time(min_lat, max_lat, min_lon, max_lon,
                                                                            dataset1,
                                                                            timeinseconds)

        # Initialize list of differences
        differences = []
        # For each ds1tile
        for ds1_tile in ds1_nexus_tiles:
            # Get tile for ds2 using bbox from ds1_tile and day ms
            try:
                ds2_tile = self.__tile_service.find_tile_by_polygon_and_most_recent_day_of_year(
                    box(ds1_tile.bbox.min_lon, ds1_tile.bbox.min_lat, ds1_tile.bbox.max_lon, ds1_tile.bbox.max_lat),
                    dataset2, day_of_year)[0]
                # Subtract ds2 tile from ds1 tile
                diff = np.subtract(ds1_tile.data, ds2_tile.data)
            except NexusTileServiceException:
                # This happens when there is data in ds1tile but all NaNs in ds2tile because the
                # Solr query being used filters out results where stats_count = 0.
                # Technically, this should never happen if ds2 is a climatology generated in part from ds1
                # and it is probably a data error

                # For now, just treat ds2 as an array of all masked data (which essentially discards the ds1 data)
                ds2_tile = np.ma.masked_all(ds1_tile.data.shape)
                diff = np.subtract(ds1_tile.data, ds2_tile)

            # Put results in list of differences
            differences.append(np.ma.array(diff).ravel())

        # Average List of differences
        diffaverage = np.ma.mean(differences).item()
        # Return Average by day
        return int(timeinseconds), diffaverage


def pool_worker(work_queue, done_queue):
    try:
        calculator = DailyDifferenceAverageCalculator()

        for work in iter(work_queue.get, SENTINEL):
            scifunction = work[0]
            args = work[1:]
            result = calculator.__getattribute__(scifunction)(*args)
            done_queue.put(result)
    except Exception as e:
        e_str = traceback.format_exc(e)
        done_queue.put(('error', e_str))
