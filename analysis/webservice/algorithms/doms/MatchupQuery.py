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

import math
import uuid
from datetime import datetime

import numpy as np
import utm
from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon
from scipy import spatial

import BaseDomsHandler
import ResultsStorage
import datafetch
import fetchedgeimpl
import geo
import workerthread
from webservice.NexusHandler import nexus_handler


@nexus_handler
class CombinedDomsMatchupQueryHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "Experimental Combined DOMS In-Situ Matchup"
    path = "/domsmatchup"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self)

    def fetchData(self, endpoints, startTime, endTime, bbox, depth_min, depth_max, platforms):

        boundsConstrainer = geo.BoundsConstrainer(asString=bbox)
        threads = []
        for endpoint in endpoints:
            thread = workerthread.WorkerThread(datafetch.fetchData,
                                               params=(endpoint, startTime, endTime, bbox, depth_min, depth_max))
            threads.append(thread)
        workerthread.wait(threads, startFirst=True, poll=0.01)

        data2 = []
        for thread in threads:
            data, bounds = thread.results
            data2 += data
            boundsConstrainer.testOtherConstrainer(bounds)

        return data2, boundsConstrainer

    def __parseDatetime(self, dtString):
        dt = datetime.strptime(dtString, "%Y-%m-%dT%H:%M:%SZ")
        epoch = datetime.utcfromtimestamp(0)
        time = (dt - epoch).total_seconds() * 1000.0
        return time

    def calc(self, computeOptions, **args):
        primary = computeOptions.get_argument("primary", None)
        matchup = computeOptions.get_argument("matchup", None)
        startTime = computeOptions.get_argument("s", None)
        endTime = computeOptions.get_argument("e", None)
        bbox = computeOptions.get_argument("b", None)
        timeTolerance = computeOptions.get_float_arg("tt")
        depth_min = computeOptions.get_float_arg("depthMin", default=None)
        depth_max = computeOptions.get_float_arg("depthMax", default=None)
        radiusTolerance = computeOptions.get_float_arg("rt")
        platforms = computeOptions.get_argument("platforms", None)

        if primary is None or len(primary) == 0:
            raise Exception("No primary dataset specified")

        if matchup is None or len(matchup) == 0:
            raise Exception("No matchup datasets specified")

        start = self._now()

        primarySpec = self.getDataSourceByName(primary)
        if primarySpec is None:
            raise Exception("Specified primary dataset not found using identifier '%s'" % primary)

        primaryData, bounds = self.fetchData([primarySpec], startTime, endTime, bbox, depth_min, depth_max, platforms)

        primaryContext = MatchupContext(primaryData)

        matchupIds = matchup.split(",")

        for matchupId in matchupIds:
            matchupSpec = self.getDataSourceByName(matchupId)

            if matchupSpec is not None:  # Then it's in the in-situ configuration
                proc = InsituDatasetProcessor(primaryContext, matchupSpec, startTime, endTime, bbox, depth_min,
                                              depth_max,
                                              platforms, timeTolerance, radiusTolerance)
                proc.start()
            else:  # We assume it to be a Nexus tiled dataset

                '''
                Single Threaded at the moment...
                '''
                daysinrange = self._get_tile_service().find_days_in_range_asc(bounds.south, bounds.north, bounds.west,
                                                                        bounds.east, matchupId,
                                                                        self.__parseDatetime(startTime) / 1000,
                                                                        self.__parseDatetime(endTime) / 1000)

                tilesByDay = {}
                for dayTimestamp in daysinrange:
                    ds1_nexus_tiles = self._get_tile_service().get_tiles_bounded_by_box_at_time(bounds.south, bounds.north,
                                                                                          bounds.west, bounds.east,
                                                                                          matchupId, dayTimestamp)

                    # print "***", type(ds1_nexus_tiles)
                    # print ds1_nexus_tiles[0].__dict__
                    tilesByDay[dayTimestamp] = ds1_nexus_tiles

                primaryContext.processGridded(tilesByDay, matchupId, radiusTolerance, timeTolerance)

        matches, numMatches = primaryContext.getFinal(len(matchupIds))

        end = self._now()

        args = {
            "primary": primary,
            "matchup": matchupIds,
            "startTime": startTime,
            "endTime": endTime,
            "bbox": bbox,
            "timeTolerance": timeTolerance,
            "depthMin": depth_min,
            "depthMax": depth_max,
            "radiusTolerance": radiusTolerance,
            "platforms": platforms
        }

        details = {
            "timeToComplete": (end - start),
            "numInSituRecords": primaryContext.insituCount,
            "numInSituMatched": primaryContext.insituMatches,
            "numGriddedChecked": primaryContext.griddedCount,
            "numGriddedMatched": primaryContext.griddedMatched
        }

        with ResultsStorage.ResultsStorage() as resultsStorage:
            execution_id = resultsStorage.insertResults(results=matches, params=args, stats=details, startTime=start,
                                                        completeTime=end, userEmail="")

        return BaseDomsHandler.DomsQueryResults(results=matches, args=args, details=details, bounds=None, count=None,
                                                computeOptions=None, executionId=execution_id)


class MatchupContextMap:
    def __init__(self):
        pass

    def add(self, context):
        pass

    def delete(self, context):
        pass


class MatchupContext:
    def __init__(self, primaryData):
        self.id = str(uuid.uuid4())

        self.griddedCount = 0
        self.griddedMatched = 0

        self.insituCount = len(primaryData)
        self.insituMatches = 0

        self.primary = primaryData
        for r in self.primary:
            r["matches"] = []

        self.data = []
        for s in primaryData:
            u = utm.from_latlon(s["y"], s["x"])
            v = (u[0], u[1], 0.0)
            self.data.append(v)

        if len(self.data) > 0:
            self.tree = spatial.KDTree(self.data)
        else:
            self.tree = None

    def getFinal(self, minMatchesToInclude):

        matched = []
        ttlMatches = 0
        for m in self.primary:
            if len(m["matches"]) >= minMatchesToInclude:
                matched.append(m)
                ttlMatches += len(m["matches"])

        return matched, ttlMatches

    def processGridded(self, tilesByDay, source, xyTolerance, timeTolerance):
        for r in self.primary:
            foundSatNodes = self.__getSatNodeForLatLonAndTime(tilesByDay, source, r["y"], r["x"], r["time"],
                                                              xyTolerance)
            self.griddedCount += 1
            self.griddedMatched += len(foundSatNodes)
            r["matches"].extend(foundSatNodes)

    def processInSitu(self, records, xyTolerance, timeTolerance):
        if self.tree is not None:
            for s in records:
                self.insituCount += 1
                u = utm.from_latlon(s["y"], s["x"])
                coords = np.array([u[0], u[1], 0])
                ball = self.tree.query_ball_point(coords, xyTolerance)

                self.insituMatches += len(ball)

                for i in ball:
                    match = self.primary[i]
                    if abs(match["time"] - s["time"]) <= (timeTolerance * 1000.0):
                        match["matches"].append(s)

    def __getValueForLatLon(self, chunks, lat, lon, arrayName="data"):
        value = get_approximate_value_for_lat_lon(chunks, lat, lon, arrayName)
        return value

    def __checkNumber(self, value):
        if isinstance(value, float) and (math.isnan(value) or value == np.nan):
            value = None
        elif value is not None:
            value = float(value)
        return value

    def __buildSwathIndexes(self, chunk):
        latlons = []
        utms = []
        indexes = []
        for i in range(0, len(chunk.latitudes)):
            _lat = chunk.latitudes[i]
            if isinstance(_lat, np.ma.core.MaskedConstant):
                continue
            for j in range(0, len(chunk.longitudes)):
                _lon = chunk.longitudes[j]
                if isinstance(_lon, np.ma.core.MaskedConstant):
                    continue

                value = self.__getChunkValueAtIndex(chunk, (i, j))
                if isinstance(value, float) and (math.isnan(value) or value == np.nan):
                    continue

                u = utm.from_latlon(_lat, _lon)
                v = (u[0], u[1], 0.0)
                latlons.append((_lat, _lon))
                utms.append(v)
                indexes.append((i, j))

        tree = None
        if len(latlons) > 0:
            tree = spatial.KDTree(utms)

        chunk.swathIndexing = {
            "tree": tree,
            "latlons": latlons,
            "indexes": indexes
        }

    def __getChunkIndexesForLatLon(self, chunk, lat, lon, xyTolerance):
        foundIndexes = []
        foundLatLons = []

        if "swathIndexing" not in chunk.__dict__:
            self.__buildSwathIndexes(chunk)

        tree = chunk.swathIndexing["tree"]
        if tree is not None:
            indexes = chunk.swathIndexing["indexes"]
            latlons = chunk.swathIndexing["latlons"]
            u = utm.from_latlon(lat, lon)
            coords = np.array([u[0], u[1], 0])
            ball = tree.query_ball_point(coords, xyTolerance)
            for i in ball:
                foundIndexes.append(indexes[i])
                foundLatLons.append(latlons[i])
        return foundIndexes, foundLatLons

    def __getChunkValueAtIndex(self, chunk, index, arrayName=None):

        if arrayName is None or arrayName == "data":
            data_val = chunk.data[0][index[0]][index[1]]
        else:
            data_val = chunk.meta_data[arrayName][0][index[0]][index[1]]
        return data_val.item() if (data_val is not np.ma.masked) and data_val.size == 1 else float('Nan')

    def __getSatNodeForLatLonAndTime(self, chunksByDay, source, lat, lon, searchTime, xyTolerance):
        timeDiff = 86400 * 365 * 1000
        foundNodes = []

        for ts in chunksByDay:
            chunks = chunksByDay[ts]
            if abs((ts * 1000) - searchTime) < timeDiff:
                for chunk in chunks:
                    indexes, latlons = self.__getChunkIndexesForLatLon(chunk, lat, lon, xyTolerance)

                    # for index in indexes:
                    for i in range(0, len(indexes)):
                        index = indexes[i]
                        latlon = latlons[i]
                        sst = None
                        sss = None
                        windSpeed = None
                        windDirection = None
                        windU = None
                        windV = None

                        value = self.__getChunkValueAtIndex(chunk, index)

                        if isinstance(value, float) and (math.isnan(value) or value == np.nan):
                            continue

                        if "GHRSST" in source:
                            sst = value
                        elif "ASCATB" in source:
                            windU = value
                        elif "SSS" in source:  # SMAP
                            sss = value

                        if len(chunks) > 0 and "wind_dir" in chunks[0].meta_data:
                            windDirection = self.__checkNumber(self.__getChunkValueAtIndex(chunk, index, "wind_dir"))
                        if len(chunks) > 0 and "wind_v" in chunks[0].meta_data:
                            windV = self.__checkNumber(self.__getChunkValueAtIndex(chunk, index, "wind_v"))
                        if len(chunks) > 0 and "wind_speed" in chunks[0].meta_data:
                            windSpeed = self.__checkNumber(self.__getChunkValueAtIndex(chunk, index, "wind_speed"))

                        foundNode = {
                            "sea_water_temperature": sst,
                            "sea_water_salinity": sss,
                            "wind_speed": windSpeed,
                            "wind_direction": windDirection,
                            "wind_u": windU,
                            "wind_v": windV,
                            "time": ts,
                            "x": self.__checkNumber(latlon[1]),
                            "y": self.__checkNumber(latlon[0]),
                            "depth": 0,
                            "sea_water_temperature_depth": 0,
                            "source": source,
                            "id": "%s:%s:%s" % (ts, lat, lon)
                        }

                        foundNodes.append(foundNode)
                timeDiff = abs(ts - searchTime)

        return foundNodes

    def __getSatNodeForLatLonAndTime__(self, chunksByDay, source, lat, lon, searchTime):

        timeDiff = 86400 * 365 * 1000
        foundNodes = []

        for ts in chunksByDay:
            chunks = chunksByDay[ts]
            # print chunks
            # ts = calendar.timegm(chunks.start.utctimetuple()) * 1000
            if abs((ts * 1000) - searchTime) < timeDiff:
                value = self.__getValueForLatLon(chunks, lat, lon, arrayName="data")
                value = self.__checkNumber(value)

                # _Really_ don't like doing it this way...

                sst = None
                sss = None
                windSpeed = None
                windDirection = None
                windU = None
                windV = None

                if "GHRSST" in source:
                    sst = value

                if "ASCATB" in source:
                    windU = value

                if len(chunks) > 0 and "wind_dir" in chunks[0].meta_data:
                    windDirection = self.__checkNumber(self.__getValueForLatLon(chunks, lat, lon, arrayName="wind_dir"))
                if len(chunks) > 0 and "wind_v" in chunks[0].meta_data:
                    windV = self.__checkNumber(self.__getValueForLatLon(chunks, lat, lon, arrayName="wind_v"))
                if len(chunks) > 0 and "wind_speed" in chunks[0].meta_data:
                    windSpeed = self.__checkNumber(self.__getValueForLatLon(chunks, lat, lon, arrayName="wind_speed"))

                foundNode = {
                    "sea_water_temperature": sst,
                    "sea_water_salinity": sss,
                    "wind_speed": windSpeed,
                    "wind_direction": windDirection,
                    "wind_uv": {
                        "u": windU,
                        "v": windV
                    },
                    "time": ts,
                    "x": lon,
                    "y": lat,
                    "depth": 0,
                    "sea_water_temperature_depth": 0,
                    "source": source,
                    "id": "%s:%s:%s" % (ts, lat, lon)
                }

                isValidNode = True
                if "ASCATB" in source and windSpeed is None:
                    isValidNode = None

                if isValidNode:
                    foundNodes.append(foundNode)
                timeDiff = abs(ts - searchTime)

        return foundNodes


class InsituDatasetProcessor:
    def __init__(self, primary, datasource, startTime, endTime, bbox, depth_min, depth_max, platforms, timeTolerance,
                 radiusTolerance):
        self.primary = primary
        self.datasource = datasource
        self.startTime = startTime
        self.endTime = endTime
        self.bbox = bbox
        self.depth_min = depth_min
        self.depth_max = depth_max
        self.platforms = platforms
        self.timeTolerance = timeTolerance
        self.radiusTolerance = radiusTolerance

    def start(self):
        def callback(pageData):
            self.primary.processInSitu(pageData, self.radiusTolerance, self.timeTolerance)

        fetchedgeimpl.fetch(self.datasource, self.startTime, self.endTime, self.bbox, self.depth_min, self.depth_max,
                            self.platforms, pageCallback=callback)


class InsituPageProcessor:
    def __init__(self):
        pass
