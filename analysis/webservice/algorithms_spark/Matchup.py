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



import json
import logging
import threading
from datetime import datetime
from itertools import chain
from math import cos, radians

import numpy as np
import pyproj
import requests
from nexustiles.nexustiles import NexusTileService
from pytz import timezone, UTC
from scipy import spatial
from shapely import wkt
from shapely.geometry import Point
from shapely.geometry import box
from shapely.geos import WKTReadingError

from webservice.NexusHandler import nexus_handler
from webservice.algorithms_spark.NexusCalcSparkHandler import NexusCalcSparkHandler
from webservice.algorithms.doms import config as edge_endpoints
from webservice.algorithms.doms import values as doms_values
from webservice.algorithms.doms.BaseDomsHandler import DomsQueryResults
from webservice.algorithms.doms.ResultsStorage import ResultsStorage
from webservice.webmodel import NexusProcessingException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


def iso_time_to_epoch(str_time):
    return (datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=UTC) - EPOCH).total_seconds()


@nexus_handler
class Matchup(NexusCalcSparkHandler):
    name = "Matchup"
    path = "/match_spark"
    description = "Match measurements between two or more datasets"

    params = {
        "primary": {
            "name": "Primary Dataset",
            "type": "string",
            "description": "The Primary dataset used to find matches for. Required"
        },
        "matchup": {
            "name": "Match-Up Datasets",
            "type": "comma-delimited string",
            "description": "The Dataset(s) being searched for measurements that match the Primary. Required"
        },
        "parameter": {
            "name": "Match-Up Parameter",
            "type": "string",
            "description": "The parameter of interest used for the match up. One of 'sst', 'sss', 'wind'. Required"
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
        "depthMin": {
            "name": "Minimum Depth",
            "type": "float",
            "description": "Minimum depth of measurements. Must be less than depthMax. Optional. Default: no limit"
        },
        "depthMax": {
            "name": "Maximum Depth",
            "type": "float",
            "description": "Maximum depth of measurements. Must be greater than depthMin. Optional. Default: no limit"
        },
        "tt": {
            "name": "Time Tolerance",
            "type": "long",
            "description": "Tolerance in time (seconds) when comparing two measurements. Optional. Default: 86400"
        },
        "rt": {
            "name": "Radius Tolerance",
            "type": "float",
            "description": "Tolerance in radius (meters) when comparing two measurements. Optional. Default: 1000"
        },
        "platforms": {
            "name": "Platforms",
            "type": "comma-delimited integer",
            "description": "Platforms to include for matchup consideration. Required"
        },
        "matchOnce": {
            "name": "Match Once",
            "type": "boolean",
            "description": "Optional True/False flag used to determine if more than one match per primary point is returned. "
                           + "If true, only the nearest point will be returned for each primary point. "
                           + "If false, all points within the tolerances will be returned for each primary point. Default: False"
        },
        "resultSizeLimit": {
            "name": "Result Size Limit",
            "type": "int",
            "description": "Optional integer value that limits the number of results returned from the matchup. "
                           "If the number of primary matches is greater than this limit, the service will respond with "
                           "(HTTP 202: Accepted) and an empty response body. A value of 0 means return all results. "
                           "Default: 500"
        }
    }
    singleton = True

    def __init__(self, algorithm_config=None, sc=None):
        NexusCalcSparkHandler.__init__(self, algorithm_config=algorithm_config, sc=sc)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")
        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(
                reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                code=400)
        primary_ds_name = request.get_argument('primary', None)
        if primary_ds_name is None:
            raise NexusProcessingException(reason="'primary' argument is required", code=400)
        matchup_ds_names = request.get_argument('matchup', None)
        if matchup_ds_names is None:
            raise NexusProcessingException(reason="'matchup' argument is required", code=400)

        parameter_s = request.get_argument('parameter', 'sst')
        if parameter_s not in ['sst', 'sss', 'wind']:
            raise NexusProcessingException(
                reason="Parameter %s not supported. Must be one of 'sst', 'sss', 'wind'." % parameter_s, code=400)

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

        if start_time > end_time:
            raise NexusProcessingException(
                reason="The starting time must be before the ending time. Received startTime: %s, endTime: %s" % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        depth_min = request.get_decimal_arg('depthMin', default=None)
        depth_max = request.get_decimal_arg('depthMax', default=None)

        if depth_min is not None and depth_max is not None and depth_min >= depth_max:
            raise NexusProcessingException(
                reason="Depth Min should be less than Depth Max", code=400)

        time_tolerance = request.get_int_arg('tt', default=86400)
        radius_tolerance = request.get_decimal_arg('rt', default=1000.0)
        platforms = request.get_argument('platforms', None)
        if platforms is None:
            raise NexusProcessingException(reason="'platforms' argument is required", code=400)
        try:
            p_validation = platforms.split(',')
            p_validation = [int(p) for p in p_validation]
            del p_validation
        except:
            raise NexusProcessingException(reason="platforms must be a comma-delimited list of integers", code=400)

        match_once = request.get_boolean_arg("matchOnce", default=False)

        result_size_limit = request.get_int_arg("resultSizeLimit", default=500)

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        return bounding_polygon, primary_ds_name, matchup_ds_names, parameter_s, \
               start_time, start_seconds_from_epoch, end_time, end_seconds_from_epoch, \
               depth_min, depth_max, time_tolerance, radius_tolerance, \
               platforms, match_once, result_size_limit

    def calc(self, request, **args):
        start = datetime.utcnow()
        # TODO Assuming Satellite primary
        bounding_polygon, primary_ds_name, matchup_ds_names, parameter_s, \
        start_time, start_seconds_from_epoch, end_time, end_seconds_from_epoch, \
        depth_min, depth_max, time_tolerance, radius_tolerance, \
        platforms, match_once, result_size_limit = self.parse_arguments(request)

        with ResultsStorage() as resultsStorage:

            execution_id = str(resultsStorage.insertExecution(None, start, None, None))

        self.log.debug("Querying for tiles in search domain")
        # Get tile ids in box
        tile_ids = [tile.tile_id for tile in
                    self._get_tile_service().find_tiles_in_polygon(bounding_polygon, primary_ds_name,
                                                             start_seconds_from_epoch, end_seconds_from_epoch,
                                                             fetch_data=False, fl='id',
                                                             sort=['tile_min_time_dt asc', 'tile_min_lon asc',
                                                                   'tile_min_lat asc'], rows=5000)]

        # Call spark_matchup
        self.log.debug("Calling Spark Driver")
        try:
            spark_result = spark_matchup_driver(tile_ids, wkt.dumps(bounding_polygon), primary_ds_name,
                                                matchup_ds_names, parameter_s, depth_min, depth_max, time_tolerance,
                                                radius_tolerance, platforms, match_once, sc=self._sc)
        except Exception as e:
            self.log.exception(e)
            raise NexusProcessingException(reason="An unknown error occurred while computing matches", code=500)

        end = datetime.utcnow()

        self.log.debug("Building and saving results")
        args = {
            "primary": primary_ds_name,
            "matchup": matchup_ds_names,
            "startTime": start_time,
            "endTime": end_time,
            "bbox": request.get_argument('b'),
            "timeTolerance": time_tolerance,
            "radiusTolerance": float(radius_tolerance),
            "platforms": platforms,
            "parameter": parameter_s
        }

        if depth_min is not None:
            args["depthMin"] = float(depth_min)

        if depth_max is not None:
            args["depthMax"] = float(depth_max)

        total_keys = len(spark_result.keys())
        total_values = sum(len(v) for v in spark_result.itervalues())
        details = {
            "timeToComplete": int((end - start).total_seconds()),
            "numInSituRecords": 0,
            "numInSituMatched": total_values,
            "numGriddedChecked": 0,
            "numGriddedMatched": total_keys
        }

        matches = Matchup.convert_to_matches(spark_result)

        def do_result_insert():
            with ResultsStorage() as storage:
                storage.insertResults(results=matches, params=args, stats=details,
                                      startTime=start, completeTime=end, userEmail="",
                                      execution_id=execution_id)

        threading.Thread(target=do_result_insert).start()

        if 0 < result_size_limit < len(matches):
            result = DomsQueryResults(results=None, args=args, details=details, bounds=None, count=None,
                                      computeOptions=None, executionId=execution_id, status_code=202)
        else:
            result = DomsQueryResults(results=matches, args=args, details=details, bounds=None, count=None,
                                      computeOptions=None, executionId=execution_id)

        return result

    @classmethod
    def convert_to_matches(cls, spark_result):
        matches = []
        for primary_domspoint, matched_domspoints in spark_result.iteritems():
            p_matched = [cls.domspoint_to_dict(p_match) for p_match in matched_domspoints]

            primary = cls.domspoint_to_dict(primary_domspoint)
            primary['matches'] = list(p_matched)
            matches.append(primary)
        return matches

    @staticmethod
    def domspoint_to_dict(domspoint):
        return {
            "sea_water_temperature": domspoint.sst,
            "sea_water_temperature_depth": domspoint.sst_depth,
            "sea_water_salinity": domspoint.sss,
            "sea_water_salinity_depth": domspoint.sss_depth,
            "wind_speed": domspoint.wind_speed,
            "wind_direction": domspoint.wind_direction,
            "wind_u": domspoint.wind_u,
            "wind_v": domspoint.wind_v,
            "platform": doms_values.getPlatformById(domspoint.platform),
            "device": doms_values.getDeviceById(domspoint.device),
            "x": str(domspoint.longitude),
            "y": str(domspoint.latitude),
            "point": "Point(%s %s)" % (domspoint.longitude, domspoint.latitude),
            "time": datetime.strptime(domspoint.time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC),
            "fileurl": domspoint.file_url,
            "id": domspoint.data_id,
            "source": domspoint.source,
        }


class DomsPoint(object):
    def __init__(self, longitude=None, latitude=None, time=None, depth=None, data_id=None):

        self.time = time
        self.longitude = longitude
        self.latitude = latitude
        self.depth = depth
        self.data_id = data_id

        self.wind_u = None
        self.wind_v = None
        self.wind_direction = None
        self.wind_speed = None
        self.sst = None
        self.sst_depth = None
        self.sss = None
        self.sss_depth = None
        self.source = None
        self.depth = None
        self.platform = None
        self.device = None
        self.file_url = None

    def __repr__(self):
        return str(self.__dict__)

    @staticmethod
    def from_nexus_point(nexus_point, tile=None, parameter='sst'):
        point = DomsPoint()

        point.data_id = "%s[%s]" % (tile.tile_id, nexus_point.index)

        # TODO Not an ideal solution; but it works for now.
        if parameter == 'sst':
            point.sst = nexus_point.data_val.item()
        elif parameter == 'sss':
            point.sss = nexus_point.data_val.item()
        elif parameter == 'wind':
            point.wind_u = nexus_point.data_val.item()
            try:
                point.wind_v = tile.meta_data['wind_v'][tuple(nexus_point.index)].item()
            except (KeyError, IndexError):
                pass
            try:
                point.wind_direction = tile.meta_data['wind_dir'][tuple(nexus_point.index)].item()
            except (KeyError, IndexError):
                pass
            try:
                point.wind_speed = tile.meta_data['wind_speed'][tuple(nexus_point.index)].item()
            except (KeyError, IndexError):
                pass
        else:
            raise NotImplementedError('%s not supported. Only sst, sss, and wind parameters are supported.' % parameter)

        point.longitude = nexus_point.longitude.item()
        point.latitude = nexus_point.latitude.item()

        point.time = datetime.utcfromtimestamp(nexus_point.time).strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            point.depth = nexus_point.depth
        except KeyError:
            # No depth associated with this measurement
            pass

        point.sst_depth = 0
        point.source = tile.dataset
        point.file_url = tile.granule

        # TODO device should change based on the satellite making the observations.
        point.platform = 9
        point.device = 5
        return point

    @staticmethod
    def from_edge_point(edge_point):
        point = DomsPoint()

        try:
            x, y = wkt.loads(edge_point['point']).coords[0]
        except WKTReadingError:
            try:
                x, y = Point(*[float(c) for c in edge_point['point'].split(' ')]).coords[0]
            except ValueError:
                y, x = Point(*[float(c) for c in edge_point['point'].split(',')]).coords[0]

        point.longitude = x
        point.latitude = y

        point.time = edge_point['time']

        point.wind_u = edge_point.get('eastward_wind')
        point.wind_v = edge_point.get('northward_wind')
        point.wind_direction = edge_point.get('wind_direction')
        point.wind_speed = edge_point.get('wind_speed')
        point.sst = edge_point.get('sea_water_temperature')
        point.sst_depth = edge_point.get('sea_water_temperature_depth')
        point.sss = edge_point.get('sea_water_salinity')
        point.sss_depth = edge_point.get('sea_water_salinity_depth')
        point.source = edge_point.get('source')
        point.platform = edge_point.get('platform')
        point.device = edge_point.get('device')
        point.file_url = edge_point.get('fileurl')

        try:
            point.data_id = unicode(edge_point['id'])
        except KeyError:
            point.data_id = "%s:%s:%s" % (point.time, point.longitude, point.latitude)

        return point


from threading import Lock

DRIVER_LOCK = Lock()


def spark_matchup_driver(tile_ids, bounding_wkt, primary_ds_name, matchup_ds_names, parameter, depth_min, depth_max,
                         time_tolerance, radius_tolerance, platforms, match_once, sc=None):
    from functools import partial

    with DRIVER_LOCK:
        # Broadcast parameters
        primary_b = sc.broadcast(primary_ds_name)
        matchup_b = sc.broadcast(matchup_ds_names)
        depth_min_b = sc.broadcast(float(depth_min) if depth_min is not None else None)
        depth_max_b = sc.broadcast(float(depth_max) if depth_max is not None else None)
        tt_b = sc.broadcast(time_tolerance)
        rt_b = sc.broadcast(float(radius_tolerance))
        platforms_b = sc.broadcast(platforms)
        bounding_wkt_b = sc.broadcast(bounding_wkt)
        parameter_b = sc.broadcast(parameter)

        # Parallelize list of tile ids
        rdd = sc.parallelize(tile_ids, determine_parllelism(len(tile_ids)))

    # Map Partitions ( list(tile_id) )
    rdd_filtered = rdd.mapPartitions(
        partial(match_satellite_to_insitu, primary_b=primary_b, matchup_b=matchup_b, parameter_b=parameter_b, tt_b=tt_b,
                rt_b=rt_b, platforms_b=platforms_b, bounding_wkt_b=bounding_wkt_b, depth_min_b=depth_min_b,
                depth_max_b=depth_max_b), preservesPartitioning=True) \
        .filter(lambda p_m_tuple: abs(
        iso_time_to_epoch(p_m_tuple[0].time) - iso_time_to_epoch(p_m_tuple[1].time)) <= time_tolerance)

    if match_once:
        # Only the 'nearest' point for each primary should be returned. Add an extra map/reduce which calculates
        # the distance and finds the minimum

        # Method used for calculating the distance between 2 DomsPoints
        from pyproj import Geod

        def dist(primary, matchup):
            wgs84_geod = Geod(ellps='WGS84')
            lat1, lon1 = (primary.latitude, primary.longitude)
            lat2, lon2 = (matchup.latitude, matchup.longitude)
            az12, az21, distance = wgs84_geod.inv(lon1, lat1, lon2, lat2)
            return distance

        rdd_filtered = rdd_filtered \
            .map(lambda (primary, matchup): tuple([primary, tuple([matchup, dist(primary, matchup)])])) \
            .reduceByKey(lambda match_1, match_2: match_1 if match_1[1] < match_2[1] else match_2) \
            .mapValues(lambda x: [x[0]])
    else:
        rdd_filtered = rdd_filtered \
            .combineByKey(lambda value: [value],  # Create 1 element list
                          lambda value_list, value: value_list + [value],  # Add 1 element to list
                          lambda value_list_a, value_list_b: value_list_a + value_list_b)  # Add two lists together

    result_as_map = rdd_filtered.collectAsMap()

    return result_as_map


def determine_parllelism(num_tiles):
    """
    Try to stay at a maximum of 140 tiles per partition; But don't go over 128 partitions.
    Also, don't go below the default of 8
    """
    num_partitions = max(min(num_tiles / 140, 128), 8)
    return num_partitions


def add_meters_to_lon_lat(lon, lat, meters):
    """
    Uses a simple approximation of
    1 degree latitude = 111,111 meters
    1 degree longitude = 111,111 meters * cosine(latitude)
    :param lon: longitude to add meters to
    :param lat: latitude to add meters to
    :param meters: meters to add to the longitude and latitude values
    :return: (longitude, latitude) increased by given meters
    """
    longitude = lon + ((meters / 111111) * cos(radians(lat)))
    latitude = lat + (meters / 111111)

    return longitude, latitude


def match_satellite_to_insitu(tile_ids, primary_b, matchup_b, parameter_b, tt_b, rt_b, platforms_b,
                              bounding_wkt_b, depth_min_b, depth_max_b):
    the_time = datetime.now()
    tile_ids = list(tile_ids)
    if len(tile_ids) == 0:
        return []
    tile_service = NexusTileService()

    # Determine the spatial temporal extents of this partition of tiles
    tiles_bbox = tile_service.get_bounding_box(tile_ids)
    tiles_min_time = tile_service.get_min_time(tile_ids)
    tiles_max_time = tile_service.get_max_time(tile_ids)

    # Increase spatial extents by the radius tolerance
    matchup_min_lon, matchup_min_lat = add_meters_to_lon_lat(tiles_bbox.bounds[0], tiles_bbox.bounds[1],
                                                             -1 * rt_b.value)
    matchup_max_lon, matchup_max_lat = add_meters_to_lon_lat(tiles_bbox.bounds[2], tiles_bbox.bounds[3], rt_b.value)

    # Don't go outside of the search domain
    search_min_x, search_min_y, search_max_x, search_max_y = wkt.loads(bounding_wkt_b.value).bounds
    matchup_min_lon = max(matchup_min_lon, search_min_x)
    matchup_min_lat = max(matchup_min_lat, search_min_y)
    matchup_max_lon = min(matchup_max_lon, search_max_x)
    matchup_max_lat = min(matchup_max_lat, search_max_y)

    # Find the centroid of the matchup bounding box and initialize the projections
    matchup_center = box(matchup_min_lon, matchup_min_lat, matchup_max_lon, matchup_max_lat).centroid.coords[0]
    aeqd_proj = pyproj.Proj(proj='aeqd', lon_0=matchup_center[0], lat_0=matchup_center[1])
    lonlat_proj = pyproj.Proj(proj='lonlat')

    # Increase temporal extents by the time tolerance
    matchup_min_time = tiles_min_time - tt_b.value
    matchup_max_time = tiles_max_time + tt_b.value
    print "%s Time to determine spatial-temporal extents for partition %s to %s" % (
        str(datetime.now() - the_time), tile_ids[0], tile_ids[-1])

    # Query edge for all points within the spatial-temporal extents of this partition
    the_time = datetime.now()
    edge_session = requests.Session()
    edge_results = []
    with edge_session:
        for insitudata_name in matchup_b.value.split(','):
            bbox = ','.join(
                [str(matchup_min_lon), str(matchup_min_lat), str(matchup_max_lon), str(matchup_max_lat)])
            edge_response = query_edge(insitudata_name, parameter_b.value, matchup_min_time, matchup_max_time, bbox,
                                       platforms_b.value, depth_min_b.value, depth_max_b.value, session=edge_session)
            if edge_response['totalResults'] == 0:
                continue
            r = edge_response['results']
            for p in r:
                p['source'] = insitudata_name
            edge_results.extend(r)
    print "%s Time to call edge for partition %s to %s" % (str(datetime.now() - the_time), tile_ids[0], tile_ids[-1])
    if len(edge_results) == 0:
        return []

    # Convert edge points to utm
    the_time = datetime.now()
    matchup_points = np.ndarray((len(edge_results), 2), dtype=np.float32)
    for n, edge_point in enumerate(edge_results):
        try:
            x, y = wkt.loads(edge_point['point']).coords[0]
        except WKTReadingError:
            try:
                x, y = Point(*[float(c) for c in edge_point['point'].split(' ')]).coords[0]
            except ValueError:
                y, x = Point(*[float(c) for c in edge_point['point'].split(',')]).coords[0]

        matchup_points[n][0], matchup_points[n][1] = pyproj.transform(p1=lonlat_proj, p2=aeqd_proj, x=x, y=y)
    print "%s Time to convert match points for partition %s to %s" % (
        str(datetime.now() - the_time), tile_ids[0], tile_ids[-1])

    # Build kdtree from matchup points
    the_time = datetime.now()
    m_tree = spatial.cKDTree(matchup_points, leafsize=30)
    print "%s Time to build matchup tree" % (str(datetime.now() - the_time))

    # The actual matching happens in the generator. This is so that we only load 1 tile into memory at a time
    match_generators = [match_tile_to_point_generator(tile_service, tile_id, m_tree, edge_results, bounding_wkt_b.value,
                                                      parameter_b.value, rt_b.value, lonlat_proj, aeqd_proj) for tile_id
                        in tile_ids]

    return chain(*match_generators)


def match_tile_to_point_generator(tile_service, tile_id, m_tree, edge_results, search_domain_bounding_wkt,
                                  search_parameter, radius_tolerance, lonlat_proj, aeqd_proj):
    from nexustiles.model.nexusmodel import NexusPoint
    from webservice.algorithms_spark.Matchup import DomsPoint  # Must import DomsPoint or Spark complains

    # Load tile
    try:
        the_time = datetime.now()
        tile = tile_service.mask_tiles_to_polygon(wkt.loads(search_domain_bounding_wkt),
                                                  tile_service.find_tile_by_id(tile_id))[0]
        print "%s Time to load tile %s" % (str(datetime.now() - the_time), tile_id)
    except IndexError:
        # This should only happen if all measurements in a tile become masked after applying the bounding polygon
        raise StopIteration

    # Convert valid tile lat,lon tuples to UTM tuples
    the_time = datetime.now()
    # Get list of indices of valid values
    valid_indices = tile.get_indices()
    primary_points = np.array(
        [pyproj.transform(p1=lonlat_proj, p2=aeqd_proj, x=tile.longitudes[aslice[2]], y=tile.latitudes[aslice[1]]) for
         aslice in valid_indices])

    print "%s Time to convert primary points for tile %s" % (str(datetime.now() - the_time), tile_id)

    a_time = datetime.now()
    p_tree = spatial.cKDTree(primary_points, leafsize=30)
    print "%s Time to build primary tree" % (str(datetime.now() - a_time))

    a_time = datetime.now()
    matched_indexes = p_tree.query_ball_tree(m_tree, radius_tolerance)
    print "%s Time to query primary tree for tile %s" % (str(datetime.now() - a_time), tile_id)
    for i, point_matches in enumerate(matched_indexes):
        if len(point_matches) > 0:
            p_nexus_point = NexusPoint(tile.latitudes[valid_indices[i][1]],
                                       tile.longitudes[valid_indices[i][2]], None,
                                       tile.times[valid_indices[i][0]], valid_indices[i],
                                       tile.data[tuple(valid_indices[i])])
            p_doms_point = DomsPoint.from_nexus_point(p_nexus_point, tile=tile, parameter=search_parameter)
            for m_point_index in point_matches:
                m_doms_point = DomsPoint.from_edge_point(edge_results[m_point_index])
                yield p_doms_point, m_doms_point


def query_edge(dataset, variable, startTime, endTime, bbox, platform, depth_min, depth_max, itemsPerPage=1000,
               startIndex=0, stats=True, session=None):
    try:
        startTime = datetime.utcfromtimestamp(startTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        endTime = datetime.utcfromtimestamp(endTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        platform = platform.split(',')
    except AttributeError:
        # Assume we were passed a list
        pass

    params = {"variable": variable,
              "startTime": startTime,
              "endTime": endTime,
              "bbox": bbox,
              "minDepth": depth_min,
              "maxDepth": depth_max,
              "platform": platform,
              "itemsPerPage": itemsPerPage, "startIndex": startIndex, "stats": str(stats).lower()}

    if session is not None:
        edge_request = session.get(edge_endpoints.getEndpointByName(dataset)['url'], params=params)
    else:
        edge_request = requests.get(edge_endpoints.getEndpointByName(dataset)['url'], params=params)

    edge_request.raise_for_status()
    edge_response = json.loads(edge_request.text)

    # Get all edge results
    next_page_url = edge_response.get('next', None)
    while next_page_url is not None:
        if session is not None:
            edge_page_request = session.get(next_page_url)
        else:
            edge_page_request = requests.get(next_page_url)

        edge_page_request.raise_for_status()
        edge_page_response = json.loads(edge_page_request.text)

        edge_response['results'].extend(edge_page_response['results'])

        next_page_url = edge_page_response.get('next', None)

    return edge_response
