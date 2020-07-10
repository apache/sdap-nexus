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

import StringIO
import csv
import json
import logging
from datetime import datetime

import requests

import BaseDomsHandler
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.doms import config as edge_endpoints
from webservice.webmodel import NexusProcessingException, NoDataException

ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "DOMS In Situ Subsetter"
    path = "/domsinsitusubset"
    description = "Subset a DOMS in situ source given the search domain."

    params = [
        {
            "name": "source",
            "type": "comma-delimited string",
            "description": "The in situ Dataset to be sub-setted",
            "required": "true",
            "sample": "spurs"
        },
        {
            "name": "parameter",
            "type": "string",
            "description": "The parameter of interest. One of 'sst', 'sss', 'wind'",
            "required": "false",
            "default": "All",
            "sample": "sss"
        },
        {
            "name": "startTime",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH",
            "required": "true",
            "sample": "2013-10-21T00:00:00Z"
        },
        {
            "name": "endTime",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH",
            "required": "true",
            "sample": "2013-10-31T23:59:59Z"
        },
        {
            "name": "b",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
            "required": "true",
            "sample": "-30,15,-45,30"
        },
        {
            "name": "depthMin",
            "type": "float",
            "description": "Minimum depth of measurements. Must be less than depthMax",
            "required": "false",
            "default": "No limit",
            "sample": "0"
        },
        {
            "name": "depthMax",
            "type": "float",
            "description": "Maximum depth of measurements. Must be greater than depthMin",
            "required": "false",
            "default": "No limit",
            "sample": "5"
        },
        {
            "name": "platforms",
            "type": "comma-delimited integer",
            "description": "Platforms to include for subset consideration",
            "required": "false",
            "default": "All",
            "sample": "1,2,3,4,5,6,7,8,9"
        },
        {
            "name": "output",
            "type": "string",
            "description": "Output type. Only 'CSV' or 'JSON' is currently supported",
            "required": "false",
            "default": "JSON",
            "sample": "CSV"
        }
    ]
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")

        source_name = request.get_argument('source', None)
        if source_name is None or source_name.strip() == '':
            raise NexusProcessingException(reason="'source' argument is required", code=400)

        parameter_s = request.get_argument('parameter', None)
        if parameter_s not in ['sst', 'sss', 'wind', None]:
            raise NexusProcessingException(
                reason="Parameter %s not supported. Must be one of 'sst', 'sss', 'wind'." % parameter_s, code=400)

        try:
            start_time = request.get_start_datetime()
            start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
            end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        if start_time > end_time:
            raise NexusProcessingException(
                reason="The starting time must be before the ending time. Received startTime: %s, endTime: %s" % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(
                reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                code=400)

        depth_min = request.get_decimal_arg('depthMin', default=None)
        depth_max = request.get_decimal_arg('depthMax', default=None)

        if depth_min is not None and depth_max is not None and depth_min >= depth_max:
            raise NexusProcessingException(
                reason="Depth Min should be less than Depth Max", code=400)

        platforms = request.get_argument('platforms', None)
        if platforms is not None:
            try:
                p_validation = platforms.split(',')
                p_validation = [int(p) for p in p_validation]
                del p_validation
            except:
                raise NexusProcessingException(reason="platforms must be a comma-delimited list of integers", code=400)

        return source_name, parameter_s, start_time, end_time, bounding_polygon, depth_min, depth_max, platforms

    def calc(self, request, **args):

        source_name, parameter_s, start_time, end_time, bounding_polygon, \
        depth_min, depth_max, platforms = self.parse_arguments(request)

        with requests.session() as edge_session:
            edge_results = query_edge(source_name, parameter_s, start_time, end_time,
                                      ','.join([str(bound) for bound in bounding_polygon.bounds]),
                                      platforms, depth_min, depth_max, edge_session)['results']

        if len(edge_results) == 0:
            raise NoDataException
        return InSituSubsetResult(results=edge_results)


class InSituSubsetResult(object):
    def __init__(self, results):
        self.results = results

    def toJson(self):
        return json.dumps(self.results, indent=4)

    def toCSV(self):
        fieldnames = sorted(next(iter(self.results)).keys())

        csv_mem_file = StringIO.StringIO()
        try:
            writer = csv.DictWriter(csv_mem_file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(self.results)
            csv_out = csv_mem_file.getvalue()
        finally:
            csv_mem_file.close()

        return csv_out


def query_edge(dataset, variable, startTime, endTime, bbox, platform, depth_min, depth_max, session, itemsPerPage=1000,
               startIndex=0, stats=True):
    log = logging.getLogger('webservice.algorithms.doms.insitusubset.query_edge')
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

    params = {"startTime": startTime,
              "endTime": endTime,
              "bbox": bbox,
              "minDepth": depth_min,
              "maxDepth": depth_max,
              "itemsPerPage": itemsPerPage, "startIndex": startIndex, "stats": str(stats).lower()}

    if variable:
        params['variable'] = variable
    if platform:
        params['platform'] = platform

    edge_request = session.get(edge_endpoints.getEndpointByName(dataset)['url'], params=params)

    edge_request.raise_for_status()
    edge_response = json.loads(edge_request.text)

    # Get all edge results
    next_page_url = edge_response.get('next', None)
    while next_page_url is not None:
        log.debug("requesting %s" % next_page_url)
        edge_page_request = session.get(next_page_url)

        edge_page_request.raise_for_status()
        edge_page_response = json.loads(edge_page_request.text)

        edge_response['results'].extend(edge_page_response['results'])

        next_page_url = edge_page_response.get('next', None)

    return edge_response
