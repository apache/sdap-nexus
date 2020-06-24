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
import os
import tempfile
import zipfile
from datetime import datetime

import requests

import BaseDomsHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusProcessingException

ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


def is_blank(my_string):
    return not (my_string and my_string.strip() != '')


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "DOMS Subsetter"
    path = "/domssubset"
    description = "Subset DOMS sources given the search domain"

    params = {
        "dataset": {
            "name": "NEXUS Dataset",
            "type": "string",
            "description": "The NEXUS dataset. Optional but at least one of 'dataset' or 'insitu' are required"
        },
        "insitu": {
            "name": "In Situ sources",
            "type": "comma-delimited string",
            "description": "The in situ source(s). Optional but at least one of 'dataset' or 'insitu' are required"
        },
        "parameter": {
            "name": "Data Parameter",
            "type": "string",
            "description": "The parameter of interest. One of 'sst', 'sss', 'wind'. Required"
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
            "description": "Minimum depth of measurements. Must be less than depthMax. Optional"
        },
        "depthMax": {
            "name": "Maximum Depth",
            "type": "float",
            "description": "Maximum depth of measurements. Must be greater than depthMin. Optional"
        },
        "platforms": {
            "name": "Platforms",
            "type": "comma-delimited integer",
            "description": "Platforms to include for subset consideration. Optional"
        },
        "output": {
            "name": "Output",
            "type": "string",
            "description": "Output type. Only 'ZIP' is currently supported. Required"
        }
    }
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")

        primary_ds_name = request.get_argument('dataset', None)
        matchup_ds_names = request.get_argument('insitu', None)

        if is_blank(primary_ds_name) and is_blank(matchup_ds_names):
            raise NexusProcessingException(reason="Either 'dataset', 'insitu', or both arguments are required",
                                           code=400)

        if matchup_ds_names is not None:
            try:
                matchup_ds_names = matchup_ds_names.split(',')
            except:
                raise NexusProcessingException(reason="'insitu' argument should be a comma-seperated list", code=400)

        parameter_s = request.get_argument('parameter', None)
        if parameter_s not in ['sst', 'sss', 'wind']:
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

        return primary_ds_name, matchup_ds_names, parameter_s, start_time, end_time, \
               bounding_polygon, depth_min, depth_max, platforms

    def calc(self, request, **args):

        primary_ds_name, matchup_ds_names, parameter_s, start_time, end_time, \
        bounding_polygon, depth_min, depth_max, platforms = self.parse_arguments(request)

        primary_url = "https://doms.jpl.nasa.gov/datainbounds"
        primary_params = {
            'ds': primary_ds_name,
            'parameter': parameter_s,
            'b': ','.join([str(bound) for bound in bounding_polygon.bounds]),
            'startTime': start_time,
            'endTime': end_time,
            'output': "CSV"
        }

        matchup_url = "https://doms.jpl.nasa.gov/domsinsitusubset"
        matchup_params = {
            'source': None,
            'parameter': parameter_s,
            'startTime': start_time,
            'endTime': end_time,
            'b': ','.join([str(bound) for bound in bounding_polygon.bounds]),
            'depthMin': depth_min,
            'depthMax': depth_max,
            'platforms': platforms,
            'output': 'CSV'
        }

        primary_temp_file_path = None
        matchup_downloads = None

        with requests.session() as session:

            if not is_blank(primary_ds_name):
                # Download primary
                primary_temp_file, primary_temp_file_path = tempfile.mkstemp(suffix='.csv')
                download_file(primary_url, primary_temp_file_path, session, params=primary_params)

            if len(matchup_ds_names) > 0:
                # Download matchup
                matchup_downloads = {}
                for matchup_ds in matchup_ds_names:
                    matchup_downloads[matchup_ds] = tempfile.mkstemp(suffix='.csv')
                    matchup_params['source'] = matchup_ds
                    download_file(matchup_url, matchup_downloads[matchup_ds][1], session, params=matchup_params)

        # Zip downloads
        date_range = "%s-%s" % (datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d"),
                                datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d"))
        bounds = '%.4fW_%.4fS_%.4fE_%.4fN' % bounding_polygon.bounds
        zip_dir = tempfile.mkdtemp()
        zip_path = '%s/subset.%s.%s.zip' % (zip_dir, date_range, bounds)
        with zipfile.ZipFile(zip_path, 'w') as my_zip:
            if primary_temp_file_path:
                my_zip.write(primary_temp_file_path, arcname='%s.%s.%s.csv' % (primary_ds_name, date_range, bounds))
            if matchup_downloads:
                for matchup_ds, download in matchup_downloads.iteritems():
                    my_zip.write(download[1], arcname='%s.%s.%s.csv' % (matchup_ds, date_range, bounds))

        # Clean up
        if primary_temp_file_path:
            os.remove(primary_temp_file_path)
        if matchup_downloads:
            for matchup_ds, download in matchup_downloads.iteritems():
                os.remove(download[1])

        return SubsetResult(zip_path)


class SubsetResult(object):
    def __init__(self, zip_path):
        self.zip_path = zip_path

    def toJson(self):
        raise NotImplementedError

    def toZip(self):
        with open(self.zip_path, 'rb') as zip_file:
            zip_contents = zip_file.read()

        return zip_contents

    def cleanup(self):
        os.remove(self.zip_path)


def download_file(url, filepath, session, params=None):
    r = session.get(url, params=params, stream=True)
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
