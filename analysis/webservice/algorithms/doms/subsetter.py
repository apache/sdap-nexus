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

import io
import logging
import os
import zipfile
from datetime import datetime

from pytz import timezone
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.doms.insitu import query_insitu
from webservice.webmodel import NexusProcessingException, NexusResults

from . import BaseDomsHandler

ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'
EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))


def is_blank(my_string):
    return not (my_string and my_string.strip() != '')


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = 'CDMS Subsetter'
    path = '/cdmssubset'
    description = 'Subset CDMS sources given the search domain'

    params = {
        'dataset': {
            'name': 'NEXUS Dataset',
            'type': 'string',
            'description': "The NEXUS dataset. Optional but at least one of 'dataset' or 'insitu' are required"
        },
        'insitu': {
            'name': 'In Situ sources',
            'type': 'comma-delimited string',
            'description': "The in situ source(s). Optional but at least one of 'dataset' or 'insitu' are required"
        },
        'parameter': {
            'name': 'Data Parameter',
            'type': 'string',
            'description': 'The insitu parameter of interest. Only required if insitu is present.'
        },
        'startTime': {
            'name': 'Start Time',
            'type': 'string',
            'description': 'Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required'
        },
        'endTime': {
            'name': 'End Time',
            'type': 'string',
            'description': 'Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required'
        },
        'b': {
            'name': 'Bounding box',
            'type': 'comma-delimited float',
            'description': 'Minimum (Western) Longitude, Minimum (Southern) Latitude, '
                           'Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required'
        },
        'depthMin': {
            'name': 'Minimum Depth',
            'type': 'float',
            'description': 'Minimum depth of measurements. Must be less than depthMax. Default 0. Optional'
        },
        'depthMax': {
            'name': 'Maximum Depth',
            'type': 'float',
            'description': 'Maximum depth of measurements. Must be greater than depthMin. Default 5. Optional'
        },
        'platforms': {
            'name': 'Platforms',
            'type': 'comma-delimited integer',
            'description': 'Platforms to include for subset consideration. Optional'
        },
        'output': {
            'name': 'Output',
            'type': 'string',
            'description': "Output type. Only 'ZIP' is currently supported. Required"
        }
    }
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self, tile_service_factory)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug('Parsing arguments')

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
        else:
            matchup_ds_names = []

        parameter_s = request.get_argument('parameter', None)

        try:
            start_time = request.get_start_datetime()
            start_time = int((start_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
            end_time = int((end_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        if start_time > end_time:
            raise NexusProcessingException(
                reason='The starting time must be before the ending time. Received startTime: %s, endTime: %s' % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(
                reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                code=400)

        depth_min = request.get_decimal_arg('depthMin', default=0)
        depth_max = request.get_decimal_arg('depthMax', default=5)

        if depth_min is not None and depth_max is not None and depth_min >= depth_max:
            raise NexusProcessingException(
                reason='Depth Min should be less than Depth Max', code=400)

        platforms = request.get_argument('platforms', None)

        return primary_ds_name, matchup_ds_names, parameter_s, start_time, end_time, \
               bounding_polygon, depth_min, depth_max, platforms

    def calc(self, request, **args):
        primary_ds_name, matchup_ds_names, parameter_s, start_time, end_time, \
        bounding_polygon, depth_min, depth_max, platforms = self.parse_arguments(request)

        min_lat = max_lat = min_lon = max_lon = None
        if bounding_polygon:
            min_lat = bounding_polygon.bounds[1]
            max_lat = bounding_polygon.bounds[3]
            min_lon = bounding_polygon.bounds[0]
            max_lon = bounding_polygon.bounds[2]

        self.log.info('Fetching tile ids in bounds')

        tile_service = self._get_tile_service()

        tiles = tile_service.find_tiles_in_box(
            min_lat=min_lat, max_lat=max_lat, min_lon=min_lon,
            max_lon=max_lon, ds=primary_ds_name, start_time=start_time,
            end_time=end_time, fetch_data=False
        )

        self.log.info(f'Fetched {len(tiles)} tile ids')
        self.log.info('Processing satellite tiles')

        # Satellite
        data = []
        data_dict = {}
        for i in range(len(tiles)-1, -1, -1):
            tile = tiles.pop(i)

            self.log.debug(f'Processing tile {tile.tile_id}')

            tile = tile_service.fetch_data_for_tiles(tile)[0]

            tile = tile_service.mask_tiles_to_bbox(min_lat, max_lat, min_lon, max_lon, [tile])
            tile = tile_service.mask_tiles_to_time_range(start_time, end_time, tile)

            if len(tile) == 0:
                self.log.debug(f'Skipping empty tile')
                continue

            tile = tile[0]

            def get_best_name(variable):
                if variable.standard_name:
                    return variable.standard_name
                elif variable.variable_name:
                    return variable.variable_name
                else:
                    raise NexusProcessingException(
                        reason=f'Variable in subsetted data does not have defined name',
                        code=500
                    )

            for nexus_point in tile.nexus_point_generator():
                if tile.is_multi:
                    data_points = {
                        get_best_name(tile.variables[idx]): nexus_point.data_vals[idx]
                        for idx in range(len(tile.variables))
                    }
                else:
                    data_points = {get_best_name(tile.variables[0]): nexus_point.data_vals}
                data.append({
                    'latitude': nexus_point.latitude,
                    'longitude': nexus_point.longitude,
                    'time': nexus_point.time,
                    'data': data_points
                })

        data_dict[primary_ds_name] = data

        self.log.info('Finished satellite subsetting')
        self.log.info(f'Processed tiles to {len(data)} points')

        # In-situ
        non_data_fields = [
            'meta',
            'platform',
            'job_id',
            'latitude',
            'longitude',
            'time',
        ]
        for insitu_dataset in matchup_ds_names:
            data = []
            edge_response = query_insitu(
                dataset=insitu_dataset,
                variable=parameter_s,
                start_time=start_time,
                end_time=end_time,
                bbox=','.join(list(map(str, [min_lon, min_lat, max_lon, max_lat]))),
                platform=platforms,
                depth_min=depth_min,
                depth_max=depth_max
            )
            for result in edge_response['results']:
                data_points = {
                    key: value for key, value in result.items()
                    if value is not None and key not in non_data_fields
                }
                data.append({
                    'latitude': result['latitude'],
                    'longitude': result['longitude'],
                    'id': result['meta'],
                    'time': (datetime.strptime(result['time'], '%Y-%m-%dT%H:%M:%SZ') - datetime.fromtimestamp(0)).total_seconds(),
                    'data': data_points
                })
            data_dict[insitu_dataset] = data

        self.log.info('Finished Insitu Subsetting')

        if len(tiles) > 0:
            meta = [tile.get_summary() for tile in tiles]
        else:
            meta = None

        self.log.info('Subsetting complete - creating result')

        result = SubsetResult(
            results=data_dict,
            meta=meta
        )

        result.extendMeta(min_lat, max_lat, min_lon, max_lon, '', start_time, end_time)

        return result


class SubsetResult(NexusResults):
    def toJson(self):
        raise NotImplementedError

    def toCsv(self):
        """
        Convert results to csv
        """
        dataset_results = self.results()
        csv_results = {}

        logging.info('Converting result to CSV')

        for dataset_name, results in dataset_results.items():
            rows = []

            headers = [
                'longitude',
                'latitude',
                'time'
            ]
            data_variables = list(set([keys for result in results for keys in result['data'].keys()]))
            data_variables.sort()

            if 'id' in list(set([keys for result in results for keys in result.keys()])):
                headers.append('id')

            headers.extend(data_variables)
            for i, result in enumerate(results):
                cols = []

                cols.append(result['longitude'])
                cols.append(result['latitude'])
                cols.append(datetime.utcfromtimestamp(result['time']).strftime('%Y-%m-%dT%H:%M:%SZ'))

                if 'id' in headers:
                    cols.append(result.get('id'))

                for var in data_variables:
                    cols.append(result['data'].get(var))
                if i == 0:
                    rows.append(','.join(headers))
                rows.append(','.join(map(str, cols)))

            csv_results[dataset_name] = '\r\n'.join(rows)

        logging.info('Finished converting result to CSV')
        return csv_results

    def toZip(self):
        """
        Convert csv results to zip. Each subsetted dataset becomes a csv
        inside the zip named as <dataset-short-name>.csv
        """
        csv_results = self.toCsv()

        logging.info('Writing zip output')
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
            for dataset_name, csv_contents in csv_results.items():
                zip_file.writestr(f'{dataset_name}.csv', csv_contents)

        logging.info('Done writing zip output')
        buffer.seek(0)
        return buffer.read()

    def cleanup(self):
        os.remove(self.zip_path)
