# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import re
import uuid
from typing import List

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.doms.ResultsStorage import ResultsRetrieval
from webservice.webmodel import NexusProcessingException
from webservice.webmodel import NexusResults

from . import BaseDomsHandler


class StacResults(NexusResults):
    def __init__(self, contents):
        NexusResults.__init__(self)
        self.contents = contents

    def toJson(self):
        return json.dumps(self.contents, indent=4)


@nexus_handler
class StacCatalog(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = 'STAC Catalog Handler'
    path = '^/cdmscatalog/?.*$'
    description = ''
    params = {}
    singleton = True

    def __init__(self, tile_service_factory, config=None):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self, tile_service_factory)
        self.config = config

    def construct_catalog(self, execution_id: str):
        return {
            'stac_version': '1.0.0',
            'type': 'Catalog',
            'id': str(execution_id),
            'description': 'STAC Catalog for CDMS output',
            'links': [
                {
                    'rel': 'collection',
                    'href': f'https://{self.host}/cdmscatalog/{execution_id}/{output_format}',
                    'title': f'Collection of pages for {execution_id} {output_format} output'
                }
                for output_format in ['CSV', 'JSON', 'NETCDF']
            ]
        }

    def construct_collection(self, execution_id: str, output_format: str,
                             num_primary_matched: int, page_size: int, start_time: str,
                             end_time: str, bbox: List[float]):
        links = [
            {
                'rel': 'self',
                'href': f'https://{self.host}/cdmscatalog/{execution_id}/{output_format}',
                'title': 'The current page',
                'type': 'application/json'
            },
            {
                'rel': 'root',
                'href': f'https://{self.host}/cdmscatalog/{execution_id}',
                'title': f'Root catalog for {execution_id}',
            }
        ]

        url = f'https://{self.host}/cdmsresults?id={execution_id}&output={output_format}'
        for page_num in range(1, num_primary_matched, page_size):
            links.append({
                'rel': 'data',
                'href': f'{url}&pageNum={page_num}&pageSize={page_size}'
            })

        return {
            'stac_version': '1.0.0',
            'type': 'Collection',
            'license': 'not-provided',
            'id': f'{execution_id}.{output_format}',
            'description': 'Collection of results for CDMS execution and result format',
            'extent': {
                'spatial': {
                    'bbox': bbox
                },
                'temporal': {
                    'interval': [start_time, end_time]
                }
            },
            'links': links,
        }

    def calc(self, request, **args):
        page_size = request.get_int_arg('pageSize', default=1000)
        url_path_regex = '^\/cdmscatalog\/?(?P<id>[a-zA-Z0-9-]*)\/?(?P<format>[a-zA-Z0-9]*)'
        match = re.search(url_path_regex, request.requestHandler.request.path)

        execution_id = match.group('id')
        output_format = match.group('format')

        self.host = request.requestHandler.request.host

        if not execution_id:
            raise NexusProcessingException(
                reason=f'Execution ID path param must be provided.',
                code=400
            )

        if execution_id:
            try:
                execution_id = uuid.UUID(execution_id)
            except ValueError:
                raise NexusProcessingException(
                    reason=f'"{execution_id}" is not a valid uuid',
                    code=400
                )

        if output_format and output_format.upper() not in ['CSV', 'JSON', 'NETCDF']:
            raise NexusProcessingException(
                reason=f'"{output_format}" is not a valid format. Should be CSV, JSON, or NETCDF.',
                code=400
            )

        if execution_id and not output_format:
            # Route to STAC catalog for execution
            stac_output = self.construct_catalog(execution_id)
        elif execution_id and output_format:
            # Route to STAC collection for execution+format

            with ResultsRetrieval(self.config) as retrieval:
                try:
                    execution_stats = retrieval.retrieveStats(execution_id)
                    execution_params = retrieval.retrieveParams(execution_id)
                except NexusProcessingException:
                    execution_stats = {}

            num_primary_matched = execution_stats.get('numPrimaryMatched', 0)
            start_time = execution_params['startTime'].isoformat()
            end_time = execution_params['endTime'].isoformat()
            bbox = list(map(float, execution_params['bbox'].split(',')))

            stac_output = self.construct_collection(
                execution_id, output_format, num_primary_matched, page_size,
                start_time, end_time, bbox
            )
        else:
            raise NexusProcessingException(
                reason=f'Invalid path parameters were provided',
                code=400
            )

        return StacResults(stac_output)
