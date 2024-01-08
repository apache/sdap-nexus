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

import uuid

from . import BaseDomsHandler
from . import ResultsStorage
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusProcessingException
from nexustiles.nexustiles import NexusTileService

import logging

log = logging.getLogger(__name__)


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "DOMS Resultset Retrieval"
    path = "/cdmsresults"
    description = ""
    params = {}
    singleton = True

    def __init__(self, tile_service_factory, config=None):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self, tile_service_factory)
        self.config = config

    def calc(self, computeOptions, **args):
        execution_id = computeOptions.get_argument("id", None)
        page_num = computeOptions.get_int_arg('pageNum', default=1)
        page_size = computeOptions.get_int_arg('pageSize', default=1000)

        try:
            execution_id = uuid.UUID(execution_id)
        except:
            raise NexusProcessingException(reason="'id' argument must be a valid uuid", code=400)

        simple_results = computeOptions.get_boolean_arg("simpleResults", default=False)

        with ResultsStorage.ResultsRetrieval(self.config) as storage:
            params, stats, data = storage.retrieveResults(execution_id, trim_data=simple_results, page_num=page_num, page_size=page_size)

        try:
            ds_metadata = {}
            ds_meta_primary_name = params['primary']

            primary_metadata = NexusTileService.get_metadata_for_dataset(ds_meta_primary_name)

            ds_metadata['primary'] = {ds_meta_primary_name: primary_metadata}

            ds_metadata['secondary'] = {}

            for secondary_ds_name in params['matchup'].split(','):
                ds_metadata['secondary'][secondary_ds_name] = NexusTileService.get_metadata_for_dataset(secondary_ds_name)
        except:
            log.warning('Could not build dataset metadata dict due to an error')
            ds_metadata = {}

        return BaseDomsHandler.DomsQueryResults(results=data, args=params, details=stats, bounds=None, count=len(data),
                                                computeOptions=None, executionId=execution_id, page_num=page_num,
                                                page_size=page_size, meta=dict(datasets=ds_metadata))
