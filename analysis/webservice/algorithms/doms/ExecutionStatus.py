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

import uuid

from . import BaseDomsHandler
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.doms.ResultsStorage import ResultsRetrieval
from webservice.webmodel import NexusExecutionResults
from webservice.webmodel import NexusProcessingException


@nexus_handler
class ExecutionStatusHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = 'Execution Status Handler'
    path = '/job'
    description = ''
    params = {}
    singleton = True

    def __init__(self, tile_service_factory, config=None):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self, tile_service_factory)
        self.config = config

    def calc(self, request, **args):
        execution_id = request.get_argument('id', None)

        try:
            execution_id = uuid.UUID(execution_id)
        except ValueError:
            raise NexusProcessingException(reason='"id" argument must be a valid uuid', code=400)

        # Check if the job is done
        with ResultsRetrieval(self.config) as retrieval:
            try:
                execution_details = retrieval.retrieveExecution(execution_id)
                execution_params = retrieval.retrieveParams(execution_id)
            except ValueError:
                raise NexusProcessingException(
                    reason=f'Execution {execution_id} not found',
                    code=404
                )

        job_status = NexusExecutionResults.ExecutionStatus(execution_details['status'])
        host = f'https://{request.requestHandler.request.host}'

        return NexusExecutionResults.NexusExecutionResults(
            status=job_status,
            created=execution_details['timeStarted'],
            completed=execution_details['timeCompleted'],
            execution_id=execution_id,
            message=execution_details['message'],
            params=execution_params,
            host=host
        )
