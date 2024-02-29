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

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.doms.ResultsStorage import ResultsRetrieval
from webservice.webmodel import NexusExecutionResults
from webservice.algorithms_spark.NexusCalcSparkTornadoHandler import NexusCalcSparkTornadoHandler
from datetime import datetime
from webservice.algorithms.doms.ResultsStorage import ResultsStorage
from webservice.webmodel.NexusExecutionResults import ExecutionStatus
from webservice.webmodel import NexusProcessingException


@nexus_handler
class ExecutionStatusHandler(NexusCalcSparkTornadoHandler):
    name = 'Execution Status Handler'
    path = '/job/cancel'
    description = ''
    params = {}
    singleton = True

    def __init__(self, algorithm_config=None, sc=None, tile_service_factory=None, config=None):
        NexusCalcSparkTornadoHandler.__init__(
            self,
            algorithm_config=algorithm_config,
            sc=sc,
            tile_service_factory=tile_service_factory
        )
        self.tile_service_factory = tile_service_factory
        self.config = config

    def calc(self, request, tornado_io_loop, **args):
        execution_id = request.get_argument('id', None)

        try:
            execution_id = uuid.UUID(execution_id)
        except ValueError:
            raise NexusProcessingException(reason='"id" argument must be a valid uuid', code=400)

        with ResultsRetrieval(self.config) as retrieval:
            try:
                execution_details = retrieval.retrieveExecution(execution_id)
            except ValueError:
                raise NexusProcessingException(
                    reason=f'Execution {execution_id} not found',
                    code=404
                )

        job_status = NexusExecutionResults.ExecutionStatus(execution_details['status'])

        # Only proceed if status is "running". Otherwise, noop
        if job_status == ExecutionStatus.RUNNING:
            # Update job status to "cancelled"
            end = datetime.utcnow()
            with ResultsStorage(self.config) as storage:
                storage.updateExecution(
                    execution_id,
                    completeTime=end,
                    status=ExecutionStatus.CANCELLED.value,
                    message=None,
                    stats=None,
                    results=None
                )

            # Cancel Spark job
            self._sc.cancelJobGroup(str(execution_id))

        # Redirect to job status endpoint
        request.requestHandler.redirect(f'/job?id={execution_id}')
