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

import BaseDomsHandler
import ResultsStorage
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusProcessingException


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryCalcHandler):
    name = "DOMS Resultset Retrieval"
    path = "/domsresults"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryCalcHandler.__init__(self)

    def calc(self, computeOptions, **args):
        execution_id = computeOptions.get_argument("id", None)

        try:
            execution_id = uuid.UUID(execution_id)
        except:
            raise NexusProcessingException(reason="'id' argument must be a valid uuid", code=400)

        simple_results = computeOptions.get_boolean_arg("simpleResults", default=False)

        with ResultsStorage.ResultsRetrieval() as storage:
            params, stats, data = storage.retrieveResults(execution_id, trim_data=simple_results)

        return BaseDomsHandler.DomsQueryResults(results=data, args=params, details=stats, bounds=None, count=None,
                                                computeOptions=None, executionId=execution_id)
