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
import datetime

import BaseDomsHandler
import ResultsStorage
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusProcessingException

import json
#import matchup

@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryHandler):
    name = "DOMS Resultset Retrieval TEST"
    path = "/domsresultstest"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)

    def calc(self, computeOptions, **args):
        execution_id = computeOptions.get_argument("id", None)

        try:
            #execution_id = uuid.UUID(execution_id)
            print("execution is is: ", execution_id)
        except:
            raise NexusProcessingException(reason="'id' argument must be a valid uuid", code=400)

        simple_results = computeOptions.get_boolean_arg("simpleResults", default=False)

        #with ResultsStorage.ResultsRetrieval() as storage:
        #    params, stats, data = storage.retrieveResults(execution_id, trim_data=simple_results)

        with open('webservice/algorithms/doms/matchupAVHRR.json') as f:
            json_data = json.load(f)

        params = json_data["params"]
        stats = json_data["details"]
        data = json_data["data"]

        # Edit object to include things left out of json
        params["startTime"] = datetime.datetime.utcfromtimestamp(params["startTime"])
        params["endTime"] = datetime.datetime.utcfromtimestamp(params["endTime"])

        # stats["numInSituChecked"] = 0
        # stats["numGriddedChecked"] = 0

        return BaseDomsHandler.DomsQueryResults(results=data, args=params, details=stats, bounds=None, count=None,
                                                computeOptions=None, executionId=execution_id)