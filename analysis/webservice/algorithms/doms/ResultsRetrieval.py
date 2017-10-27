import uuid

import BaseDomsHandler
import ResultsStorage
from webservice.webmodel import NexusProcessingException
from webservice.NexusHandler import nexus_handler


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryHandler):
    name = "DOMS Resultset Retrieval"
    path = "/domsresults"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)

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
