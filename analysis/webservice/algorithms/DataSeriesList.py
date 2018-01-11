

import json

from webservice.NexusHandler import NexusHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import cached


@nexus_handler
class DataSeriesListHandlerImpl(NexusHandler):
    name = "Dataset List"
    path = "/list"
    description = "Lists datasets currently available for analysis"
    params = {}

    def __init__(self):
        NexusHandler.__init__(self, skipCassandra=True)

    @cached(ttl=(60 * 60 * 1000))  # 1 hour cached
    def calc(self, computeOptions, **args):
        class SimpleResult(object):
            def __init__(self, result):
                self.result = result

            def toJson(self):
                return json.dumps(self.result)

        return SimpleResult(self._tile_service.get_dataseries_list())
