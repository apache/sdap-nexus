

import json

from webservice.NexusHandler import NexusHandler, nexus_handler


@nexus_handler
class HeartbeatHandlerImpl(NexusHandler):
    name = "Backend Services Status"
    path = "/heartbeat"
    description = "Returns health status of Nexus backend services"
    params = {}
    singleton = True

    def __init__(self):
        NexusHandler.__init__(self, skipCassandra=True)

    def calc(self, computeOptions, **args):
        solrOnline = self._tile_service.pingSolr()

        # Not sure how to best check cassandra cluster status so just return True for now
        cassOnline = True

        if solrOnline and cassOnline:
            status = {"online": True}
        else:
            status = {"online": False}

        class SimpleResult(object):
            def __init__(self, result):
                self.result = result

            def toJson(self):
                return json.dumps(self.result)

        return SimpleResult(status)
