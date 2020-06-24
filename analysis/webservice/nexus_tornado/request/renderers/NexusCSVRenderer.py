import sys
import traceback
from webservice.webmodel import NexusProcessingException


class NexusCSVRenderer(object):
    def __init__(self, nexus_request):
        self._request = nexus_request

    def render(self, tornado_handler, result):
        tornado_handler.set_header("Content-Type", "text/csv")
        tornado_handler.set_header("Content-Disposition", "filename=\"%s\"" % self._request.get_argument('filename', "download.csv"))
        try:
            self.write(result.toCSV())
        except:
            traceback.print_exc(file=sys.stdout)
            raise NexusProcessingException(reason="Unable to convert results to CSV.")