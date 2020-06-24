import sys
import traceback
from webservice.webmodel import NexusProcessingException


class NexusPNGRenderer(object):
    def __init__(self, nexus_request):
        self._request = nexus_request

    def render(self, tornado_handler, result):
        tornado_handler.set_header("Content-Type", "image/png")
        try:
            tornado_handler.write(result.toImage())
            tornado_handler.finish()
        except AttributeError:
            traceback.print_exc(file=sys.stdout)
            raise NexusProcessingException(reason="Unable to convert results to an Image.")