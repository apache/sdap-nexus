import sys
import traceback
from webservice.webmodel import NexusProcessingException


class NexusNETCDFRenderer(object):
    def __init__(self, nexus_request):
        self._request = nexus_request

    def render(self, tornado_handler, result):
        tornado_handler.set_header("Content-Type", "application/x-netcdf")
        tornado_handler.set_header("Content-Disposition", "filename=\"%s\"" % self._request.get_argument('filename', "download.nc"))
        try:
            self.write(result.toNetCDF())
        except:
            traceback.print_exc(file=sys.stdout)
            raise NexusProcessingException(reason="Unable to convert results to NetCDF.")