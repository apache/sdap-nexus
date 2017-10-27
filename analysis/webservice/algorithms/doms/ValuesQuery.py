from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException
import BaseDomsHandler
import datafetch
from pytz import timezone, UTC
from datetime import datetime

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

@nexus_handler
class DomsValuesQueryHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS In-Situ Value Lookup"
    path = "/domsvalues"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):

        source = computeOptions.get_argument("source", None)
        startTime = computeOptions.get_start_datetime()
        endTime = computeOptions.get_end_datetime()
        bbox = computeOptions.get_argument("b", None)
        timeTolerance = computeOptions.get_float_arg("tt")
        depth_min = computeOptions.get_float_arg("depthMin", default=None)
        depth_max = computeOptions.get_float_arg("depthMax", default=None)
        radiusTolerance = computeOptions.get_float_arg("rt")
        platforms = computeOptions.get_argument("platforms", "")

        source1 = self.getDataSourceByName(source)
        if source1 is None:
            raise Exception("Source '%s' not found"%source)

        values, bounds = datafetch.getValues(source1, startTime.strftime('%Y-%m-%dT%H:%M:%SZ'), endTime.strftime('%Y-%m-%dT%H:%M:%SZ'), bbox, depth_min, depth_max, platforms, placeholders=True)
        count = len(values)

        args = {
            "source": source,
            "startTime": startTime,
            "endTime": endTime,
            "bbox": bbox,
            "timeTolerance": timeTolerance,
            "depthMin": depth_min,
            "depthMax": depth_max,
            "radiusTolerance": radiusTolerance,
            "platforms": platforms
        }

        return BaseDomsHandler.DomsQueryResults(results=values, args=args, bounds=bounds, details={}, count=count, computeOptions=None)