
import BaseDomsHandler
import histogramplot
import mapplot
import scatterplot
from webservice.NexusHandler import nexus_handler


class PlotTypes:
    SCATTER = "scatter"
    MAP = "map"
    HISTOGRAM = "histogram"


@nexus_handler
class DomsResultsPlotHandler(BaseDomsHandler.BaseDomsQueryHandler):
    name = "DOMS Results Plotting"
    path = "/domsplot"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)

    def calc(self, computeOptions, **args):
        id = computeOptions.get_argument("id", None)
        parameter = computeOptions.get_argument('parameter', 'sst')

        plotType = computeOptions.get_argument("type", PlotTypes.SCATTER)

        normAndCurve = computeOptions.get_boolean_arg("normandcurve", False)

        if plotType == PlotTypes.SCATTER:
            return scatterplot.createScatterPlot(id, parameter)
        elif plotType == PlotTypes.MAP:
            return mapplot.createMapPlot(id, parameter)
        elif plotType == PlotTypes.HISTOGRAM:
            return histogramplot.createHistogramPlot(id, parameter, normAndCurve)
        else:
            raise Exception("Unsupported plot type '%s' specified." % plotType)
