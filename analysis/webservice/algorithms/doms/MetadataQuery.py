from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException, DatasetNotFoundException
import BaseDomsHandler
import datafetch
import config
import requests
import json

@nexus_handler
class DomsMetadataQueryHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS Metadata Listing"
    path = "/domsmetadata"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):

        dataset = computeOptions.get_argument("dataset", None)
        if dataset is None or len(dataset) == 0:
            raise Exception("'dataset' parameter not specified")

        metadataUrl = self.__getUrlForDataset(dataset)

        try:
            r = requests.get(metadataUrl)
            results = json.loads(r.text)
            return BaseDomsHandler.DomsQueryResults(results=results)
        except:
            raise DatasetNotFoundException("Dataset '%s' not found")

    def __getUrlForDataset(self, dataset):
        datasetSpec = config.getEndpointByName(dataset)
        if datasetSpec is not None:
            return datasetSpec["metadataUrl"]
        else:

            # KMG: NOT a good hack
            if dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1" or dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1_CLIM":
                dataset = "MUR-JPL-L4-GLOB-v4.1"
            elif dataset == "SMAP_L2B_SSS":
                dataset = "JPL_SMAP-SSS_L2_EVAL-V2"

            return "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=%s&format=umm-json"%dataset