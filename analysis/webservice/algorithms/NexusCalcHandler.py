import time
import types

from nexustiles.nexustiles import NexusTileService


class NexusCalcHandler(object):
    @classmethod
    def validate(cls):
        if "calc" not in cls.__dict__ or not type(cls.__dict__["calc"]) == types.FunctionType:
            raise Exception("Method 'calc' has not been declared")

        if "path" not in cls.__dict__:
            raise Exception("Property 'path' has not been defined")

        if "name" not in cls.__dict__:
            raise Exception("Property 'name' has not been defined")

        if "description" not in cls.__dict__:
            raise Exception("Property 'description' has not been defined")

        if "params" not in cls.__dict__:
            raise Exception("Property 'params' has not been defined")

    def __init__(self, algorithm_config=None, skipCassandra=False, skipSolr=False):
        self.algorithm_config = algorithm_config
        self._skipCassandra = skipCassandra
        self._skipSolr = skipSolr
        self._tile_service = NexusTileService(skipDatastore=self._skipCassandra,
                                              skipMetadatastore=self._skipSolr,
                                              config=self.algorithm_config)

    def _get_tile_service(self):
        return self._tile_service

    def calc(self, computeOptions, **args):
        raise Exception("calc() not yet implemented")

    def _mergeDicts(self, x, y):
        z = x.copy()
        z.update(y)
        return z

    def _now(self):
        millis = int(round(time.time() * 1000))
        return millis

    def _mergeDataSeries(self, resultsData, dataNum, resultsMap):

        for entry in resultsData:

            # frmtdTime = datetime.fromtimestamp(entry["time"] ).strftime("%Y-%m")
            frmtdTime = entry["time"]

            if not frmtdTime in resultsMap:
                resultsMap[frmtdTime] = []
            entry["ds"] = dataNum
            resultsMap[frmtdTime].append(entry)

    def _resultsMapToList(self, resultsMap):
        resultsList = []
        for key, value in resultsMap.iteritems():
            resultsList.append(value)

        resultsList = sorted(resultsList, key=lambda entry: entry[0]["time"])
        return resultsList

    def _mergeResults(self, resultsRaw):
        resultsMap = {}

        for i in range(0, len(resultsRaw)):
            resultsSeries = resultsRaw[i]
            resultsData = resultsSeries[0]
            self._mergeDataSeries(resultsData, i, resultsMap)

        resultsList = self._resultsMapToList(resultsMap)
        return resultsList
