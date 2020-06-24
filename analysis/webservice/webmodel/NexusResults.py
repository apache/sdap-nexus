import json
from datetime import datetime

from pytz import timezone
from webservice.webmodel.CustomEncoder import CustomEncoder

ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

class NexusResults:
    def __init__(self, results=None, meta=None, stats=None, computeOptions=None, status_code=200, **args):
        self.status_code = status_code
        self.__results = results
        self.__meta = meta if meta is not None else {}
        self.__stats = stats if stats is not None else {}
        self.__computeOptions = computeOptions
        if computeOptions is not None:
            self.__minLat = computeOptions.get_min_lat()
            self.__maxLat = computeOptions.get_max_lat()
            self.__minLon = computeOptions.get_min_lon()
            self.__maxLon = computeOptions.get_max_lon()
            self.__ds = computeOptions.get_dataset()
            self.__startTime = computeOptions.get_start_time()
            self.__endTime = computeOptions.get_end_time()
        else:
            self.__minLat = args["minLat"] if "minLat" in args else -90.0
            self.__maxLat = args["maxLat"] if "maxLat" in args else 90.0
            self.__minLon = args["minLon"] if "minLon" in args else -180.0
            self.__maxLon = args["maxLon"] if "maxLon" in args else 180.0
            self.__ds = args["ds"] if "ds" in args else None
            self.__startTime = args["startTime"] if "startTime" in args else None
            self.__endTime = args["endTime"] if "endTime" in args else None

        self.extendMeta(minLat=self.__minLat,
                        maxLat=self.__maxLat,
                        minLon=self.__minLon,
                        maxLon=self.__maxLon,
                        ds=self.__ds,
                        startTime=self.__startTime,
                        endTime=self.__endTime)

    def computeOptions(self):
        return self.__computeOptions

    def results(self):
        return self.__results

    def meta(self):
        return self.__meta

    def stats(self):
        return self.__stats

    def _extendMeta(self, meta, minLat, maxLat, minLon, maxLon, ds, startTime, endTime):
        if meta is None:
            return None

        meta["shortName"] = ds
        if "title" in meta and "units" in meta:
            meta["label"] = "%s (%s)" % (meta["title"], meta["units"])
        if all(p is not None for p in [minLat, maxLat, minLon, maxLon]):
            meta["bounds"] = {
                "east": maxLon,
                "west": minLon,
                "north": maxLat,
                "south": minLat
            }
        if startTime is not None and endTime is not None:
            meta["time"] = {
                "start": startTime,
                "stop": endTime,
                "iso_start": datetime.utcfromtimestamp(int(startTime)).replace(tzinfo=timezone('UTC')).strftime(ISO_8601),
                "iso_stop": datetime.utcfromtimestamp(int(endTime)).replace(tzinfo=timezone('UTC')).strftime(ISO_8601)
            }
        return meta

    def extendMeta(self, minLat, maxLat, minLon, maxLon, ds, startTime, endTime):
        if self.__meta is None:
            return None
        if type(ds) == list:
            for i in range(0, len(ds)):
                shortName = ds[i]

                if type(self.__meta) == list:
                    subMeta = self.__meta[i]
                else:
                    subMeta = self.__meta  # Risky
                self._extendMeta(subMeta, minLat, maxLat, minLon, maxLon, shortName, startTime, endTime)
        else:
            if type(self.__meta) == list:
                self.__meta = self.__meta[0]
            else:
                self.__meta = self.__meta  # Risky
            self._extendMeta(self.__meta, minLat, maxLat, minLon, maxLon, ds, startTime, endTime)

    def toJson(self):
        data = {
            'meta': self.__meta,
            'data': self.__results,
            'stats': self.__stats
        }
        return json.dumps(data, indent=4, cls=CustomEncoder)

    def toImage(self):
        raise Exception("Not implemented for this result type")