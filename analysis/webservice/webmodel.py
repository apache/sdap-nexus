# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import hashlib
import inspect
import json
import re
import time
from datetime import datetime
from decimal import Decimal

import numpy as np
from pytz import UTC, timezone
from shapely.geometry import Polygon

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


class RequestParameters(object):
    SEASONAL_CYCLE_FILTER = "seasonalFilter"
    MAX_LAT = "maxLat"
    MIN_LAT = "minLat"
    MAX_LON = "maxLon"
    MIN_LON = "minLon"
    DATASET = "ds"
    ENVIRONMENT = "env"
    OUTPUT = "output"
    START_TIME = "startTime"
    END_TIME = "endTime"
    START_YEAR = "startYear"
    END_YEAR = "endYear"
    CLIM_MONTH = "month"
    START_ROW = "start"
    ROW_COUNT = "numRows"
    APPLY_LOW_PASS = "lowPassFilter"
    LOW_CUT = "lowCut"
    ORDER = "lpOrder"
    PLOT_SERIES = "plotSeries"
    PLOT_TYPE = "plotType"
    SPARK_CFG = "spark"
    METADATA_FILTER = "metadataFilter"


class StandardNexusErrors:
    UNKNOWN = 1000
    NO_DATA = 1001
    DATASET_MISSING = 1002


class NexusProcessingException(Exception):
    def __init__(self, error=StandardNexusErrors.UNKNOWN, reason="", code=500):
        self.error = error
        self.reason = reason
        self.code = code
        Exception.__init__(self, reason)


class NoDataException(NexusProcessingException):
    def __init__(self, reason="No data found for the selected timeframe"):
        NexusProcessingException.__init__(self, StandardNexusErrors.NO_DATA, reason, 400)


class DatasetNotFoundException(NexusProcessingException):
    def __init__(self, reason="Dataset not found"):
        NexusProcessingException.__init__(self, StandardNexusErrors.DATASET_MISSING, reason, code=404)


class SparkConfig(object):
    MAX_NUM_EXECS = 64
    MAX_NUM_PARTS = 8192
    DEFAULT = "local,1,1"


class StatsComputeOptions(object):
    def __init__(self):
        pass

    def get_apply_seasonal_cycle_filter(self, default="false"):
        raise Exception("Please implement")

    def get_max_lat(self, default=90.0):
        raise Exception("Please implement")

    def get_min_lat(self, default=-90.0):
        raise Exception("Please implement")

    def get_max_lon(self, default=180):
        raise Exception("Please implement")

    def get_min_lon(self, default=-180):
        raise Exception("Please implement")

    def get_dataset(self):
        raise Exception("Please implement")

    def get_environment(self):
        raise Exception("Please implement")

    def get_start_time(self):
        raise Exception("Please implement")

    def get_end_time(self):
        raise Exception("Please implement")

    def get_start_year(self):
        raise Exception("Please implement")

    def get_end_year(self):
        raise Exception("Please implement")

    def get_clim_month(self):
        raise Exception("Please implement")

    def get_start_row(self):
        raise Exception("Please implement")

    def get_end_row(self):
        raise Exception("Please implement")

    def get_content_type(self):
        raise Exception("Please implement")

    def get_apply_low_pass_filter(self, default=False):
        raise Exception("Please implement")

    def get_low_pass_low_cut(self, default=12):
        raise Exception("Please implement")

    def get_low_pass_order(self, default=9):
        raise Exception("Please implement")

    def get_plot_series(self, default="mean"):
        raise Exception("Please implement")

    def get_plot_type(self, default="default"):
        raise Exception("Please implement")

    def get_spark_cfg(self, default=SparkConfig.DEFAULT):
        raise Exception("Please implement")


class NexusRequestObject(StatsComputeOptions):
    shortNamePattern = re.compile("^[a-zA-Z0-9_\-,\.]+$")
    floatingPointPattern = re.compile('[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?')

    def __init__(self, reqHandler):
        if reqHandler is None:
            raise Exception("Request handler cannot be null")
        self.requestHandler = reqHandler
        StatsComputeOptions.__init__(self)

    def get_argument(self, name, default=None):
        return self.requestHandler.get_argument(name, default=default)

    def get_list_int_arg(self, name, default=None):
        arg = self.get_argument(name, default=default)
        return arg.split(',')

    def __validate_is_shortname(self, v):
        if v is None or len(v) == 0:
            return False
        return self.shortNamePattern.match(v) is not None

    def __validate_is_number(self, v):
        if v is None or (type(v) == str and len(v) == 0):
            return False
        elif type(v) == int or type(v) == float:
            return True
        else:
            return self.floatingPointPattern.match(v) is not None

    def get_float_arg(self, name, default=0.0):
        arg = self.get_argument(name, default)
        if self.__validate_is_number(arg):
            return float(arg)
        else:
            return default

    def get_decimal_arg(self, name, default=0.0):
        arg = self.get_argument(name, default)
        if self.__validate_is_number(arg):
            return Decimal(arg)
        else:
            if default is None:
                return None
            return Decimal(default)

    def get_int_arg(self, name, default=0):
        arg = self.get_argument(name, default)
        if self.__validate_is_number(arg):
            return int(arg)
        else:
            return default

    def get_boolean_arg(self, name, default=False):
        arg = self.get_argument(name, "false" if not default else "true")
        return arg is not None and arg in ['true', '1', 't', 'y', 'yes', 'True', 'T', 'Y',
                                           'Yes', True]

    def get_datetime_arg(self, name, default=None):
        time_str = self.get_argument(name, default=default)
        if time_str == default:
            return default
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str)).replace(tzinfo=UTC)
        return dt

    def get_apply_seasonal_cycle_filter(self, default=True):
        return self.get_boolean_arg(RequestParameters.SEASONAL_CYCLE_FILTER, default=default)

    def get_max_lat(self, default=Decimal(90)):
        return self.get_decimal_arg("maxLat", default)

    def get_min_lat(self, default=Decimal(-90)):
        return self.get_decimal_arg("minLat", default)

    def get_max_lon(self, default=Decimal(180)):
        return self.get_decimal_arg("maxLon", default)

    def get_min_lon(self, default=Decimal(-180)):
        return self.get_decimal_arg("minLon", default)

    def get_bounding_polygon(self):
        west, south, east, north = [float(b) for b in self.get_argument("b").split(",")]
        polygon = Polygon([(west, south), (east, south), (east, north), (west, north), (west, south)])
        return polygon

    def get_dataset(self):
        ds = self.get_argument(RequestParameters.DATASET, None)
        if ds is not None and not self.__validate_is_shortname(ds):
            raise Exception("Invalid shortname")
        else:
            return ds.split(",")

    def get_metadata_filter(self):
        return self.requestHandler.get_arguments(RequestParameters.METADATA_FILTER)

    def get_environment(self):
        env = self.get_argument(RequestParameters.ENVIRONMENT, None)
        if env is None and "Origin" in self.requestHandler.request.headers:
            origin = self.requestHandler.request.headers["Origin"]
            if origin == "http://localhost:63342":
                env = "DEV"
            if origin == "https://sealevel.uat.earthdata.nasa.gov":
                env = "UAT"
            elif origin == "https://sealevel.sit.earthdata.nasa.gov":
                env = "SIT"
            elif origin == "https://sealevel.earthdata.nasa.gov":
                env = "PROD"

        if env not in ("DEV", "SIT", "UAT", "PROD", None):
            raise Exception("Invalid Environment")
        else:
            return env

    def get_start_time(self):
        return self.get_int_arg(RequestParameters.START_TIME, 0)

    def get_end_time(self):
        return self.get_int_arg(RequestParameters.END_TIME, -1)

    def get_start_year(self):
        return self.get_int_arg(RequestParameters.START_YEAR, 0)

    def get_end_year(self):
        return self.get_int_arg(RequestParameters.END_YEAR, -1)

    def get_clim_month(self):
        return self.get_int_arg(RequestParameters.CLIM_MONTH, -1)

    def get_start_datetime(self):
        time_str = self.get_argument(RequestParameters.START_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str)).replace(tzinfo=UTC)
        return dt

    def get_end_datetime(self):
        time_str = self.get_argument(RequestParameters.END_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str)).replace(tzinfo=UTC)
        return dt

    def get_start_datetime_ms(self):
        time_str = self.get_argument(RequestParameters.START_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str) / 1000).replace(tzinfo=UTC)
        return dt

    def get_end_datetime_ms(self):
        time_str = self.get_argument(RequestParameters.END_TIME)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str) / 1000).replace(tzinfo=UTC)
        return dt

    def get_start_row(self):
        return self.get_int_arg(RequestParameters.START_ROW, 0)

    def get_row_count(self):
        return self.get_int_arg(RequestParameters.ROW_COUNT, 10)

    def get_content_type(self, default_type="JSON"):
        return self.get_argument(RequestParameters.OUTPUT, default_type)

    def get_apply_low_pass_filter(self, default=True):
        return self.get_boolean_arg(RequestParameters.APPLY_LOW_PASS, default)

    def get_low_pass_low_cut(self, default=12):
        return self.get_float_arg(RequestParameters.LOW_CUT, default)

    def get_low_pass_order(self, default=9):
        return self.get_float_arg(RequestParameters.ORDER, default)

    def get_include_meta(self):
        return self.get_boolean_arg("includemeta", True)

    def get_plot_series(self, default="mean"):
        return self.get_argument(RequestParameters.PLOT_SERIES, default=default)

    def get_plot_type(self, default="default"):
        return self.get_argument(RequestParameters.PLOT_TYPE, default=default)

    def get_spark_cfg(self, default=SparkConfig.DEFAULT):
        arg = self.get_argument(RequestParameters.SPARK_CFG, default)
        try:
            master, nexecs, nparts = arg.split(',')
        except:
            raise ValueError('Invalid spark configuration: %s' % arg)
        if master not in ("local", "yarn", "mesos"):
            raise ValueError('Invalid spark master: %s' % master)
        nexecs = int(nexecs)
        if (nexecs < 1) or (nexecs > SparkConfig.MAX_NUM_EXECS):
            raise ValueError('Invalid number of Spark executors: %d (must be between 1 and %d)' % (
            nexecs, SparkConfig.MAX_NUM_EXECS))
        nparts = int(nparts)
        if (nparts < 1) or (nparts > SparkConfig.MAX_NUM_PARTS):
            raise ValueError('Invalid number of Spark data partitions: %d (must be between 1 and %d)' % (
            nparts, SparkConfig.MAX_NUM_PARTS))
        if master == "local":
            master = "local[%d]" % nexecs
        return master, nexecs, nparts


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
        meta["bounds"] = {
            "east": maxLon,
            "west": minLon,
            "north": maxLat,
            "south": minLat
        }
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


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        """If input object is an ndarray it will be converted into a dict
        holding dtype, shape and the data, base64 encoded.
        """
        numpy_types = (
            np.bool_,
            # np.bytes_, -- python `bytes` class is not json serializable
            # np.complex64,  -- python `complex` class is not json serializable
            # np.complex128,  -- python `complex` class is not json serializable
            # np.complex256,  -- python `complex` class is not json serializable
            # np.datetime64,  -- python `datetime.datetime` class is not json serializable
            np.float16,
            np.float32,
            np.float64,
            # np.float128,  -- special handling below
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            # np.object_  -- should already be evaluated as python native
            np.str_,
            np.uint8,
            np.uint16,
            np.uint32,
            np.uint64,
            np.void,
        )
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, numpy_types):
            return obj.item()
        elif isinstance(obj, np.float128):
            return obj.astype(np.float64).item()
        elif isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, datetime):
            return str(obj)
        elif obj is np.ma.masked:
            return str(np.NaN)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


__CACHE = {}


def cached(ttl=60000):
    def _hash_function_signature(func):
        hash_object = hashlib.md5(str(inspect.getargspec(func)) + str(func))
        return hash_object.hexdigest()

    def _now():
        return int(round(time.time() * 1000))

    def _expired(t):
        if t is None or _now() - t > ttl:
            return True
        else:
            return False

    def _cached_decorator(func):

        def func_wrapper(self, computeOptions, **args):
            hash = _hash_function_signature(func)
            force = computeOptions.get_boolean_arg("nocached", default=False)

            if force or hash not in __CACHE or (hash in __CACHE and _expired(__CACHE[hash]["time"])):
                result = func(self, computeOptions, **args)
                __CACHE[hash] = {
                    "time": _now(),
                    "result": result
                }

            return __CACHE[hash]["result"]

        return func_wrapper

    return _cached_decorator
