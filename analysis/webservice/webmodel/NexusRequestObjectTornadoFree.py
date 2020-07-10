import logging
import re
from datetime import datetime
from decimal import Decimal

from pytz import UTC
from webservice.webmodel.RequestParameters import RequestParameters
from webservice.webmodel.StatsComputeOptions import StatsComputeOptions


class NexusRequestObjectTornadoFree(StatsComputeOptions):
    shortNamePattern = re.compile("^[a-zA-Z0-9_\-,\.]+$")
    floatingPointPattern = re.compile('[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?')

    def __init__(self, request_handler):
        self.__log = logging.getLogger(__name__)
        if request_handler is None:
            raise Exception("Request handler cannot be null")
        StatsComputeOptions.__init__(self)

        self._dataset = self._parse_dataset(request_handler)

        self._bounding_box = self._parse_bounding_box(request_handler)

        self._start_time = self._parse_start_time(request_handler)
        self._end_time = self._parse_end_time(request_handler)

        self._nparts = self._parse_nparts(request_handler)

        self._content_type = self._parse_content_type(request_handler)

    def get_dataset(self):
        return self._dataset

    def get_bounding_box(self):
        return self._bounding_box

    def get_start_datetime(self):
        return self._start_time

    def get_end_datetime(self):
        return self._end_time

    def get_nparts(self):
        return self._nparts

    def get_content_type(self):
        return self._content_type

    def _parse_dataset(self, request_handler):
        ds = request_handler.get_argument(RequestParameters.DATASET, None)
        if ds is not None and not self.__validate_is_shortname(ds):
            raise Exception("Invalid shortname")

        return ds

    def _parse_bounding_box(self, request_handler):

        b = request_handler.get_argument("b", '')
        if b:
            min_lon, min_lat, max_lon, max_lat = [float(e) for e in b.split(",")]
        else:
            max_lat = request_handler.get_argument("maxLat", 90)
            max_lat = Decimal(max_lat) if self.__validate_is_number(max_lat) else 90

            min_lat = request_handler.get_argument("minLat", -90)
            min_lat = Decimal(min_lat) if self.__validate_is_number(min_lat) else -90

            max_lon = request_handler.get_argument("maxLon", 180)
            max_lon = Decimal(max_lon) if self.__validate_is_number(max_lon) else 180

            min_lon = request_handler.get_argument("minLon", -90)
            min_lon = Decimal(min_lon) if self.__validate_is_number(min_lon) else -90

        return min_lon, min_lat, max_lon, max_lat

    def _parse_start_time(self, request_handler):
        return self._parse_time(request_handler, RequestParameters.START_TIME, default=0)

    def _parse_end_time(self, request_handler):
        return self._parse_time(request_handler, RequestParameters.END_TIME, default=-1)

    def _parse_time(self, request_handler, arg_name, default=None):
        time_str = request_handler.get_argument(arg_name, default)
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            dt = datetime.utcfromtimestamp(int(time_str)).replace(tzinfo=UTC)
        return dt

    def _parse_nparts(self, request_handler):
        return int(request_handler.get_argument(RequestParameters.NPARTS, 0))

    def _parse_content_type(self, request_handler):
        return request_handler.get_argument(RequestParameters.OUTPUT, "JSON")

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