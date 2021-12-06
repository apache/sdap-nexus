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
import logging
import sys
from datetime import datetime
import math
import numpy as np
from pytz import timezone

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger(__name__)

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


@nexus_handler
class DataInBoundsSearchCalcMBHandlerImpl(NexusCalcHandler):
    name = "Data In-Bounds Search"
    path = "/datainboundsmb"
    description = "Fetches point values for a given dataset and geographical area"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation. Required"
        },
        "parameter": {
            "name": "Parameter",
            "type": "string",
            "description": "The parameter of interest. One of 'sst', 'sss', 'wind'. Required"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required if 'metadataFilter' not provided"
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "metadataFilter": {
            "name": "Metadata Filter",
            "type": "string",
            "description": "Filter in format key:value. Required if 'b' not provided"
        }
    }
    singleton = True

    def parse_arguments(self, request):
        # Parse input arguments

        try:
            ds = request.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        parameter_s = request.get_argument('parameter', None)
        if parameter_s not in ['sst', 'sss', 'wind', None]:
            raise NexusProcessingException(
                reason="Parameter %s not supported. Must be one of 'sst', 'sss', 'wind'." % parameter_s, code=400)

        try:
            start_time = request.get_start_datetime()
            start_time = int((start_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
            end_time = int((end_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        if start_time > end_time:
            raise NexusProcessingException(
                reason="The starting time must be before the ending time. Received startTime: %s, endTime: %s" % (
                    request.get_start_datetime().strftime(ISO_8601), request.get_end_datetime().strftime(ISO_8601)),
                code=400)

        bounding_polygon = metadata_filter = None
        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            metadata_filter = request.get_metadata_filter()
            if 0 == len(metadata_filter):
                raise NexusProcessingException(
                    reason="'b' or 'metadataFilter' argument is required. 'b' must be comma-delimited float formatted "
                           "as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, "
                           "Maximum (Northern) Latitude. 'metadataFilter' must be in the form key:value",
                    code=400)

        return ds, parameter_s, start_time, end_time, bounding_polygon, metadata_filter

    @staticmethod
    def accept_point(nexus_point):
        return isinstance(nexus_point.latitude, np.float64) \
            and isinstance(nexus_point.longitude, np.float64) \
            and isinstance(nexus_point.time, np.int64)

    def calc(self, computeOptions, **args):
        ds, parameter, start_time, end_time, bounding_polygon, metadata_filter = self.parse_arguments(computeOptions)

        includemeta = computeOptions.get_include_meta()

        min_lat = max_lat = min_lon = max_lon = None
        if bounding_polygon:
            min_lat = bounding_polygon.bounds[1]
            max_lat = bounding_polygon.bounds[3]
            min_lon = bounding_polygon.bounds[0]
            max_lon = bounding_polygon.bounds[2]

            tiles = self._get_tile_service().get_tiles_bounded_by_box(min_lat, max_lat, min_lon, max_lon, ds, start_time,
                                                                end_time)
        else:
            tiles = self._get_tile_service().get_tiles_by_metadata(metadata_filter, ds, start_time, end_time)

        data = []
        """
        array_x = np.array([-2, -1, 0, 1, 2, 3])
        multiply = np.broadcast_to(array_x[:, np.newaxis, np.newaxis], (array_x.size, 30, 30))
        """
        for tile in tiles:
            for nexus_point in tile.nexus_point_generator_multi_band(include_nan=False):
                if DataInBoundsSearchCalcMBHandlerImpl.accept_point(nexus_point):
                    point = dict()
                    point['id'] = tile.tile_id

                    if parameter == 'sst':
                        point['sst'] = nexus_point.data_val
                    elif parameter == 'sss':
                        point['sss'] = nexus_point.data_val
                    elif parameter == 'wind':
                        point['wind_u'] = nexus_point.data_val
                        try:
                            point['wind_v'] = tile.meta_data['wind_v'][tuple(nexus_point.index)]
                        except (KeyError, IndexError):
                            pass
                        try:
                            point['wind_direction'] = tile.meta_data['wind_dir'][tuple(nexus_point.index)]
                        except (KeyError, IndexError):
                            pass
                        try:
                            point['wind_speed'] = tile.meta_data['wind_speed'][tuple(nexus_point.index)]
                        except (KeyError, IndexError):
                            pass
                    else:
                        point['variable'] = nexus_point.data_val
                        # TODO hardcoding EVI calculation for demo
                        point['evi'] = DataInBoundsSearchCalcMBHandlerImpl.evi_on_time_series(point['variable'].data)
                        data.append({
                            'latitude': nexus_point.latitude,
                            'longitude': nexus_point.longitude,
                            'time': nexus_point.time,
                            'data': [
                                point['evi']
                            ]
                        })

        if includemeta and len(tiles) > 0:
            meta = [tile.get_summary() for tile in tiles]
        else:
            meta = None

        result = DataInBoundsResult(
            results=data,
            stats={},
            meta=meta)

        result.extendMeta(min_lat, max_lat, min_lon, max_lon, "", start_time, end_time)

        return result

    @staticmethod
    def evi_on_time_series(band_val_tim_series):
        return [DataInBoundsSearchCalcMBHandlerImpl.calculate_evi(v) for v in band_val_tim_series]

    @staticmethod
    def calculate_evi(band_vals):
        """
        Hardcoded method for HLS data.
        Assuming incoming point has 6 data points.
        Assuming they are in this order:
        band 2; blue
        band 3; green
        band 4; red
        band 5; nir
        band 6; swirOne
        band 7; swirTwo
        calculating this formula:   if (whatIndex == 'evi2') {index <- 2.5*(nir - red) / (nir + 2.4*red + 1)}
        """
        x_weights = [0, 0, -2.5, 2.5, 0, 0]
        y_weights = [0, 0, 2.4, 1, 0, 0]
        x_constant = 0
        y_constant = 1
        if len(band_vals) != len(x_weights):
            logger.warning(f'nexus_point array size is different from x_weights. not calculating')
            return None
        x = sum(band_vals * x_weights) + x_constant
        y = sum(band_vals * y_weights) + y_constant
        if math.isnan(x) or math.isnan(y):
            logger.error(f'x or y is resulted in NaN. not calculating. {x} / {y}')
            return None
        if y == 0:
            logger.warning(f'y is None after multiplying. not calculating')
            return None
        return x / y


class DataInBoundsResult(NexusResults):
    def toCSV(self):
        rows = []

        headers = [
            "id",
            "lon",
            "lat",
            "time"
        ]

        for i, result in enumerate(self.results()):
            cols = []

            cols.append(str(result['data'][0]['id']))
            cols.append(str(result['longitude']))
            cols.append(str(result['latitude']))
            cols.append(datetime.utcfromtimestamp(result["time"]).strftime('%Y-%m-%dT%H:%M:%SZ'))
            if 'sst' in result['data'][0]:
                cols.append(str(result['data'][0]['sst']))
                if i == 0:
                    headers.append("sea_water_temperature")
            elif 'sss' in result['data'][0]:
                cols.append(str(result['data'][0]['sss']))
                if i == 0:
                    headers.append("sea_water_salinity")
            elif 'wind_u' in result['data'][0]:
                cols.append(str(result['data'][0]['wind_u']))
                cols.append(str(result['data'][0]['wind_v']))
                cols.append(str(result['data'][0]['wind_direction']))
                cols.append(str(result['data'][0]['wind_speed']))
                if i == 0:
                    headers.append("eastward_wind")
                    headers.append("northward_wind")
                    headers.append("wind_direction")
                    headers.append("wind_speed")

            if i == 0:
                rows.append(",".join(headers))
            rows.append(",".join(cols))

        return "\r\n".join(rows)