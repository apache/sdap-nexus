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


from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults


# @nexus_handler
class ChunkSearchCalcHandlerImpl(NexusCalcHandler):
    name = "Data Tile Search"
    path = "/tiles"
    description = "Lists dataset tiles given a geographical area and time range"
    singleton = True
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "One dataset shortname"
        },
        "minLat": {
            "name": "Minimum Latitude",
            "type": "float",
            "description": "Minimum (Southern) bounding box Latitude"
        },
        "maxLat": {
            "name": "Maximum Latitude",
            "type": "float",
            "description": "Maximum (Northern) bounding box Latitude"
        },
        "minLon": {
            "name": "Minimum Longitude",
            "type": "float",
            "description": "Minimum (Western) bounding box Longitude"
        },
        "maxLon": {
            "name": "Maximum Longitude",
            "type": "float",
            "description": "Maximum (Eastern) bounding box Longitude"
        },
        "startTime": {
            "name": "Start Time",
            "type": "long integer",
            "description": "Starting time in seconds since midnight Jan. 1st, 1970 UTC"
        },
        "endTime": {
            "name": "End Time",
            "type": "long integer",
            "description": "Ending time in seconds since midnight Jan. 1st, 1970 UTC"
        }
    }

    def __init__(self):
        NexusCalcHandler.__init__(self, skipCassandra=True)

    def calc(self, computeOptions, **args):
        minLat = computeOptions.get_min_lat()
        maxLat = computeOptions.get_max_lat()
        minLon = computeOptions.get_min_lon()
        maxLon = computeOptions.get_max_lon()
        ds = computeOptions.get_dataset()[0]
        startTime = computeOptions.get_start_time()
        endTime = computeOptions.get_end_time()
        # TODO update to expect tile objects back
        res = [tile.get_summary() for tile in
               self._get_tile_service().find_tiles_in_box(minLat, maxLat, minLon, maxLon, ds, startTime, endTime,
                                                    fetch_data=False)]

        res = NexusResults(results=res)
        res.extendMeta(minLat, maxLat, minLon, maxLon, ds, startTime, endTime)
        return res
