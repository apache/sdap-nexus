"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from webservice.NexusHandler import NexusHandler
from webservice.webmodel import NexusResults


# @nexus_handler
class ChunkSearchHandlerImpl(NexusHandler):
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
        NexusHandler.__init__(self, skipCassandra=True)

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
               self._tile_service.find_tiles_in_box(minLat, maxLat, minLon, maxLon, ds, startTime, endTime,
                                                    fetch_data=False)]

        res = NexusResults(results=res)
        res.extendMeta(minLat, maxLat, minLon, maxLon, ds, startTime, endTime)
        return res
