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

import os
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from nexusproto.serialization import from_shaped_array

try:
    var_name = os.environ["STORED_VAR_NAME"]
except KeyError:
    # STORED_VAR_NAME is optional
    pass


class NoTimeException(Exception):
    pass


def parse_input(nexus_tile_data):
    return nexusproto.NexusTile.FromString(nexus_tile_data)


def summarize_nexustile(self, tiledata):
    nexus_tile = parse_input(tiledata)
    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    latitudes = numpy.ma.masked_invalid(from_shaped_array(the_tile_data.latitude))
    longitudes = numpy.ma.masked_invalid(from_shaped_array(the_tile_data.longitude))

    data = from_shaped_array(the_tile_data.variable_data)

    if nexus_tile.HasField("summary"):
        tilesummary = nexus_tile.summary
    else:
        tilesummary = nexusproto.TileSummary()

    tilesummary.bbox.lat_min = numpy.nanmin(latitudes).item()
    tilesummary.bbox.lat_max = numpy.nanmax(latitudes).item()
    tilesummary.bbox.lon_min = numpy.nanmin(longitudes).item()
    tilesummary.bbox.lon_max = numpy.nanmax(longitudes).item()

    tilesummary.stats.min = numpy.nanmin(data).item()
    tilesummary.stats.max = numpy.nanmax(data).item()

    # In order to accurately calculate the average we need to weight the data based on the cosine of its latitude
    # This is handled slightly differently for swath vs. grid data
    if the_tile_type == 'swath_tile':
        # For Swath tiles, len(data) == len(latitudes) == len(longitudes). So we can simply weight each element in the
        # data array
        tilesummary.stats.mean = numpy.ma.average(numpy.ma.masked_invalid(data),
                                                  weights=numpy.cos(numpy.radians(latitudes))).item()
    elif the_tile_type == 'grid_tile':
        # Grid tiles need to repeat the weight for every longitude
        # TODO This assumes data axis' are ordered as latitude x longitude
        tilesummary.stats.mean = numpy.ma.average(numpy.ma.masked_invalid(data).flatten(),
                                                  weights=numpy.cos(
                                                      numpy.radians(numpy.repeat(latitudes, len(longitudes))))).item()
    else:
        # Default to simple average with no weighting
        tilesummary.stats.mean = numpy.nanmean(data).item()

    tilesummary.stats.count = data.size - numpy.count_nonzero(numpy.isnan(data))

    try:
        min_time, max_time = find_time_min_max(the_tile_data)
        tilesummary.stats.min_time = min_time
        tilesummary.stats.max_time = max_time
    except NoTimeException:
        pass

    try:
        tilesummary.data_var_name = var_name
    except NameError:
        pass

    nexus_tile.summary.CopyFrom(tilesummary)
    yield nexus_tile.SerializeToString()


def find_time_min_max(tile_data):
    # Only try to grab min/max time if it exists as a ShapedArray
    if tile_data.HasField("time") and isinstance(tile_data.time, nexusproto.ShapedArray):
        time_data = from_shaped_array(tile_data.time)
        min_time = long(numpy.nanmin(time_data).item())
        max_time = long(numpy.nanmax(time_data).item())

        return min_time, max_time
    elif tile_data.HasField("time") and isinstance(tile_data.time, (int, long)):
        return tile_data.time, tile_data.time

    raise NoTimeException


def start():
    start_server(summarize_nexustile, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
