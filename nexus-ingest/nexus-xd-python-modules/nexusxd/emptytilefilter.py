"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from nexusproto.serialization import from_shaped_array


def parse_input(nexus_tile_data):
    return nexusproto.NexusTile.FromString(nexus_tile_data)


def filter_empty_tiles(self, tile_data):
    nexus_tile = parse_input(tile_data)
    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    data = from_shaped_array(the_tile_data.variable_data)

    # Only supply data if there is actual values in the tile
    if data.size - numpy.count_nonzero(numpy.isnan(data)) > 0:
        yield tile_data
    elif nexus_tile.HasField("summary"):
        print "Discarding data %s from %s because it is empty" % (
            nexus_tile.summary.section_spec, nexus_tile.summary.granule)


def start():
    start_server(filter_empty_tiles, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
