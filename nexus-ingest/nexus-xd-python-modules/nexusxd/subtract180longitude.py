"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import nexusproto.NexusContent_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array, to_shaped_array
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor


def transform(self, tile_data):
    """
    This method will transform longitude values in degrees_east from 0 TO 360 to -180 to 180

    :param self:
    :param tile_data: The tile data
    :return: Tile data with altered longitude values
    """
    nexus_tile = nexusproto.NexusTile.FromString(tile_data)

    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    longitudes = from_shaped_array(the_tile_data.longitude)

    # Only subtract 360 if the longitude is greater than 180
    longitudes[longitudes > 180] -= 360

    the_tile_data.longitude.CopyFrom(to_shaped_array(longitudes))

    yield nexus_tile.SerializeToString()


def start():
    start_server(transform, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
