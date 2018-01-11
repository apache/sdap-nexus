

import pyximport
pyximport.install()

import ConfigParser

import pkg_resources

from nexustiles.dao.CassandraProxy import CassandraProxy

config = ConfigParser.RawConfigParser()

config.readfp(pkg_resources.resource_stream(__name__, "config/datastores.ini"), filename='datastores.ini')

cass = CassandraProxy(config)

tiles = cass.fetch_nexus_tiles('d9b5afe3-bd7f-3824-ad8a-d8d3b364689c')
print len(tiles[0].tile_blob)
