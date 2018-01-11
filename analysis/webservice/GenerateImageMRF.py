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
"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import errno
import os
from shutil import copyfile
from subprocess import call

tmpdir = '/tmp/tmp/'
gdal_dir = '/usr/local/anaconda2/envs/nexus/bin/'
gdal_translate = 'gdal_translate'
gdaladdo = 'gdaladdo'
gdalwarp = 'gdalwarp'

COMPRESSION = 'PNG'
OUTWIDTH = 2560
OUTHEIGHT = 1280
OUTWIDTHPOLAR = 8192
OUTHEIGHTPOLAR = 8192

GEO_TILEMATRIXSET = 'EPSG4326_16km'
GEO_PROJECTION = 'EPSG:4326'

ANTARCTIC_TILEMATRIXSET = 'EPSG3031_1km'
ANTARCTIC_PROJECTION = 'EPSG:3031'

ARCTIC_TILEMATRIXSET = 'EPSG3413_1km'
ARCTIC_PROJECTION = 'EPSG:3413'


def create_geo_mrf_header(shortname):
    header = """<MRF_META>
  <Raster>
    <Size x="${_OUTWIDTH}" y="${_OUTHEIGHT}" c="3" />
    <Compression>${_COMPRESSION}</Compression>
    <DataValues NoData="0 0 0 " />
    <Quality>80</Quality>
    <PageSize x="512" y="512" c="3" />
  </Raster>
  <Rsets model="uniform" scale="2" />
  <GeoTags>
    <BoundingBox minx="-180.00000000" miny="-90.00000000" maxx="180.00000000" maxy=" 90.00000000" />
    <Projection>GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]</Projection>
  </GeoTags>
</MRF_META>"""

    header = header.replace('${_OUTWIDTH}', str(OUTWIDTH))
    header = header.replace('${_OUTHEIGHT}', str(OUTHEIGHT))
    header = header.replace('${_COMPRESSION}', COMPRESSION)

    path = shortname + '/MRF-GEO/'
    create_path(path)
    filename = path + shortname + '-geo.mrf'
    write_to_file(filename, header)


def create_geo_xml_config(shortname, prefix):
    config = """<?xml version="1.0" encoding="UTF-8"?>
<LayerConfiguration>
 <Identifier>${shortname}</Identifier>
 <Title>${shortname}</Title>
 <FileNamePrefix>${prefix}_</FileNamePrefix>
 <TiledGroupName>${prefix} tileset</TiledGroupName>
 <HeaderFileName>/etc/onearth/config/headers/${shortname}-geo.mrf</HeaderFileName>
 <Compression>${_COMPRESSION}</Compression>
 <TileMatrixSet>${_GEO_TILEMATRIXSET}</TileMatrixSet>
 <EmptyTileSize offset="0">0</EmptyTileSize>
 <Projection>${_GEO_PROJECTION}</Projection>
 <EnvironmentConfig>/etc/onearth/config/conf/environment_geographic.xml</EnvironmentConfig>
 <ArchiveLocation static="false" year="true" root="geographic">${shortname}</ArchiveLocation>
 <ColorMap>sample.xml</ColorMap>
 <Time>DETECT/P1M</Time>
</LayerConfiguration>"""

    config = config.replace('${shortname}', shortname)
    config = config.replace('${prefix}', prefix)
    config = config.replace('${_COMPRESSION}', COMPRESSION)
    config = config.replace('${_GEO_TILEMATRIXSET}', GEO_TILEMATRIXSET)
    config = config.replace('${_GEO_PROJECTION}', GEO_PROJECTION)

    path = shortname + '/MRF-GEO/'
    create_path(path)
    filename = path + shortname + '-geo.xml'
    write_to_file(filename, config)


def create_arctic_mrf_header(shortname):
    header = """<MRF_META>
  <Raster>
  <Size x="${_OUTWIDTHPOLAR}" y="${_OUTHEIGHTPOLAR}" c="3" />
  <Compression>${_COMPRESSION}</Compression>
    <DataValues NoData="0 0 0" />
    <Quality>80</Quality>
    <PageSize x="512" y="512" c="3" />
  </Raster>
  <Rsets model="uniform" />
  <GeoTags>
    <BoundingBox minx="-4194300.00000000" miny="-4194200.00000000" maxx="4194200.00000000" maxy="4194300.00000000" />
    <Projection>PROJCS["WGS 84 / NSIDC Sea Ice Polar Stereographic North",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","3413"]]</Projection>
  </GeoTags>
</MRF_META>"""

    header = header.replace('${_OUTWIDTHPOLAR}', str(OUTWIDTHPOLAR))
    header = header.replace('${_OUTHEIGHTPOLAR}', str(OUTHEIGHTPOLAR))
    header = header.replace('${_COMPRESSION}', COMPRESSION)

    path = shortname + '/MRF-ARCTIC/'
    create_path(path)
    filename = path + shortname + '-arctic.mrf'
    write_to_file(filename, header)


def create_arctic_xml_config(shortname, prefix):
    config = """<?xml version="1.0" encoding="UTF-8"?>
<LayerConfiguration>
 <Identifier>${shortname}</Identifier>
 <Title>${shortname}</Title>
 <FileNamePrefix>${prefix}_</FileNamePrefix>
 <TiledGroupName>${prefix} tileset</TiledGroupName>
 <HeaderFileName>/etc/onearth/config/headers/${shortname}-arctic.mrf</HeaderFileName>
 <Compression>${_COMPRESSION}</Compression>
 <TileMatrixSet>${_ARCTIC_TILEMATRIXSET}</TileMatrixSet>
 <EmptyTileSize offset="0">0</EmptyTileSize>
 <Projection>${_ARCTIC_PROJECTION}</Projection>
 <EnvironmentConfig>/etc/onearth/config/conf/environment_arctic.xml</EnvironmentConfig>
 <ArchiveLocation static="false" year="true" root="arctic">${shortname}</ArchiveLocation>
 <ColorMap>sample.xml</ColorMap>
 <Time>DETECT/P1M</Time>
</LayerConfiguration>"""

    config = config.replace('${shortname}', shortname)
    config = config.replace('${prefix}', prefix)
    config = config.replace('${_COMPRESSION}', COMPRESSION)
    config = config.replace('${_ARCTIC_TILEMATRIXSET}', ARCTIC_TILEMATRIXSET)
    config = config.replace('${_ARCTIC_PROJECTION}', ARCTIC_PROJECTION)

    path = shortname + '/MRF-ARCTIC/'
    create_path(path)
    filename = path + shortname + '-arctic.xml'
    write_to_file(filename, config)


def create_antarctic_mrf_header(shortname):
    header = """<MRF_META>
  <Raster>
  <Size x="${_OUTWIDTHPOLAR}" y="${_OUTHEIGHTPOLAR}" c="3" />
  <Compression>${_COMPRESSION}</Compression>
    <DataValues NoData="0 0 0" />
    <Quality>80</Quality>
    <PageSize x="512" y="512" c="3" />
  </Raster>
  <Rsets model="uniform" />
  <GeoTags>
    <BoundingBox minx="-4194300.00000000" miny="-4194200.00000000" maxx="4194200.00000000" maxy="4194300.00000000" />
    <Projection>PROJCS["WGS 84 / Antarctic Polar Stereographic",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",-71],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","3031"]]</Projection>
  </GeoTags>
</MRF_META>"""

    header = header.replace('${_OUTWIDTHPOLAR}', str(OUTWIDTHPOLAR))
    header = header.replace('${_OUTHEIGHTPOLAR}', str(OUTHEIGHTPOLAR))
    header = header.replace('${_COMPRESSION}', COMPRESSION)

    path = shortname + '/MRF-ANTARCTIC/'
    create_path(path)
    filename = path + shortname + '-antarctic.mrf'
    write_to_file(filename, header)


def create_antarctic_xml_config(shortname, prefix):
    config = """<?xml version="1.0" encoding="UTF-8"?>
<LayerConfiguration>
 <Identifier>${shortname}</Identifier>
 <Title>${shortname}</Title>
 <FileNamePrefix>${prefix}_</FileNamePrefix>
 <TiledGroupName>${prefix} tileset</TiledGroupName>
 <HeaderFileName>/etc/onearth/config/headers/${shortname}-antarctic.mrf</HeaderFileName>
 <Compression>${_COMPRESSION}</Compression>
 <TileMatrixSet>${_ANTARCTIC_TILEMATRIXSET}</TileMatrixSet>
 <EmptyTileSize offset="0">0</EmptyTileSize>
 <Projection>${_ANTARCTIC_PROJECTION}</Projection>
 <EnvironmentConfig>/etc/onearth/config/conf/environment_antarctic.xml</EnvironmentConfig>
 <ArchiveLocation static="false" year="true" root="antarctic">${shortname}</ArchiveLocation>
 <ColorMap>sample.xml</ColorMap>
 <Time>DETECT/P1M</Time>
</LayerConfiguration>"""

    config = config.replace('${shortname}', shortname)
    config = config.replace('${prefix}', prefix)
    config = config.replace('${_COMPRESSION}', COMPRESSION)
    config = config.replace('${_ANTARCTIC_TILEMATRIXSET}', ANTARCTIC_TILEMATRIXSET)
    config = config.replace('${_ANTARCTIC_PROJECTION}', ANTARCTIC_PROJECTION)

    path = shortname + '/MRF-ANTARCTIC/'
    create_path(path)
    filename = path + shortname + '-antarctic.xml'
    write_to_file(filename, config)


def geo_to_mrf(intiff, prefix, year, dt, shortname):
    path = shortname + '/MRF-GEO/' + str(year)
    create_path(path)

    print('Creating Geographic MRF...')
    src = os.getcwd() + '/resources/transparent.png'
    dst = path + '/' + prefix + '_' + str(dt) + "_.ppg"
    copyfile(src, dst)

    output = path + '/' + prefix + '_' + str(dt) + '_.mrf'

    retcode = call([gdal_dir + gdal_translate, "-of", "MRF", "-co", "COMPRESS=" + COMPRESSION, "-co", "BLOCKSIZE=512",
                    "-outsize", str(OUTWIDTH), str(OUTHEIGHT), intiff, output])

    if retcode == 0:
        print("Creating Geographic Tiles...")
        retcode = call([gdal_dir + gdaladdo, output, "-r", "nearest", "2", "4", "8", "16"])

    return retcode


def geo_to_arctic_mrf(intiff, prefix, year, dt, shortname):
    path = shortname + '/MRF-ARCTIC/' + str(year)
    create_path(path)

    geo_wkt_file = os.getcwd() + '/resources/wkt.txt'
    subsetnorthtiff = tmpdir + prefix + '-epsg3413_stage_0.tif'
    outputnorthtiff = tmpdir + prefix + '-epsg3413_stage_1.tif'
    output = path + '/' + prefix + '_' + str(dt) + "_.mrf"
    tgt_proj4_north = '+proj=stere +lat_0=90 +lat_ts=52.6 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs'

    print('Creating Arctic Subset...')
    retcode = call([gdal_dir + gdal_translate, "-projwin", "-180", "90", "180", "52.6", "-projwin_srs",
                    geo_wkt_file, intiff, subsetnorthtiff])

    if retcode == 0:
        print('Reprojecting to Arctic...')
        retcode = call([gdal_dir + gdalwarp, "-s_srs", geo_wkt_file, "-t_srs", tgt_proj4_north, "-wo",
                        "SOURCE_EXTRA=125", "-dstnodata", "0", "-of", "GTiff", "-overwrite", subsetnorthtiff,
                        outputnorthtiff])

    if retcode == 0:
        print("Creating Arctic MRF...")
        src = os.getcwd() + '/resources/transparent.png'
        dst = path + '/' + prefix + '_' + str(dt) + "_.ppg"
        copyfile(src, dst)

        retcode = call(
            [gdal_dir + gdal_translate, "-of", "MRF", "-co", "COMPRESS=" + COMPRESSION, "-co", "BLOCKSIZE=512",
             "-outsize", str(OUTWIDTHPOLAR), str(OUTHEIGHTPOLAR), outputnorthtiff, output])

    if retcode == 0:
        print("Creating Arctic Tiles...")
        retcode = call([gdal_dir + gdaladdo, output, "-r", "nearest", "2", "4", "8", "16"])

    return retcode


def geo_to_antarctic_mrf(intiff, prefix, year, dt, shortname, interp):
    if (interp == "") or (interp is None):
        interp = "near"

    path = shortname + '/MRF-ANTARCTIC/' + str(year)
    create_path(path)

    geo_wkt_file = os.getcwd() + '/resources/wkt.txt'
    subsetsouthtiff = tmpdir + prefix + '-epsg3031_stage_0.tif'
    outputsouthtiff = tmpdir + prefix + '-epsg3031_stage_1.tif'
    output = path + '/' + prefix + '_' + str(dt) + "_.mrf"
    tgt_proj4_south = '+proj=stere +lat_0=-90 +lat_ts=-52.6 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs'

    print('Creating Antarctic Subset...')
    retcode = call([gdal_dir + gdal_translate, "-projwin", "-180", "-52.6", "180", "-90", "-projwin_srs",
                    geo_wkt_file, intiff, subsetsouthtiff])

    if retcode == 0:
        print("Reprojecting to Antarctic...")
        retcode = call([gdal_dir + gdalwarp, "-s_srs", geo_wkt_file, "-t_srs", tgt_proj4_south, "-wo",
                        "SOURCE_EXTRA=125", "-r", interp, "-dstnodata", "0", "-of", "GTiff", "-overwrite",
                        subsetsouthtiff,
                        outputsouthtiff])

    if retcode == 0:
        print("Creating Antarctic MRF...")
        src = os.getcwd() + '/resources/transparent.png'
        dst = path + '/' + prefix + '_' + str(dt) + "_.ppg"
        copyfile(src, dst)

        retcode = call(
            [gdal_dir + gdal_translate, "-of", "MRF", "-co", "COMPRESS=" + COMPRESSION, "-co", "BLOCKSIZE=512",
             "-r", interp, "-outsize", str(OUTWIDTHPOLAR), str(OUTHEIGHTPOLAR), outputsouthtiff, output])

    if retcode == 0:
        print("Creating Antarctic Tiles...")
        retcode = call([gdal_dir + gdaladdo, output, "-r", interp, "2", "4", "8", "16"])

    return retcode


def write_to_file(filename, data):
    try:
        f = open(filename, 'w')
        f.write(data)
        f.close()
    except Exception as e:
        print("Error creating " + filename + ":\n" + str(e))


def create_path(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def create_all(shortname, prefix):
    create_geo_mrf_header(shortname)
    create_geo_xml_config(shortname, prefix)
    create_arctic_mrf_header(shortname)
    create_arctic_xml_config(shortname, prefix)
    create_antarctic_mrf_header(shortname)
    create_antarctic_xml_config(shortname, prefix)


def png_to_tif(input, output):
    retcode = call([gdal_dir + gdal_translate, "-of", "GTiff", "-ot", "byte", "-a_ullr", "-180", "90", "180", "-90",
                    input, output])
    return retcode
