"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import datetime
import urllib2
from collections import OrderedDict
from contextlib import contextmanager
from os import environ, sep, path, remove

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from netCDF4 import Dataset, num2date
from nexusproto.serialization import to_shaped_array, to_metadata
from pytz import timezone

from tcpstream import LengthHeaderTcpProcessor, start_server

EPOCH = timezone('UTC').localize(datetime.datetime(1970, 1, 1))


@contextmanager
def closing(thing):
    try:
        yield thing
    finally:
        thing.close()


def parse_input(the_input):
    # Split string on ';'
    specs_and_path = [str(part).strip() for part in str(the_input).split(';')]

    # Tile specifications are all but the last element
    specs = specs_and_path[:-1]
    # Generate a list of tuples, where each tuple is a (string, map) that represents a
    # tile spec in the form (str(section_spec), { dimension_name : slice, dimension2_name : slice })
    tile_specifications = [slices_from_spec(section_spec) for section_spec in specs]

    # The path is the last element of the input split by ';'
    file_path = specs_and_path[-1]
    file_name = file_path.split(sep)[-1]
    # If given a temporary directory location, copy the file to the temporary directory and return that path
    if temp_dir is not None:
        temp_file_path = path.join(temp_dir, file_name)
        with closing(urllib2.urlopen(file_path)) as original_granule:
            with open(temp_file_path, 'wb') as temp_granule:
                for chunk in iter((lambda: original_granule.read(512000)), ''):
                    temp_granule.write(chunk)

                file_path = temp_file_path
    return tile_specifications, file_path


def slices_from_spec(spec):
    dimtoslice = {}
    for dimension in spec.split(','):
        name, start, stop = dimension.split(':')
        dimtoslice[name] = slice(int(start), int(stop))

    return spec, dimtoslice


def to_seconds_from_epoch(date, units):
    date = num2date(date, units=units)
    if isinstance(date, datetime.datetime):
        date = timezone('UTC').localize(date)
    else:
        date = timezone('UTC').localize(datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S'))

    return long((date - EPOCH).total_seconds())


def get_ordered_slices(ds, variable, dimension_to_slice):
    dimensions_for_variable = [str(dimension) for dimension in ds[variable].dimensions]
    ordered_slices = OrderedDict()
    for dimension in dimensions_for_variable:
        ordered_slices[dimension] = dimension_to_slice[dimension]
    return ordered_slices


def new_nexus_tile(file_path, section_spec):
    nexus_tile = nexusproto.NexusTile()
    tile_summary = nexusproto.TileSummary()
    tile_summary.granule = file_path.split(sep)[-1]
    tile_summary.section_spec = section_spec
    nexus_tile.summary.CopyFrom(tile_summary)
    return nexus_tile


def read_grid_data(self, section_spec_dataset):
    tile_specifications, file_path = parse_input(section_spec_dataset)

    # Time is optional for Grid data
    try:
        time = environ['TIME']
    except KeyError:
        time = None

    with Dataset(file_path) as ds:
        for section_spec, dimtoslice in tile_specifications:
            tile = nexusproto.GridTile()

            tile.latitude.CopyFrom(to_shaped_array(ds[latitude][dimtoslice[latitude]]))

            tile.longitude.CopyFrom(to_shaped_array(ds[longitude][dimtoslice[longitude]]))

            # Before we read the data we need to make sure the dimensions are in the proper order so we don't have any
            #  indexing issues
            ordered_slices = get_ordered_slices(ds, variable_to_read, dimtoslice)
            # Read data using the ordered slices, replacing masked values with NaN
            data_array = numpy.ma.filled(ds[variable_to_read][tuple(ordered_slices.itervalues())], numpy.NaN)

            tile.variable_data.CopyFrom(to_shaped_array(data_array))

            if metadata is not None:
                tile.meta_data.add().CopyFrom(to_metadata(metadata, ds[metadata][tuple(ordered_slices.itervalues())]))

            if time is not None:
                timevar = ds[time]
                tile.time = to_seconds_from_epoch(timevar[0], timevar.getncattr('units'))

            nexus_tile = new_nexus_tile(file_path, section_spec)
            nexus_tile.tile.grid_tile.CopyFrom(tile)

            yield nexus_tile.SerializeToString()

    # If temp dir is defined, delete the temporary file
    if temp_dir is not None:
        remove(file_path)


def read_swath_data(self, section_spec_dataset):
    tile_specifications, file_path = parse_input(section_spec_dataset)

    # Time is required for swath data
    time = environ['TIME']

    with Dataset(file_path) as ds:
        for section_spec, dimtoslice in tile_specifications:
            tile = nexusproto.SwathTile()
            # Time Lat Long Data and metadata should all be indexed by the same dimensions, order the incoming spec once using the data variable
            ordered_slices = get_ordered_slices(ds, variable_to_read, dimtoslice)
            tile.latitude.CopyFrom(to_shaped_array(ds[latitude][tuple(ordered_slices.itervalues())]))

            tile.longitude.CopyFrom(to_shaped_array(ds[longitude][tuple(ordered_slices.itervalues())]))

            timeunits = ds[time].getncattr('units')
            timetile = ds[time][tuple(ordered_slices.itervalues())]
            for index in numpy.ndindex(timetile.shape):
                timetile[index] = to_seconds_from_epoch(timetile[index].item(), timeunits)
            tile.time.CopyFrom(to_shaped_array(timetile))

            # Read the data converting masked values to NaN
            data_array = numpy.ma.filled(ds[variable_to_read][tuple(ordered_slices.itervalues())], numpy.NaN)
            tile.variable_data.CopyFrom(to_shaped_array(data_array))

            if metadata is not None:
                tile.meta_data.add().CopyFrom(to_metadata(metadata, ds[metadata][tuple(ordered_slices.itervalues())]))

            nexus_tile = new_nexus_tile(file_path, section_spec)
            nexus_tile.tile.swath_tile.CopyFrom(tile)

            yield nexus_tile.SerializeToString()

    # If temp dir is defined, delete the temporary file
    if temp_dir is not None:
        remove(file_path)


if __name__ == "__main__":
    # Required variables for all reader types
    variable_to_read = environ['VARIABLE']
    latitude = environ['LATITUDE']
    longitude = environ['LONGITUDE']
    reader_type = environ['READER']

    # Optional variables for all reader types
    try:
        temp_dir = environ['TEMP_DIR']
    except KeyError:
        temp_dir = None

    try:
        metadata = environ['META']
    except KeyError:
        metadata = None

    # for data in read_grid_data(None,
    #                            "time:0:1,lat:0:1,lon:0:1;time:0:1,lat:1:2,lon:0:1;/Users/greguska/data/mur/20150101090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"):
    #     import sys
    #     from struct import pack
    #
    #     sys.stdout.write(pack("!L", len(data)) + data)

    # for data in read_grid_data(None,
    #                       "lat:0:1,lon:0:1;lat:1:2,lon:0:1;/Users/greguska/data/modis/A2012001.L3m_DAY_NSST_sst_4km.nc"):
    #     import sys
    #     from struct import pack
    #
    #     sys.stdout.write(pack("!L", len(data)) + data)

    # for data in read_swath_data(None,
    #                       "NUMROWS:0:1,NUMCELLS:0:5;NUMROWS:1:2,NUMCELLS:0:5;file:///Users/greguska/data/ascat/ascat_20130314_004801_metopb_02520_eps_o_coa_2101_ovw.l2.nc"):
    #     import sys
    #     from struct import pack
    #     sys.stdout.write(pack("!L", len(data)) + data)

    reader_types = {
        'GRIDTILE': read_grid_data,
        'SWATHTILE': read_swath_data
    }

    try:
        read_method = reader_types[reader_type]
    except KeyError as ke:
        raise NotImplementedError('Environment variable READER must be one of: [%s] but it was ''%s''' % (
            ','.join(reader_types.keys()), reader_type))

    start_server(read_method, LengthHeaderTcpProcessor)
