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

from springxd.tcpstream import LengthHeaderTcpProcessor, start_server

EPOCH = timezone('UTC').localize(datetime.datetime(1970, 1, 1))
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

try:
    start_of_day = environ['GLBLATTR_DAY']
    start_of_day_pattern = environ['GLBLATTR_DAY_FORMAT']
except KeyError:
    start_of_day = None
    start_of_day_pattern = None

try:
    time_offset = long(environ['TIME_OFFSET'])
except KeyError:
    time_offset = None

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

    # Remove file:// if it's there because netcdf lib doesn't like it
    file_path = file_path[len('file://'):] if file_path.startswith('file://') else file_path

    return tile_specifications, file_path


def slices_from_spec(spec):
    dimtoslice = {}
    for dimension in spec.split(','):
        name, start, stop = dimension.split(':')
        dimtoslice[name] = slice(int(start), int(stop))

    return spec, dimtoslice


def to_seconds_from_epoch(date, timeunits=None, start_day=None, timeoffset=None):
    try:
        date = num2date(date, units=timeunits)
    except ValueError:
        assert isinstance(start_day, datetime.date), "start_day is not a datetime.date object"
        the_datetime = datetime.datetime.combine(start_day, datetime.datetime.min.time())
        date = the_datetime + datetime.timedelta(seconds=date)

    if isinstance(date, datetime.datetime):
        date = timezone('UTC').localize(date)
    else:
        date = timezone('UTC').localize(datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S'))

    if timeoffset is not None:
        return long((date - EPOCH).total_seconds()) + timeoffset
    else:
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

            tile.latitude.CopyFrom(to_shaped_array(numpy.ma.filled(ds[latitude][dimtoslice[latitude]], numpy.NaN)))

            tile.longitude.CopyFrom(to_shaped_array(numpy.ma.filled(ds[longitude][dimtoslice[longitude]], numpy.NaN)))

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
                # Note assumption is that index of time is start value in dimtoslice
                tile.time = to_seconds_from_epoch(timevar[dimtoslice[time].start], timeunits=timevar.getncattr('units'), timeoffset=time_offset)

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
            tile.latitude.CopyFrom(
                to_shaped_array(numpy.ma.filled(ds[latitude][tuple(ordered_slices.itervalues())], numpy.NaN)))

            tile.longitude.CopyFrom(
                to_shaped_array(numpy.ma.filled(ds[longitude][tuple(ordered_slices.itervalues())], numpy.NaN)))

            timetile = ds[time][tuple([ordered_slices[time_dim] for time_dim in ds[time].dimensions])].astype('float64',
                                                                                                              casting='same_kind',
                                                                                                              copy=False)
            timeunits = ds[time].getncattr('units')
            try:
                start_of_day_date = datetime.datetime.strptime(ds.getncattr(start_of_day), start_of_day_pattern)
            except Exception:
                start_of_day_date = None

            for index in numpy.ndindex(timetile.shape):
                timetile[index] = to_seconds_from_epoch(timetile[index].item(), timeunits=timeunits,
                                                        start_day=start_of_day_date, timeoffset=time_offset)

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


def start():
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


if __name__ == "__main__":
    start()
