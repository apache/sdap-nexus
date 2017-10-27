"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import StringIO

import numpy

import nexusproto.NexusContent_pb2 as nexusproto


def from_shaped_array(shaped_array):
    memfile = StringIO.StringIO()
    memfile.write(shaped_array.array_data)
    memfile.seek(0)
    data_array = numpy.load(memfile)
    memfile.close()

    return data_array


def to_shaped_array(data_array):
    shaped_array = nexusproto.ShapedArray()

    shaped_array.shape.extend([dimension_size for dimension_size in data_array.shape])
    shaped_array.dtype = str(data_array.dtype)

    memfile = StringIO.StringIO()
    numpy.save(memfile, data_array)
    shaped_array.array_data = memfile.getvalue()
    memfile.close()

    return shaped_array

def to_metadata(name, data_array):
    metadata = nexusproto.MetaData()
    metadata.name = name
    metadata.meta_data.CopyFrom(to_shaped_array(data_array))

    return metadata
