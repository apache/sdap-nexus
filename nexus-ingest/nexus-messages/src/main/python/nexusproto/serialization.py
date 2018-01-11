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
