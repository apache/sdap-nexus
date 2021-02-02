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
#!/bin/env python

"""
array.py -- Simple utilities for arrays:  slice, compress by mask, bundle up, etc.
"""

import sys, os
import numpy as N
#import pynio as NC


def sliceArrays(i, j, *arrays):
    """Slice all input arrays using the supplied indices, a[i:j], and return them in the
same order as input.
    """
    return [a[i:j] for a in arrays]

def compressArrays(mask, *arrays):
    """Compress all input arrays using the supplied mask, a.compress(mask), and return them
in the same order as input.
    """
    return [a.compress(mask) for a in arrays]


class BadNameError(RuntimeError): pass
class DimensionError(RuntimeError): pass
class VariableError(RuntimeError): pass

ReservedKeywords = ['dimensions', 'variables', 'attributes']


class BundleOfArrays(object):
    """Simple holder class for a bundle of arrays (variables).  Each variable is a vector
or array over the supplied dimensions, or a scalar value.  Its purpose is to synchronize
modification of many arrays, such as by slice or compress, and to provide persistence for
a bundle of variables to a file (e.g. netCDF).
    """
    def __init__(self, dimensions=None, **kwargs):
        """Init object with array dimensions and scalar values."""
        self.dimensions = {}; self.variables = {}; self.attributes = {}
        self.createAttributes(**kwargs)
        if dimensions is None: dimensions = ('Record', -1)   # default dim name for vectors
        self.createDims(dimensions)                          # of unlimited dimension (-1)

    def createAttribute(self, name, val):
        self.checkForBadName(name)
        self.attributes[name] = val
        setattr(self, name, val)
        return self

    def createAttributes(self, **kwargs):
        for key, val in kwargs.items():
            self.createAttribute(key, val)

    def createDim(self, name, size):
        """Create a dimension to be used in the variable arrays."""
        self.checkForBadName(name)
        if type(size) != int: raise DimensionError('Size of dimension must be an integer')
        self.dimensions[name] = size

    def createDims(self, *dims):
        """Create multiple dimensions from list of (name, size) tuples."""
        for dim in dims:
            if type(dim) == tuple:
                name, val = dim
            else:
                name = dim.name; val = dim
            self.createDim(name, val)
        return self

    def createVar(self, name, val, copy=False):
        """Create a variable array.  If the value is None, the variable is not created.
By default, the array is NOT copied since a copy may have just been made by slicing, etc.
If you need to make a copy of the incoming val array, use copy=True.
        """
        self.checkForBadName(name)
        if val is not None:
            try:
                n = len(val)
                if copy:
                    try:
                        val = val.copy()   # use copy method if it exists, e.g. numpy array
                    except:
                        val = val[:]   # copy array by slicing
            except:
                raise VariableError('Variable must be a list, vector, array, or None.')
            self.variables[name] = val
            setattr(self, name, val)

    def createVars(self, *vars):
        """Create multiple variables from list of (name, value) tuples."""
        for var in vars:
            if type(var) == list or type(var) == tuple:
                name, val = var
            else:
                name = var.name; val = var
            self.createVar(name, val)
        return self

    def slice(self, i, j):
        """Slice all of the variable arrays as a[i:j], and return new array bundle."""
        out = self.shallowCopy()
        for key in self.variables:
            val = self.variables[key][i:j]
            out.createVar(key, val, copy=False)
        return out
    
    def compress(self, mask):
        """Compress all of the variable arrays using a mask, a.compress(mask)[i:j], and return
a new array bundle.
        """
        out = self.shallowCopy()
        for key in self.variables:
            s = self.variables[key].shape
            a = self.variables[key]
            if len(s) == 1:
                val = a.compress(mask)
            else:
                # Can't use N.compress() from old numpy version because
                # it flattens the additional dimensions (bug).
                # Temporarily, do it by lists in python (slower)
                val = N.array([a[i] for i, b in enumerate(mask) if b])
            out.createVar(key, val, copy=True)
        return out

    def shallowCopy(self):
        """Shallow copy of the object, retaining all attributes, dimensions, and variables.
*** Note:  This routine does NOT copy the arrays themselves, but slice & compress do. ***
        """
        out = BundleOfArrays()
        out.attributes = self.attributes.copy()
        # Must use copy() here or both bundles will point to same attr/dim/var dictionaries.
        # It is a shallow copy, but this is OK since attr/dim values are immutable.
        for key, val in out.attributes.items(): setattr(out, key, val)
        out.dimensions = self.dimensions.copy()
        out.variables = self.variables.copy()
        # Again, shallow copy is OK, referred-to arrays are copied when slice/compress called
        for key, val in out.variables.items(): setattr(out, key, val)
        return out

    def checkForBadName(self, name):
        """Ensure that name is not in reserved attribute list.
Raises exception BadNameError.
        """
        if name in ReservedKeywords:
            raise BadNameError('Attribute name, "%s", is in reserved list %s' % (name, \
                    str(ReservedKeywords)))
#        try:
#            k = getattr(self, name)
#            raise BadNameError('Attribute %s already exists.' % name)
#        except:
#            pass

    def __repr__(self):
        return 'Attributes:  %s\nDimensions:  %s\nVariables:  %s' % tuple(
                   map(str, (self.attributes, self.dimensions, list(self.variables.keys()))))


def test(args):
    n = 300
    vars = ['obsL', 'errL', 'obsP', 'errP']
    obsL = N.array(list(range(n)))+1.; errL = N.ones(n)
    obsP = N.array(list(range(n)))+1.; errP = N.ones(n)
    obs = BundleOfArrays(name='test', nRecords=n).createVars(*list(zip(vars, (obsL, errL, obsP, errP))))
    print(obs)
    print(len(obs.obsL))
    a = obs.shallowCopy()
    print(a)
    print(len(a.obsL))
    b = obs.slice(100, 200)
    print(b)
    print(len(b.obsL))
    print(len(a.obsL))
    print(len(obs.obsL))

def main(args):
    test(args)

if __name__ == '__main__':
    main(sys.argv[1:])
