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
import sys
from math import sqrt
from collections import namedtuple

import numpy as NP
import scipy.stats


def mad( l ):
    """Compute the median absolute deviation (a robust measure of spread) of the list of
    values *l*."""
    median = NP.median(l)
    return NP.median([abs(x - median) for x in l])


def robust_std(l, alpha=1/scipy.stats.norm.ppf(0.75)):
    """Compute a robust estimate of the standard deviation (by default, for normally
    distrbuted samples)."""
    return alpha * mad(l)


def filter_outliers( time_series, n_std=6, indices=False ):
    """Filter outliers (those samples a distance of *n_std* robust standard deviations
    from the median) and return."""
    med = NP.median( time_series )
    std = robust_std( time_series )
    lhs = zip( *[ (i, x) for i, x in enumerate( time_series ) if abs(x - med) < ( std * n_std ) ] )
    if len( lhs ) == 0:
        # No outliers detected, return the full time series
        return time_series
    I, out = lhs
    if isinstance( time_series, NP.ndarray ):
        out = NP.array( out )
    if indices:
        return out, I
    else:
        return out


####################################################################################################

#!/usr/bin/env python
#
# Stats.py -- Simple statistics class:  computes mean, stddev, min, max, rms.
#
# Author: Brian Wilson
#    @(#) Stats.py     1.0     2003/11/24
#                      1.1     2010/12/14 --- rewrote the add method so that
#                                             higher moments are c alculated correctly
#                                             (Mark D. Butala)
#
# Implemented by saving five accumulators:
#   no of points, mean, sum of squares of diffs from mean, min, and max.
# Methods:
#   add    -- add a data point to the accumulating stats
#   calc   -- compute the five statistics:  n, mean, std dev, min, max, rms
#   label  -- set the label for printing
#   format -- set the float format for printing
#   __repr__   -- generates one-line string version of statistics for easy printing
#   reset  -- zero the accumulators
#   addm   -- add an array of data points to the accumulators (add multiple)
#
# See tests at end of file for example usage.
#

StatsCalc = namedtuple('StatsCalc', 'n mean stddev min max rms skewness kurtosis')

    
class Stats(object):
    """Simple statistics class that computes mean, std dev, min, max, and rms."""
    __slots__ = ('count', 'mean', 'stddev', 'min', 'max', 'rms', 'skewness', 'kurtosis',
                 'rms2', 'M2', 'M3', 'M4', 'labelStr', 'formatStr', 'missingValue')

    def __init__(self, missingValue=-9999., label=None, format=None):
        """Create Stats object, optionally set print label and float format string."""
        self.reset(missingValue)
        self.missingValue = missingValue
        self.labelStr = label
        self.formatStr = format
        
    def add(self, val):
        """Add one data point to the accumulators."""
        self.count += 1
        n = self.count
        if n == 1:
            self.mean = 0.
            self.M2 = 0.
            self.rms2 = 0.
            self.M3 = 0.
            self.M4 = 0.
            self.min = val
            self.max = val
        else:
            self.min = min(self.min, val)
            self.max = max(self.max, val)            

        delta = val - self.mean         # use devation from mean to prevent roundoff/overflow problems
        delta_n = delta / float(n)
        delta_n2 = delta_n * delta_n
        self.mean += delta_n
        self.rms2 += (val**2 - self.rms2) / float(n)
        term = delta * delta_n * (n-1)
        self.M4 += term * delta_n2 * (n*n - 3*n + 3) + 6 * delta_n2 * self.M2 - 4 * delta_n * self.M3
        self.M3 += term * delta_n * (n - 2) - 3 * delta_n * self.M2
        self.M2 += term
        return self

    def calc(self):
        """Calculate the statistics for the data added so far.
        Returns tuple of six values:  n, mean, stddev, min, max, rms.
        """
        n = self.count
        if (n >= 2):
            M2 = self.M2
            stddev = sqrt(M2 / float(n - 1))
            rms = sqrt(self.rms2)
            self.stddev = stddev
            self.rms = rms
            self.skewness = sqrt(n) * self.M3 / (M2 * sqrt(M2))
            self.kurtosis = (n * self.M4) / (M2 * M2) - 3
        return StatsCalc(self.count, self.mean, self.stddev, self.min, self.max, self.rms, self.skewness, self.kurtosis)

    def label(self, str):
        """Label the statistics for printing."""
        self.labelStr = str
        return self
        
    def format(self, str):
        """Set the float format to be used in printing stats."""
        self.formatStr = str
        return self
        
    def __repr__(self):
        """One-line stats representation for simple printing."""
        if (self.labelStr == None or self.labelStr == ""): self.labelStr = "Stats"
        line = self.labelStr + ": "
        if self.formatStr:
            a = [self.formatStr for i in xrange(7)]
            a.insert(0, '%d')
            format = ' '.join(a)
            line += format % self.calc()
        else:
            line += "N=%d mean=%f stddev=%f min=%f max=%f rms=%f skewness=%f kurtosis=%f" % self.calc()
        return line

    def reset(self, missingValue):
        """Reset the accumulators to start over."""
        self.count = 0
        self.mean = missingValue
        self.stddev = missingValue
        self.min = missingValue
        self.max = missingValue
        self.rms = missingValue
        self.skewness = missingValue
        self.kurtosis = missingValue
        self.M2 = 0.
        self.rms2 = 0.
        self.M3 = 0.
        self.M4 = 0.
        self.labelStr = None
        self.formatStr = None
        return self

    def addm(self, seq):
        """Add multiple - add a sequence of data points all at once."""
        for val in seq:
            self.add(val)
        return self


####################################################################################################


def main(args):
    fn = args[0]    
    try:
        if fn == '-':
            fid = sys.stdin
        else:
            fid = open(fn, 'r')
        stats = Stats()
        stats.addm( (float(x) for x in fid) )
        print(stats)
    finally:
        if fid is not sys.stdin:
            fid.close()
                            

if __name__ == '__main__':
    main(sys.argv[1:])


'''
    def test():
        """
>>> print Stats()
Stats: 0 0.000000 0.000000 0.000000 0.000000 0.000000

>>> def f(s):
...     for v in [2.3, 4.5, 1.8, 6.2, 3.5]: s.add(v)
...     s.label('test2')
...     return s
>>> print f( Stats() )
test2: 5 3.660000 1.468279 1.800000 6.200000 3.888480

>>> print Stats().label('test3').addm([2.3, 4.5, 1.8, 6.2, 3.5])
test3: 5 3.660000 1.468279 1.800000 6.200000 3.888480

>>> print Stats('test4').format('%5.2f').addm([2.3, 4.5, 1.8, 6.2, 3.5])
test4: 5  3.66  1.47  1.80  6.20  3.89

>>> print Stats('test5', '%4.1f').addm([2.3, 4.5, 1.8, 6.2, 3.5])
test5: 5  3.7  1.5  1.8  6.2  3.9
        """

    import doctest
    doctest.testmod()
'''
