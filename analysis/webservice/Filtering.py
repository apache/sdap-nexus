


import logging
import traceback

import numpy as np
from scipy.signal import filtfilt, butter

log = logging.getLogger('Filtering')


def __fieldToList(results, field):
    a = np.zeros(len(results))
    for n in range(0, len(results)):
        a[n] = results[n][field]
    return a


def __listToField(results, l, field):
    if results is None or l is None:
        raise Exception("Cannot transpose values if they're null")

    if not len(results) == len(l):
        raise Exception("Cannot transpose values between lists of inequal length")

    for n in range(0, len(results)):
        results[n][field] = l[n]


def applySeasonalCycleFilter1d(l):
    if len(l) <= 12:
        return l

    for a in range(0, 12):
        values = []
        for b in range(a, len(l), 12):
            values.append(l[b])
        avg = np.average(values)
        for b in range(a, len(l), 12):
            l[b] -= avg
    return l


def applySeasonalCycleFilter2d(l):
    return l


'''
    Implements monthly filtering of seasonal cycles.
'''


def applySeasonalCycleFilter(l):
    if len(np.shape(l)) == 1:
        return applySeasonalCycleFilter1d(l)
    elif len(np.shape(l)) == 2:
        return applySeasonalCycleFilter2d(l)
    else:
        raise Exception("Cannot apply seasonal cycle filter: Unsupported array shape")


def applySeasonalCycleFilterOnResultsField(results, field):
    l = __fieldToList(results, field)
    applySeasonalCycleFilter(l)
    __listToField(results, l, field)


def applySeasonalCycleFilterOnResults(results):
    [applySeasonalCycleFilterOnResultsField(results, field) for field in ['mean', 'max', 'min']]


'''
http://www.nehalemlabs.net/prototype/blog/2013/04/05/an-introduction-to-smoothing-time-series-in-python-part-i-filtering-theory/
'''


def applyLowPassFilter(y, lowcut=12.0, order=9.0):
    if len(y) - 12 <= lowcut:
        lowcut = 3
    nyq = 0.5 * len(y)
    low = lowcut / nyq
    # high = highcut / nyq
    b, a = butter(order, low)
    m = min([len(y), len(a), len(b)])
    padlen = 30 if m >= 30 else m
    fl = filtfilt(b, a, y, padlen=padlen)
    return fl


def applyFiltersOnField(results, field, applySeasonal=False, applyLowPass=False, append=""):
    x = __fieldToList(results, field)

    if applySeasonal:
        x = applySeasonalCycleFilter(x)
    if applyLowPass:
        x = applyLowPassFilter(x)
    __listToField(results, x, "%s%s" % (field, append))


def applyAllFiltersOnField(results, field, applySeasonal=True, applyLowPass=True):
    try:
        if applySeasonal:
            applyFiltersOnField(results, field, applySeasonal=True, applyLowPass=False, append="Seasonal")
    except Exception as e:
        # If it doesn't work log the error but ignore it
        tb = traceback.format_exc()
        log.warn("Error calculating Seasonal filter:\n%s" % tb)

    try:
        if applyLowPass:
            applyFiltersOnField(results, field, applySeasonal=False, applyLowPass=True, append="LowPass")
    except Exception as e:
        # If it doesn't work log the error but ignore it
        tb = traceback.format_exc()
        log.warn("Error calculating LowPass filter:\n%s" % tb)

    try:
        if applySeasonal and applyLowPass:
            applyFiltersOnField(results, field, applySeasonal=True, applyLowPass=True, append="SeasonalLowPass")
    except Exception as e:
        # If it doesn't work log the error but ignore it
        tb = traceback.format_exc()
        log.warn("Error calculating SeasonalLowPass filter:\n%s" % tb)


'''
class ResultsFilter(object):

    def __init__(self):
        pass

    def filter(self, results, append, **kwargs):
        pass



class SeasonalCycleFilter(ResultsFilter):

    def filter(self, results, append, **kwargs):
        [applySeasonalCycleFilterOnResultsField(results, field) for field in ['mean', 'max', 'min']]

if __name__ == "__main__":

    foo = "bar"
    f = ResultsFilter()
    f.test("Tester", blah=foo)
'''
