#
# jobClimatology2.py -- mrjob class for computing climatologies by gaussian interpolation
#

from mrjob.job import MRJob
from climatology2 import climByAveragingPeriods


class MRClimatologyByGaussInterp(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()

