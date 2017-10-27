#
# sparkTest.py -- word count example to test installation
#

from pyspark import SparkContext

sc = SparkContext(appName="PythonWordCount")
#lines = sc.textFile("file:///home/code/climatology-gaussianInterp/README.md", 8)
lines = sc.textFile("file:///usr/share/dict/words", 128)
counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(lambda x,y: x + y)

output = counts.collect()
#for (word, count) in output:
#    print("%s: %i" % (word, count))

