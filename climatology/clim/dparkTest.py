
import dpark

lines = dpark.textFile("/usr/share/dict/words", 128)
#lines = dpark.textFile("/usr/share/doc/gcc-4.4.7/README.Portability", 128)
words = lines.flatMap(lambda x:x.split()).map(lambda x:(x,1))
wc = words.reduceByKey(lambda x,y:x+y).collectAsMap()
print wc[wc.keys[0]]

