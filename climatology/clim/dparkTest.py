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

import dpark

lines = dpark.textFile("/usr/share/dict/words", 128)
#lines = dpark.textFile("/usr/share/doc/gcc-4.4.7/README.Portability", 128)
words = lines.flatMap(lambda x:x.split()).map(lambda x:(x,1))
wc = words.reduceByKey(lambda x,y:x+y).collectAsMap()
print(wc[wc.keys[0]])

