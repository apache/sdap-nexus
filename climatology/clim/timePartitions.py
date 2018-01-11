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
"""
 timePartitions.py

 Routines to partition time ranges into segments, and time-ordered
 file URL's into time segments.

"""

import os, sys


def partitionFilesByKey(paths, path2keyFn):
    """Partition a list of files (paths) into groups by a key.
The key is computed from the file path by the passed-in 'path2key' function.

For example, to group files by day or month, the key function could return a string
date like 'YYYY/MM/DD' or a month as 'YYYY/MM'.
    """
    key = path2keyFn(paths[0])
    groupedPaths = []
    for path in paths:
        if path.strip() == '': continue
        nextKey = path2keyFn(path)
        if nextKey != key:
            yield (key, groupedPaths)
            key = nextKey
            groupedPaths = [path]
        else:
            groupedPaths.append(path)
    if len(groupedPaths) > 0: yield (key, groupedPaths)


