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


