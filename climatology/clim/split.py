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
#!/usr/bin/env python

"""
split.py == Some utility functions to split lists of URL's into chunks or time periods.

"""

import sys, os, re, json
import datetime


def fixedSplit(seq, n):
    '''Split a sequence into fixed-length chunks of length N.  Last chunk is different length.'''
    chunk = []
    for i, s in enumerate(seq):
        chunk.extend(s)
        if (i+1) % n == 0:
            yield chunk
            chunk = []
    if len(chunk) > 0: yield chunk


def splitByMonth(seq, timeFromFilename={'get': 'doy', 'regex': re.compile(r'\/A.(....)(...)')}, transformer=None, keyed=True):
    '''Split URL's into months using regex to extract information from the filename.  Return list or keyed dictionary.'''
    if timeFromFilename['get'][1] == 'doy':
        transformer = lambda keys: (keys[0], doy2month(*keys))
    urlsByMonth = [ku for ku in splitByKeys(seq, timeFromFilename['regex'], transformer, keyed)]
    return urlsByMonth


def splitByKeys(seq, regex, transformer=None, keyed=True):
    '''Split a sequence into chunks by a key.
The key is extracted from the string by matching a regular expression to the string and returning the matched groups.
    '''
    regex = re.compile(regex)
    chunk = []
    for i, s in enumerate(seq):
        s = s.strip()
        if i == 0:
            keys = extractKeys(s, regex, transformer)
        keys1 = extractKeys(s, regex, transformer)
        if keys1 != keys:
            if keyed:
                if len(keys) == 1:
                    try:
                        intKey = int(keys[0])
                        yield (intKey, chunk)
                    except:
                        yield (keys, chunk)
                else:
                    yield (keys, chunk)
            else:
                yield chunk
            chunk = [s]
            keys = keys1
        else:
            chunk.append(s)
    if len(chunk) > 0:
        if keyed:
            if len(keys) == 1:
                try:
                    intKey = int(keys[0])
                    yield (intKey, chunk)
                except:
                    yield (keys, chunk)
        else:
            yield chunk


def extractKeys(s, regex, transformer=None):
    '''Extract keys from a string by matching a regular expression to the string and returning the matched groups.  Transformer functions alter the keys.'''
    regex = re.compile(regex)
    mat = regex.search(s)
    if not mat:
        print('extractKeys: Fatal error, regex %s does not match %s' % (regex.pattern, s), file=sys.stderr)
        sys.exit(1)
    else:
        keys = mat.groups()
    if transformer is not None:
        keys = transformer(keys)
    return keys
    

def splitByNDays(seq, n, regex, transformer=None, keyed=True):
    '''Split URL's into N-day chunks.'''
    daily = [s for s in splitByKeys(seq, regex, transformer, keyed)]
    for chunk in fixedSplit(daily, n):
        yield chunk

def splitByNDaysKeyed(seq, n, regex, transformer=None, keyed=True):
    '''Split URL's into N-day chunks.'''
    daily = [s for s in splitByKeys(seq, regex, transformer, keyed)]    # url groups keyed by DOY first
    for chunk in daily:
        keys, chunk = chunk
        try:
            key = int(keys[0])
        except:
            key = int(keys)
        i = (int((key-1)/n)) * n + 1
        yield (i, chunk)

def groupByKeys(seq):
    '''Merge multiple keys into a single key by appending lists.'''
    seq = [s for s in seq]
    merge = {}
    for s in seq:
        key, chunk = s
        if key not in merge:
            merge[key] = chunk
        else:
            merge[key].extend(chunk)    # extend returns None, that blows
    result = []
    for k in sorted(merge.keys()):
        result.append((k, merge[k]))
    return result


def windowSplit(seq, nEpochs, nWindow):
    '''Split a sequence (e.g. of daily files/urls) into nWindow-long chunks for climatology averaging.
The length of the window will usually be longer than the nEpochs the average is good for.
    '''
    pass


# Tests follow.

def test1(args):
    n = int(args[0])
    fn = args[1]
    with open(fn, 'r') as f:
        for chunk in fixedSplit(f, n):
            print(' '.join(chunk))

def test2(args):
    regex = args[0]
    regex = re.compile(regex)
    fn = args[1]
    with open(fn, 'r') as f:
        for chunk in splitByKey(f, regex):
            print(' '.join(chunk))

def test3(args):
    '''Broken!'''
    nDays = int(args[0])
    regex = args[1]
    regex = re.compile(regex)
    fn = args[2]
    with open(fn, 'r') as f:
        for chunk in splitByNDays(f, nDays, regex):
            print(chunk)

def test4(args):
    '''Correct!'''
    nDays = int(args[0])
    regex = args[1]
    regex = re.compile(regex)
    fn = args[2]
    with open(fn, 'r') as f:
        for chunk in splitByNDays(f, nDays, regex):
            print()
            print('\n'.join(chunk))
            print(len(chunk))

def test5(args):
    '''Generate multi-line JSON for pyspark.'''
    nDays = int(args[0])
    regex = args[1]
    fn = args[2]
    with open(fn, 'r') as f:
        for chunk in splitByNDays(f, nDays, regex):
            print(json.dumps(chunk))

def test6(args):
    '''Generate keyed split by month for spark.'''
    regex = args[0]
    fn = args[1]
    with open(fn, 'r') as f:
        for chunk in splitByMonth(f, {'get': 'doy', 'regex': re.compile(regex)}):
            print(chunk)


def main(args):
#    test1(args)
#    test2(args)
#    test3(args)
#    test4(args)
#    test5(args)
    test6(args)

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])


# python split.py 5 '(...).L3m' urls_sst_daynight_2003_2015_sorted.txt

# python split.py '\/A(....)(...)' urls_sst_daynight_2003_4months.txt
# python split.py '\/A(....)(...)' urls_sst_daynight_2003_2015.txt
