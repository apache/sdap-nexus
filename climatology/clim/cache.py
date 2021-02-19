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
cache.py

Utilities to retrieve files and cache them at deterministic paths.

"""

import sys, os, urllib.parse, urllib.request, urllib.parse, urllib.error

# Directory to cache retrieved files in
CachePath = '/tmp/cache'


def isLocalFile(url):
    '''Check if URL is a local path.'''
    u = urllib.parse.urlparse(url)
    if u.scheme == '' or u.scheme == 'file':
        if not os.path.exists(u.path):
            print('isLocalFile: File at local path does not exist: %s' % u.path, file=sys.stderr)
        return (True, u.path)
    else:
        return (False, u.path)


def retrieveFile(url, cachePath=CachePath, hdfsPath=None, retries=3):
    '''Retrieve a file from a URL or from an HDFS path.'''
    if hdfsPath:
        hdfsFile = os.path.join(hdfsPath, url)
        return hdfsCopyToLocal(hdfsFile, cachePath)
    else:
        return retrieveFileWeb(url, cachePath, retries)


def retrieveFileWeb(url, cachePath=CachePath, retries=3):
    '''Retrieve a file from a URL, or if it is a local path then verify it exists.'''
    if cachePath is None: cachePath = './'
    ok, path = isLocalFile(url)
    if ok: return path

    fn = os.path.split(path)[1]
    outPath = os.path.join(cachePath, fn)
    if os.path.exists(outPath):
        print('retrieveFile: Using cached file: %s' % outPath, file=sys.stderr)
        return outPath
    else:
        print('retrieveFile: Retrieving (URL) %s to %s' % (url, outPath), file=sys.stderr)
        for i in range(retries):
            try:
                urllib.request.urlretrieve(url, outPath)
                return outPath
            except:
                print('retrieveFile: Error retrieving file at URL: %s' % url, file=sys.stderr)
                print('retrieveFile: Retrying ...', file=sys.stderr)
        print('retrieveFile: Fatal error, Cannot retrieve file at URL: %s' % url, file=sys.stderr)
        return None


def hdfsCopyFromLocal(src, dest):
    '''Copy local file into HDFS directory, overwriting using force switch.'''
    outPath = os.path.join(dest, os.path.split(src)[1])
    cmd = "hadoop fs -copyFromLocal -f %s %s" % (src, dest)
    print("Exec overwrite: %s" % cmd, file=sys.stderr)
    os.system(cmd)
    return outPath

def hdfsCopyToLocal(src, dest):
    '''Copy HDFS file to local path, overwriting.'''
    outPath = os.path.join(dest, os.path.split(src)[1])
    os.unlink(outPath)
    cmd = "hadoop fs -copyToLocal %s %s" % (src, dest)
    print("Exec overwrite: %s" % cmd, file=sys.stderr)
    os.system(cmd)
    return outPath
