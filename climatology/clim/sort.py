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
#
# sort.py -- Utility routines to sort URL lists into N-day groups for computing climatologies.
#

import sys, os, urllib.parse


def sortByKeys(urls, getKeysFn):
    '''Extract keys (e.g.  DOY and year) from filename and sort by the keys.'''
    keyed = []
    for url in urls:
        if url is None: continue
        url = url.strip()
        if url == '': continue
        keyed.append( (getKeysFn(url), url) )

    keyed.sort()
    sort = [u[1] for u in keyed]     # remove keys
    return sort


def main(args):
    from .datasets import ModisSst
    urlFile = args[0]
    urls = open(urlFile, 'r').readlines()
    urlsSorted = sortByKeys(urls, ModisSst.getKeys)
    print('\n'.join(urlsSorted))


if __name__ == '__main__':
    main(sys.argv[1:])


# Get URL's for MODIS SST daily 4km netCDF files 
# wls is a ls command for the web.  Understands FTP & HTTP root URL's and traverses directories.  Can also retrieve all of the matching files.

# python wls.py --wildcard 'A*sst*.nc' ftp://podaac.jpl.nasa.gov/OceanTemperature/modis/L3/aqua/11um/v2014.0/4km/daily > urls

# Sort by DOY, year, N/D
# python sort.py < urls > urls_sorted

# Now have URL list that is in proper order to compute N-day climatologies.

