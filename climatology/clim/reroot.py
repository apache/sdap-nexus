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
''' 
 reroot.py -- Change the root of the URL for a list of files

'''

import sys, os
import urlparse

AIRS_DAP = 'http://airspar1.gesdisc.eosdis.nasa.gov/opendap/Aqua_AIRS'
AIRS_FTP = 'ftp://airsl2.gesdisc.eosdis.nasa.gov/ftp/data/s4pa/Aqua_AIRS'
# matchStart for this case is 'Aqua_AIRS'


def reroot(url, root=AIRS_DAP, matchStart='Aqua_AIRS'):
    protocol, netloc, path, params, query, fragment = urlparse.urlparse(url)
    start = root[:root.index(matchStart)]
    rest = path[path.index(matchStart):-1]
    return start + rest


def main(args):
#    root = args[0]
#    matchStart = args[1]
    root = AIRS_DAP
    matchStart = 'Aqua_AIRS'
    for url in sys.stdin:
        print reroot(url, root, matchStart)
        

if __name__ == '__main__':
    main(sys.argv[1:])
