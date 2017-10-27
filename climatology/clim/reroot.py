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
