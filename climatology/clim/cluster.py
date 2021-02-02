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
# cluster.py -- Parallel map function that uses a distributed cluster and multicore within each node
#

import os, sys
from .split import fixedSplit
import pp
#import dispy

Servers = ("server-1", "server-2", "server-3", "server-4")
NCores = 8


def splitSeq(seq, n):
    '''Split a sequence into N almost even parts.'''
    m = int(len(seq)/n)
    return fixedSplit(seq, m)


def dmap(func,                  # function to parallel map across split sequence
         seq,                   # sequence of data
         nCores,                # number of cores to use on each node
         servers=[],            # list of servers in the cluster
         splitterFn=splitSeq,   # function to use to split the sequence of data
        ): 
    '''Parallel operations for a cluster of nodes, each with multiple cores.
    '''
    nServers = len(servers)
    if nServers == 0: servers = ('localhost',)
    jobServer = pp.Server(nCores, ppservers=servers)
    splits = splitSeq(seq, nServers * nCores)
    jobs = [(s, jobServer.submit(func, (s,), ('splitSeq',), globals=globals())) for s in splits]
    results = [(j[0], j[1]()) for j in jobs]
    return results


class PPCluster:
    '''Parallel operations for a cluster of nodes, each with multiple cores.
    '''
    def __init__(self, nCores=NCores,         # number of cores to use on each node
                       servers=Servers,       # list of servers in the cluster
                       splitterFn=splitSeq,   # function to use to split the sequence of data
                ):
        self.nCores = nCores
        self.servers = servers
        self.nServers = len(servers)
        self.splitterFn = splitterFn
        self.jobServer = pp.Server(nCores, ppservers=servers)

    def dmap(self, func, seq):
        '''Distributed map function that automatically uses a cluster of nodes and a set number of cores.
A sequence of data is split across the cluster.
        '''
        splits = splitSeq(seq, self.nServers * self.nCores)
        jobs = [(s, self.jobServer.submit(func, (s,), (splitterFn,), globals=globals())) for s in splits]
        results = [(j[0], j[1]()) for j in jobs]
        return results


# Tests follow.

def extractDay(url, substring):
    '''Extract integer DOY from filename like urls.'''
    i = url.find(substring)
    return int(url[5:8])


def main(args):
    nCores = int(args[0])
    servers = tuple(eval(args[1]))
    urlFile = args[2]
    urls = [s.strip() for s in open(urlFile, 'r')]

#    results = PPCluster(nCores, servers).dmap(lambda u: extractDOY(u, 'A2015'), urls)
    results = dmap(lambda u: extractDOY(u, 'A2015'), urls, nCores, servers, splitSeq)


if __name__ == '__main__':
    print(main(sys.argv[1:]))

# python cluster.py 8 '["deepdata-1"]' urls_sst_2015.txt
# python cluster.py 1 '["deepdata-1", "deepdata-2", "deepdata-3", "deepdata-4"]' urls_sst_2015.txt
# python cluster.py 8 '["deepdata-1", "deepdata-2", "deepdata-3", "deepdata-4"]' urls_sst_2015.txt

