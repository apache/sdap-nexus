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
 variables.py

Interface to Get Variables out of EOS HDF4/5 and netCDF3/4 files, with
smart dataset discovery and variable caching behind it.

"""

import sys, os, urllib.parse, time
#from pyhdf.SD import SD, SDC
import netCDF4
#from pydap.client import open_url
import numpy as N


def getVariables(url, varNames=None, vars={}, kind=None, arrayOnly=False, order='C', retries=2, sleep=1, set_auto_scale=True, set_auto_mask=True):
    """Interface function to get variables from many file formats or via DAP.  Here kludge for special case."""
    urlStr = url
    url = urllib.parse.urlparse(url)
    path = url.path

    if varNames is None:
        varNames = url.query.split(',')
    else:
        if isinstance(varNames, tuple):
            vars = []
        if url.scheme == 'http':
            if 'dap' in urlStr.lower():
                if kind is None: kind = 'dap'
                if url.query == '':
                    urlStr = urlStr + '?' + ','.join(varNames)
            else:
                if kind is None: kind = 'w10n'

    if url.scheme == '':
        if kind is None:
            kind = fileKind(path)
        else:
            kind = kind.lower()

        if kind == 'h5' or kind == 'hdf5':
            pass

        elif kind == 'hdf' or kind == 'hdf4':
            d = SD(path, SDC.READ)
            if varNames == 'ALL':
                varNames = list(d.datasets().keys())
            for varName in varNames:
                var = d.select(varName)
                if arrayOnly:
                    if order == 'F':
                        var = N.array(var[:], order='F')
                    else:
                        var = var[:]
                if isinstance(vars, list):
                    vars.append(var)
                else:
                    vars[varName] = var
            if not isinstance(vars, list):
                vars['_fileHandle'] = d

        elif kind == 'nc':
            d = netCDF4.Dataset(path)
            d.set_auto_scale(set_auto_scale)
            d.set_auto_mask(set_auto_mask)
            if varNames == 'ALL':
                varNames = list(d.variables.keys())
            for varName in varNames:
                var = d.variables[varName]
                if arrayOnly:
                    if order == 'F':
                        var = N.array(var[:], order='F')
                    else:
                        var = var[:]
                if isinstance(vars, list):
                    vars.append(var)
                else:
                    vars[varName] = var
            if not isinstance(vars, list):
                vars['_fileHandle'] = d

    else:
        if kind == 'dap':
            print('DAP get of: %s' % urlStr, file=sys.stderr)
            retries += 1
            retriesSave = retries
            while retries > 0:
                try:
                    d = open_url(urlStr)
                    retries = 0
                except:
                    retries -= 1
                    if retries == 0:
                        print('getVariables: Error, DAP cannot open: %s' % urlStr, file=sys.stderr)
                        return (vars, d)
                    time.sleep(sleep)

            if varNames == 'ALL':
                varNames = list(d.keys())

            for varName in varNames:
                var = d[varName]
                retries = retriesSave
                while retries > 0:
                    try:
                        if arrayOnly:
                            if order == 'F':
                                var = N.array(var[:], order='F')
                            else:
                                var = var[:]   # actually does DAP call to read array
                        retries = 0
                    except:
                        retries -= 1
                        if retries == 0:
                            print('getVariables: Error, DAP cannot get variable: %s' % varName, file=sys.stderr)
                        else:
                            time.sleep(sleep)

                    if isinstance(vars, list):
                        vars.append(var)
                    else:
                        vars[varName] = var
            if not isinstance(vars, list):
                vars['_fileHandle'] = d


        elif kind == 'w10n':
            vars = None
    return (vars, d)


def close(fh):
    if hasattr(fh, 'end'):
        fh.end()
    elif hasattr(fh, 'close'):
        fh.close()
        
def fileKind(path):
    return os.path.splitext(path)[1][1:].lower()

