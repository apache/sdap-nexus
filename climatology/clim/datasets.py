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
datasets.py -- Routines for dataset-specfic capabilities:  file handling, readers, etc.

One Class for each dataset containing static methods and constants/templates, etc.

'''

import sys, os, re, datetime

import numpy as np

from .split import splitByNDaysKeyed, groupByKeys, extractKeys


def splitModisAod(seq, n):
    return splitByNDaysKeyed(seq, n, re.compile(r'(....)(..)(..)'), lambda y, m, d: ymd2doy(y, m, d))


def splitAvhrrSst(seq, n):
    return splitByNDays_Avhrr(seq, n, re.compile(r'^(....)(..)(..)'))


class ModisSst:
    ExpectedRunTime = "28m"
    UrlsPath = "/data/share/datasets/MODIS_L3_AQUA_11UM_V2014.0_4KM_DAILY/daily_data/A*SST*.nc"
    ExampleFileName = 'A2010303.L3m_DAY_NSST_sst_4km.nc'
    GetKeysRegex = r'A(....)(...).L3m_DAY_(.)S'

    VariableName = 'sst'
    Mask = None
    Coordinates = ['lat', 'lon']

    OutputClimTemplate = ''

    @staticmethod
    def keysTransformer(s): return (s[1], s[0], s[2])  # DOY, YEAR, N=night / S=day

    @staticmethod
    def getKeys(url):
        return extractKeys(url, ModisSst.GetKeysRegex, ModisSst.keysTransformer)

    @staticmethod
    def split(seq, n):
        return [u for u in splitByNDaysKeyed(seq, n, ModisSst.GetKeysRegex, ModisSst.keysTransformer)]

    @staticmethod
    def genOutputName(doy, variable, nEpochs, averagingConfig):
        return 'A%03d.L3m_%s_%dday_clim_%s.nc' % (
            doy, variable, nEpochs, averagingConfig['name'])  # mark each file with first day in period


class ModisChlor:
    ExpectedRunTime = "11m"
    UrlsPath = "/Users/greguska/githubprojects/nexus/nexus-ingest/developer-box/data/modis_aqua_chl/A*chlor*.nc"
    ExampleFileName = "A2013187.L3m_DAY_CHL_chlor_a_4km.nc"
    GetKeysRegex = r'A(....)(...).L3m.*CHL'

    Variable = 'chlor_a'
    Mask = None
    Coordinates = ['lat', 'lon']

    OutputClimTemplate = ''

    @staticmethod
    def keysTransformer(s): return (s[1], s[0])  # DOY, YEAR

    @staticmethod
    def getKeys(url):
        return extractKeys(url, ModisChlor.GetKeysRegex, ModisChlor.keysTransformer)

    @staticmethod
    def split(seq, n):
        return [u for u in splitByNDaysKeyed(seq, n, ModisChlor.GetKeysRegex, ModisChlor.keysTransformer)]

    @staticmethod
    def genOutputName(doy, variable, nEpochs, averagingConfig):
        return 'A%03d.L3m_%s_%dday_clim_%s.nc' % (
            doy, variable, nEpochs, averagingConfig['name'])  # mark each file with first day in period


class MeasuresSsh:
    ExpectedRunTime = "2m22s"
    UrlsPath = "/data/share/datasets/MEASURES_SLA_JPL_1603/daily_data/ssh_grids_v1609*12.nc"
    ExampleFileName = "ssh_grids_v1609_2006120812.nc"
    GetKeysRegex = r'ssh.*v1609_(....)(..)(..)12.nc'

    Variable = 'SLA'  # sea level anomaly estimate
    Mask = None
    Coordinates = ['Longitude', 'Latitude']  # Time is first (len=1) coordinate, will be removed

    OutputClimTemplate = ''

    @staticmethod
    def keysTransformer(s): return (ymd2doy(s[0], s[1], s[2]), s[0])  # DOY, YEAR

    @staticmethod
    def getKeys(url):
        return extractKeys(url, MeasuresSsh.GetKeysRegex, MeasuresSsh.keysTransformer)

    @staticmethod
    def split(seq, n):
        return [u for u in splitByNDaysKeyed(seq, n, MeasuresSsh.GetKeysRegex, MeasuresSsh.keysTransformer)]

    @staticmethod
    def genOutputName(doy, variable, nEpochs, averagingConfig):
        return "ssh_grids_v1609_%03d_%dday_clim_%s.nc" % (int(doy), nEpochs, averagingConfig['name'])


class CCMPWind:
    ExpectedRunTime = "?"
    UrlsPath = "/data/share/datasets/CCMP_V2.0_L3.0/daily_data/CCMP_Wind*_V02.0_L3.0_RSS_uncompressed.nc"
    ExampleFileName = "CCMP_Wind_Analysis_20160522_V02.0_L3.0_RSS_uncompressed.nc"
    GetKeysRegex = r'CCMP_Wind_Analysis_(....)(..)(..)_V.*.nc'

    Variable = 'Wind_Magnitude'  # to be computed as sqrt(uwnd^2 + vwnd^2)
    Mask = None
    Coordinates = ['latitude', 'longitude']

    OutputClimTemplate = ''

    @staticmethod
    def keysTransformer(s):
        return (ymd2doy(s[0], s[1], s[2]), s[0])  # DOY, YEAR

    @staticmethod
    def getKeys(url):
        return extractKeys(url, CCMPWind.GetKeysRegex, CCMPWind.keysTransformer)

    @staticmethod
    def split(seq, n):
        return [u for u in splitByNDaysKeyed(seq, n, CCMPWind.GetKeysRegex, CCMPWind.keysTransformer)]

    @staticmethod
    def genOutputName(doy, variable, nEpochs, averagingConfig):
        return "CCMP_Wind_Analysis_V02.0_L3.0_RSS_%03d_%dday_clim_%s.nc" % (int(doy), nEpochs, averagingConfig['name'])

    @staticmethod
    def readAndMask(url, variable, mask=None, cachePath='/tmp/cache', hdfsPath=None):
        """
        Read a variable from a netCDF or HDF file and return a numpy masked array.
        If the URL is remote or HDFS, first retrieve the file into a cache directory.
        """
        from .variables import getVariables, close
        v = None
        if mask:
            variables = [variable, mask]
        else:
            variables = [variable]
        try:
            from .cache import retrieveFile
            path = retrieveFile(url, cachePath, hdfsPath)
        except:
            print('readAndMask: Error, continuing without file %s' % url, file=sys.stderr)
            return v

        if CCMPWind.Variable in variables:
            var, fh = getVariables(path, ['uwnd','vwnd'], arrayOnly=True,
                                   set_auto_mask=True)  # return dict of variable objects by name
            uwnd_avg = np.average(var['uwnd'], axis=0)
            vwnd_avg = np.average(var['vwnd'], axis=0)
            wind_magnitude = np.sqrt(np.add(np.multiply(uwnd_avg, uwnd_avg), np.multiply(vwnd_avg, vwnd_avg)))
            v = wind_magnitude
            if v.shape[0] == 1: v = v[0]  # throw away trivial time dimension for CF-style files
            close(fh)
        else:
            try:
                print('Reading variable %s from %s' % (variable, path), file=sys.stderr)
                var, fh = getVariables(path, variables, arrayOnly=True,
                                       set_auto_mask=True)  # return dict of variable objects by name
                v = var[
                    variable]  # could be masked array
                if v.shape[0] == 1: v = v[0]  # throw away trivial time dimension for CF-style files
                close(fh)
            except:
                print('readAndMask: Error, cannot read variable %s from file %s' % (variable, path), file=sys.stderr)

        return v

class MonthlyClimDataset:
    ExpectedRunTime = "2m"
    UrlsPath = ''
    ExampleFileName = ''
    GetKeysRegex = r'(YYYY)(MM)(DD)' # Regex to extract year, month, day
    Variable = 'var' # Variable name in granule
    Mask = None
    Coordinates = ['lat', 'lon']
    OutputClimTemplate = ''

    @staticmethod
    def keysTransformer(s): 
        return (s[1],s[0]) # MONTH, YEAR

    @staticmethod
    def getKeys(url):
        return extractKeys(url, MonthlyClimDataset.GetKeysRegex, 
                           MonthlyClimDataset.keysTransformer)

    @staticmethod
    def split(seq, n):
        return [u for u in splitByNDaysKeyed(seq, n, 
                                             MonthlyClimDataset.GetKeysRegex, 
                                             MonthlyClimDataset.keysTransformer)]

    @staticmethod
    def genOutputName(month, variable, nEpochs, averagingConfig):
        # Here we use the 15th of the month to get DOY and just use any
        # non-leap year.
        doy = datetime2doy(ymd2datetime(2017, month, 15))
        return 'monthly_clim_%s_%03d_month%02d_nepochs%d_%s.nc' % (
            variable, doy, month, nEpochs, 
            averagingConfig['name'])  # mark each file with month


class SMAP_L3M_SSS(MonthlyClimDataset):
    UrlsPath = "/data/share/datasets/SMAP_L3_SSS/monthly/RSS_smap_SSS_monthly_*.nc"
    ExampleFileName = 'RSS_smap_SSS_monthly_2015_04_v02.0.nc'
    GetKeysRegex = r'RSS_smap_SSS_monthly_(....)_(..)_v02'
    Variable = 'sss_smap'

    @staticmethod
    def getKeys(url):
        return extractKeys(url, SMAP_L3M_SSS.GetKeysRegex, 
                           SMAP_L3M_SSS.keysTransformer)

    @staticmethod
    def split(seq, n):
        return [u for u in splitByNDaysKeyed(seq, n, 
                                             SMAP_L3M_SSS.GetKeysRegex, 
                                             SMAP_L3M_SSS.keysTransformer)]

    @staticmethod
    def genOutputName(month, variable, nEpochs, averagingConfig):
        # Here we use the 15th of the month to get DOY and just use any
        # non-leap year.
        doy = datetime2doy(ymd2datetime(2017, month, 15))
        return '%s_L3m_clim_doy%03d_month%02d_nepochs%d_%s.nc' % (
            variable, doy, month, nEpochs, 
            averagingConfig['name'])  # mark each file with month

class GRACE_Tellus(MonthlyClimDataset):
    GetKeysRegex = r'GRCTellus.JPL.(....)(..)(..).GLO'
    Variable = 'lwe_thickness' # Liquid_Water_Equivalent_Thickness

    @staticmethod
    def getKeys(url):
        return extractKeys(url, GRACE_Tellus.GetKeysRegex, 
                           GRACE_Tellus.keysTransformer)

    @staticmethod
    def split(seq, n):
        return [u for u in splitByNDaysKeyed(seq, n, 
                                             GRACE_Tellus.GetKeysRegex, 
                                             GRACE_Tellus.keysTransformer)]

    @staticmethod
    def genOutputName(month, variable, nEpochs, averagingConfig):
        # Here we use the 15th of the month to get DOY and just use any
        # non-leap year.
        doy = datetime2doy(ymd2datetime(2017, month, 15))
        return 'GRACE_Tellus_monthly_%s_%03d_month%02d_nepochs%d_%s.nc' % (
            variable, doy, month, nEpochs, 
            averagingConfig['name'])  # mark each file with month

class GRACE_Tellus_monthly_land(GRACE_Tellus):
    UrlsPath = "/data/share/datasets/GRACE_Tellus/monthly_land/GRCTellus.JPL.*.nc"
    ExampleFileName = "GRCTellus.JPL.20150122.GLO.RL05M_1.MSCNv02CRIv02.land.nc"

    @staticmethod
    def genOutputName(month, variable, nEpochs, averagingConfig):
        # Here we use the 15th of the month to get DOY and just use any
        # non-leap year.
        doy = datetime2doy(ymd2datetime(2017, month, 15))
        return 'GRACE_Tellus_monthly_land_%s_%03d_month%02d_nepochs%d_%s.nc' % (
            variable, doy, month, nEpochs, 
            averagingConfig['name'])  # mark each file with month

class GRACE_Tellus_monthly_ocean(GRACE_Tellus):
    UrlsPath = "/data/share/datasets/GRACE_Tellus/monthly_ocean/GRCTellus.JPL.*.nc"
    ExampleFileName = "GRCTellus.JPL.20150122.GLO.RL05M_1.MSCNv02CRIv02.ocean.nc"

    @staticmethod
    def genOutputName(month, variable, nEpochs, averagingConfig):
        # Here we use the 15th of the month to get DOY and just use any
        # non-leap year.
        doy = datetime2doy(ymd2datetime(2017, month, 15))
        return 'GRACE_Tellus_monthly_ocean_%s_%03d_month%02d_nepochs%d_%s.nc'%(
            variable, doy, month, nEpochs, 
            averagingConfig['name'])  # mark each file with month


DatasetList = {'ModisSst': ModisSst, 'ModisChlor': ModisChlor,
               'MeasuresSsh': MeasuresSsh, 'CCMPWind': CCMPWind,
               'SMAP_L3M_SSS': SMAP_L3M_SSS, 
               'GRACE_Tellus_monthly_ocean': GRACE_Tellus_monthly_ocean,
               'GRACE_Tellus_monthly_land': GRACE_Tellus_monthly_land}


# Utils follow.
def ymd2doy(year, mon, day):
    return datetime2doy(ymd2datetime(year, mon, day))


def ymd2datetime(y, m, d):
    y, m, d = list(map(int, (y, m, d)))
    return datetime.datetime(y, m, d)


def datetime2doy(dt):
    return int(dt.strftime('%j'))


def doy2datetime(year, doy):
    '''Convert year and DOY (day of year) to datetime object.'''
    return datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(doy) - 1)


def doy2month(year, doy): return doy2datetime(year, doy).strftime('%m')
