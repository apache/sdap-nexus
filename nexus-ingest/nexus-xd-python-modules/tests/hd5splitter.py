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



def hd5_copy(source, dest):
    for key in source.keys():
        source.copy('/' + key, dest['/'], name=key)

        print key

        if str(key) == 'time':
            dest[key + '_c'] = dest[key][0:4]
        elif str(key) == 'longitude':
            dest[key + '_c'] = dest[key][0:87]
        elif str(key) == 'latitude':
            dest[key + '_c'] = dest[key][0:38]
        else:
            dest[key + '_c'] = dest[key][0:4, 0:38, 0:87]

        # Useful for swath data:
        # if dest[key].ndim == 2:
        #     dest[key + '_c'] = dest[key][0:76, 181:183]
        # elif dest[key].ndim == 3:
        #     dest[key + '_c'] = dest[key][0:76, 181:183, :]
        # elif dest[key].ndim == 1:
        #     dest[key + '_c'] = dest[key][181:183]

        for att in dest[key + '_c'].attrs:
            try:
                dest[key + '_c'].attrs.modify(dest[key].attrs.get(att, default=""))
            except IOError:
                print "error " + att
                pass
        dest[key + '_c'].attrs.update(dest[key].attrs)
        del dest[key]
        dest[key] = dest[key + '_c']
        del dest[key + '_c']

        print dest[key]

    for att in dest.attrs:
        try:
            dest.attrs.modify(source.attrs.get(att, default=""))
        except IOError:
            print "error " + att
            pass

    # dest.attrs.update(source.attrs)

    dest.flush()


def netcdf_subset(source, dest):
    dtime = dest.createDimension(dimname=TIME, size=TIME_SLICE.stop - TIME_SLICE.start)
    # dlat = dest.createDimension(dimname=LATITUDE, size=LATITUDE_SLICE.stop - LATITUDE_SLICE.start)
    # dlon = dest.createDimension(dimname=LONGITUDE, size=LONGITUDE_SLICE.stop - LONGITUDE_SLICE.start)
    drivid = dest.createDimension(dimname='rivid', size=LONGITUDE_SLICE.stop - LONGITUDE_SLICE.start)

    dest.setncatts(source.__dict__)

    for variable in [ v for v in source.variables if v in ['Qout', TIME, LONGITUDE, LATITUDE]]:
        variable = source[variable]

        if variable.name == TIME:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(dtime.name,))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[TIME_SLICE]
        elif variable.name == LONGITUDE:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(drivid.name,))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[LONGITUDE_SLICE]
        elif variable.name == LATITUDE:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(drivid.name,))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[LATITUDE_SLICE]
        else:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype,
                                       dimensions=(dtime.name, drivid.name))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[TIME_SLICE, LONGITUDE_SLICE]

    dest.sync()
    dest.close()


from netCDF4 import Dataset

LATITUDE = 'lat'
LATITUDE_SLICE = slice(0, 1000)
LONGITUDE = 'lon'
LONGITUDE_SLICE = slice(0, 1000)
TIME = 'time'
TIME_SLICE = slice(0, 1)


hinput = Dataset(
    '/Users/greguska/data/swot_example/latest/Qout_WSWM_729days_p0_dtR900s_n1_preonly_20160416.nc',
    'r')
houtput = Dataset(
    '/Users/greguska/data/swot_example/latest/Qout_WSWM_729days_p0_dtR900s_n1_preonly_20160416.split.nc',
    mode='w')

netcdf_subset(hinput, houtput)

# # from h5py import File, Dataset
# hinput = File(
#     '/Users/greguska/githubprojects/nexus/nexus-ingest/developer-box/data/ccmp/CCMP_Wind_Analysis_20160101_V02.0_L3.0_RSS.nc',
#     'r')
# houput = File(
#     '/Users/greguska/githubprojects/nexus/nexus-ingest/developer-box/data/ccmp/CCMP_Wind_Analysis_20160101_V02.0_L3.0_RSS.split.nc',
#     'w')

# hd5_copy(hinput, houput)

# print hinput['/']
# print houtput['/']

# print [attr for attr in hinput.attrs]
# print [attr for attr in houtput.attrs]

# hinput.close()
# houtput.close()
