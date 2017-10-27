"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""


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
    dlat = dest.createDimension(dimname=LATITUDE, size=LATITUDE_SLICE.stop - LATITUDE_SLICE.start)
    dlon = dest.createDimension(dimname=LONGITUDE, size=LONGITUDE_SLICE.stop - LONGITUDE_SLICE.start)

    dest.setncatts(source.__dict__)

    for variable in source.variables:
        variable = source[variable]

        if variable.name == TIME:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(dtime.name,))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[TIME_SLICE]
        elif variable.name == LONGITUDE:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(dlon.name,))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[LONGITUDE_SLICE]
        elif variable.name == LATITUDE:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(dlat.name,))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[LATITUDE_SLICE]
        else:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype,
                                       dimensions=(dtime.name, dlon.name, dlat.name))
            dest[variable.name].setncatts(variable.__dict__)
            dvar[:] = variable[TIME_SLICE, LONGITUDE_SLICE, LATITUDE_SLICE]

    dest.sync()
    dest.close()


from netCDF4 import Dataset

LATITUDE = 'Latitude'
LATITUDE_SLICE = slice(0, 26)
LONGITUDE = 'Longitude'
LONGITUDE_SLICE = slice(0, 80)
TIME = 'Time'
TIME_SLICE = slice(0, 1)


hinput = Dataset(
    '/Users/greguska/data/measures_alt/avg-regrid1x1-ssh_grids_v1609_201701.nc',
    'r')
houtput = Dataset(
    '/Users/greguska/data/measures_alt/avg-regrid1x1-ssh_grids_v1609_201701.split.nc',
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
