

import netCDF4
import datetime
import numpy as np

time_units = 'days since 1981-01-01 00:00:00'
time_units_attr_name = 'units'
time_calendar = 'standard'
time_calendar_attr_name = 'calendar'
time_var_name = 'time'
lat_var_name = 'lat'
lon_var_name = 'lon'

phony_dimension_map = {
    'phony_dim_0': time_var_name,
    'phony_dim_1': lat_var_name,
    'phony_dim_2': lon_var_name
}


def float_to_datetime(time_float):
    """
    Convert time_float (a float in the form of 4-digit_year.fractional year eg. 1994.0384) to a datetime object
    """
    year = int(time_float)
    remainder = time_float - year
    beginning_of_year = datetime.datetime(year, 1, 1)
    end_of_year = datetime.datetime(year + 1, 1, 1)
    seconds = remainder * (end_of_year - beginning_of_year).total_seconds()
    return beginning_of_year + datetime.timedelta(seconds=seconds)


with netCDF4.Dataset('/Users/greguska/data/ice_shelf_dh/ice_shelf_dh_v1.h5') as input_ds:
    latitudes_1d = input_ds[lat_var_name][:, 0]
    longitudes_1d = input_ds[lon_var_name][0, :]

    times_as_int = np.fromiter(
        (netCDF4.date2num(float_to_datetime(time), time_units, calendar=time_calendar) for time in
         input_ds[time_var_name][:]),
        dtype=str(input_ds[time_var_name].dtype), count=len(input_ds[time_var_name][:]))

    with netCDF4.Dataset('/Users/greguska/data/ice_shelf_dh/ice_shelf_dh_v1.nc', mode='w') as output_ds:
        output_ds.setncatts({att: input_ds.getncattr(att) for att in input_ds.ncattrs()})

        for in_dimension_name in input_ds.dimensions:
            out_dimension_name = phony_dimension_map[in_dimension_name]
            output_ds.createDimension(out_dimension_name, len(input_ds.dimensions[in_dimension_name]))

        for in_variable_name, in_variable in input_ds.variables.iteritems():

            if in_variable_name in [time_var_name, lat_var_name, lon_var_name]:
                output_ds.createVariable(in_variable_name, in_variable.dtype, (in_variable_name,))
            else:
                output_ds.createVariable(in_variable_name, in_variable.dtype,
                                         tuple([phony_dimension_map[dim] for dim in in_variable.dimensions]))

            for attr_name in in_variable.ncattrs():
                attr_value = in_variable.getncattr(attr_name)
                if isinstance(attr_value, list):
                    output_ds[in_variable_name].setncattr_string(attr_name, attr_value)
                else:
                    output_ds[in_variable_name].setncattr(attr_name, attr_value)

            if in_variable_name == lat_var_name:
                output_ds[in_variable_name][:] = latitudes_1d
            elif in_variable_name == lon_var_name:
                output_ds[in_variable_name][:] = longitudes_1d
            elif in_variable_name == time_var_name:
                output_ds[in_variable_name].setncattr(time_calendar_attr_name, time_calendar)
                output_ds[in_variable_name].setncattr(time_units_attr_name, time_units)
                output_ds[in_variable_name][:] = times_as_int
            else:
                output_ds[in_variable_name][:] = input_ds[in_variable_name][:]
