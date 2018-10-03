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


import numpy as np, sys
from osgeo import gdal, osr
from osgeo.gdalconst import *
import tempfile

gdal.AllRegister()



def reproject_2d_matrix(from_data):

    #sr.SetWellKnownGeogCS("EPSG:4326")
    output_raster = "foo.tif"

    driver = gdal.GetDriverByName("MEM")
    dataset = driver.Create(output_raster, from_data.shape[1], from_data.shape[0], 1, gdal.GDT_Float32)
    gt = (-180.0, (360.0 / float(from_data.shape[1])), 0, -90.0, 0, (180.0 / float(from_data.shape[0])))
    dataset.SetGeoTransform(gt)

    dataset.GetRasterBand(1).WriteArray(np.flip(from_data, axis=0))

    sr = osr.SpatialReference()
    sr.ImportFromEPSG(4326)
    dataset.SetProjection(sr.ExportToWkt())

    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(3031)

    from_sr = sr
    to_sr = outSpatialRef

    pixel_spacing = 105000.
    tx = osr.CoordinateTransformation(from_sr, to_sr)
    geo_t = dataset.GetGeoTransform()
    print geo_t
    x_size = dataset.RasterXSize  # Raster xsize
    y_size = dataset.RasterYSize  # Raster ysize
    # Work out the boundaries of the new dataset in the target projection
    (ulx, uly, ulz) = tx.TransformPoint(geo_t[0], geo_t[3])
    (lrx, lry, lrz) = tx.TransformPoint(geo_t[0] + geo_t[1] * x_size, geo_t[3] + geo_t[5] * y_size)

    #dest_cols = int((lrx - ulx) / pixel_spacing)
    #dest_rows = int((uly - lry) / pixel_spacing)
    #print dest_cols, dest_rows
    #print dest_cols, dest_rows
    dest = driver.Create('', from_data.shape[1], from_data.shape[0], 1, gdal.GDT_Float32)
    # Calculate the new geotransform
    new_geo = (ulx, pixel_spacing, geo_t[2], \
               uly, geo_t[4], -pixel_spacing)
    # Set the geotransform
    dest.SetGeoTransform(new_geo)
    dest.SetProjection(to_sr.ExportToWkt())


    res = gdal.ReprojectImage(dataset, dest,
                              from_sr.ExportToWkt(), """PROJCS["WGS 84 / NSIDC Sea Ice Polar Stereographic North",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],AUTHORITY["EPSG","3413"],AXIS["X",UNKNOWN],AXIS["Y",UNKNOWN]]""",
                              gdal.GRA_Bilinear)

    a = dest.GetRasterBand(1).ReadAsArray()
    return a

def reproject_rgba_map(from_data):
    from_data[:, :, 0] = reproject_2d_matrix(from_data[:, :, 0])
    from_data[:, :, 1] = reproject_2d_matrix(from_data[:, :, 1])
    from_data[:, :, 2] = reproject_2d_matrix(from_data[:, :, 2])
    from_data[:, :, 3] = reproject_2d_matrix(from_data[:, :, 3])


if __name__ == "__main__":

    data = np.zeros((512, 1024, 4))
    data[250:260, 0:1024, :] = 255
    data[0:512, 512:522, :] = 255

    reproject_rgba_map(data)


