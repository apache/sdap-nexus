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
from PIL import Image

gdal.AllRegister()



class TargetProjection:
    NORTH = "north"
    SOUTH = "south"

latitude_origin_north = 52.6
latitude_origin_south = -52.6

dst_proj4_north = '+proj=stere +lat_0=90 +lat_ts=52.6 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs'
dst_proj4_south = '+proj=stere +lat_0=-90 +lat_ts=-52.6 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs'


def create_global_dataset(data):
    driver = gdal.GetDriverByName("MEM")
    src_ds = driver.Create("", data.shape[1], data.shape[0], 1, gdal.GDT_Byte)
    gt = (-180.0,
          (360.0 / float(data.shape[1])),
          0,
          90.0,
          0,
          -(180.0 / float(data.shape[0])))

    sr = osr.SpatialReference()
    sr.ImportFromEPSG(4326)

    src_ds.SetProjection(sr.ExportToWkt())
    src_ds.SetGeoTransform(gt)
    src_ds.GetRasterBand(1).WriteArray(data)

    return src_ds

def create_north_dataset(data, latitude_cutoff=latitude_origin_north):

    lat_cutoff_pixel = int(round((90 - latitude_cutoff) / 180.0 * float(data.shape[0])))
    data_cutoff = data[:lat_cutoff_pixel]
    driver = gdal.GetDriverByName("MEM")
    src_ds = driver.Create("", data_cutoff.shape[1], data_cutoff.shape[0], 1, gdal.GDT_Byte)
    gt = (-180.0,
          (360.0 / float(data_cutoff.shape[1])),
          0,
          90.0,
          0,
          -((90 - latitude_cutoff) / float(data_cutoff.shape[0])))
    print gt
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(4326)

    src_ds.SetProjection(sr.ExportToWkt())
    src_ds.SetGeoTransform(gt)

    src_ds.GetRasterBand(1).WriteArray(data_cutoff)

    return src_ds


def create_south_dataset(data, latitude_cutoff=latitude_origin_south):
    lat_cutoff_pixel = int(round((90 - latitude_cutoff) / 180.0 * float(data.shape[0])))

    data_cutoff = data[lat_cutoff_pixel:]
    driver = gdal.GetDriverByName("GTiff")
    src_ds = driver.Create("test_south.tif", data_cutoff.shape[1], data_cutoff.shape[0], 1, gdal.GDT_Byte)
    gt = (-180.0,
          (360.0 / float(data_cutoff.shape[1])),
          0,
          latitude_cutoff,
          0,
          -((latitude_cutoff - -90) / float(data_cutoff.shape[0])))

    print gt
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(4326)

    src_ds.SetProjection(sr.ExportToWkt())
    src_ds.SetGeoTransform(gt)

    src_ds.GetRasterBand(1).WriteArray(data_cutoff)

    return src_ds



def reproject_2d_matrix(from_data, target_proj):
    srs = osr.SpatialReference()

    if target_proj == TargetProjection.NORTH:
        src_ds = create_north_dataset(from_data, latitude_origin_north)
        srs.ImportFromProj4(dst_proj4_north)
        dst_wkt = srs.ExportToWkt()
    elif target_proj == TargetProjection.SOUTH:
        src_ds = create_south_dataset(from_data, latitude_origin_south)
        srs.ImportFromProj4(dst_proj4_south)
        dst_wkt = srs.ExportToWkt()
    else:
        raise Exception("Unsupported target projection")

    error_threshold = 0.125  # error threshold --> use same value as in gdalwarp
    resampling = gdal.GRA_NearestNeighbour

    tmp_ds = gdal.AutoCreateWarpedVRT(src_ds,
                                      None,  # src_wkt : left to default value --> will use the one from source
                                      dst_wkt,
                                      resampling,
                                      error_threshold)


    dst_xsize = tmp_ds.RasterXSize
    dst_ysize = tmp_ds.RasterYSize
    dst_gt = tmp_ds.GetGeoTransform()

    tmp_ds = None

    dst_ds = gdal.GetDriverByName('MEM').Create('', dst_xsize, dst_ysize, src_ds.RasterCount)
    dst_ds.SetProjection(dst_wkt)
    dst_ds.SetGeoTransform(dst_gt)
    dst_ds.GetRasterBand(1).SetNoDataValue(0)

    res = gdal.ReprojectImage(src_ds,
                                dst_ds,
                                None,  # src_wkt : left to default value --> will use the one from source
                                None,  # dst_wkt : left to default value --> will use the one from destination
                                resampling,
                                0,  # WarpMemoryLimit : left to default value
                                error_threshold,
                                None,  # Progress callback : could be left to None or unspecified for silent progress
                                None,
                                ("SOURCE_EXTRA=125",)) # Progress callback user data)

    assert res == 0, 'Error in ReprojectImage'

    return dst_ds.GetRasterBand(1).ReadAsArray()

def reproject_rgba_map(from_data, target_proj):
    data_0 = reproject_2d_matrix(from_data[:, :, 0], target_proj)
    data_1 = reproject_2d_matrix(from_data[:, :, 1], target_proj)
    data_2 = reproject_2d_matrix(from_data[:, :, 2], target_proj)
    data_3 = reproject_2d_matrix(from_data[:, :, 3], target_proj)

    dest_data = np.zeros((data_0.shape[0], data_0.shape[1], 4))
    dest_data[:, :, 0] = data_0
    dest_data[:, :, 1] = data_1
    dest_data[:, :, 2] = data_2
    dest_data[:, :, 3] = data_3


    return dest_data




def create_test_data(h=512, w=1024):
    data = np.zeros((h, w, 4))

    h_mid = h / 2
    w_mid = w / 2
    data[h_mid-10:h_mid+10, 0:w, 0:2] = 255
    data[0:h, w_mid-5:w_mid+5, 0:2] = 255
    data[:, :, 3] = 255
    return data


def read_test_data():
    path = "test_pre.png"
    image = Image.open(path)
    im = np.copy(np.asarray(image, dtype=np.uint8))
    return im


if __name__ == "__main__":
    data = read_test_data()
    out_data = reproject_rgba_map(data, TargetProjection.SOUTH)
    im = Image.fromarray(np.asarray(out_data, dtype=np.uint8))
    im.save("test_warp.png")
    #im = Image.fromarray(np.asarray(out_data, dtype=np.uint8))
    #im.save("test_warp.tif")

    #reproject_rgba_map(data)

