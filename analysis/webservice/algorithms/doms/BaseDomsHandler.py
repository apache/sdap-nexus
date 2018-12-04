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

import StringIO
import os
import csv
import json
from datetime import datetime
from decimal import Decimal

import numpy as np
from pytz import timezone, UTC

import config
import geo
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import NexusResults

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

try:
    from osgeo import gdal
    from osgeo.gdalnumeric import *
except ImportError:
    import gdal
    from gdalnumeric import *

from netCDF4 import Dataset
import netCDF4
import tempfile


class BaseDomsQueryHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)

    def getDataSourceByName(self, source):
        for s in config.ENDPOINTS:
            if s["name"] == source:
                return s
        return None

    def _does_datasource_exist(self, ds):
        for endpoint in config.ENDPOINTS:
            if endpoint["name"] == ds:
                return True
        return False


class DomsEncoder(json.JSONEncoder):
    def __init__(self, **args):
        json.JSONEncoder.__init__(self, **args)

    def default(self, obj):
        # print 'MyEncoder.default() called'
        # print type(obj)
        if obj == np.nan:
            return None  # hard code string for now
        elif isinstance(obj, datetime):
            return long((obj - EPOCH).total_seconds())
        elif isinstance(obj, Decimal):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)


class DomsQueryResults(NexusResults):
    def __init__(self, results=None, args=None, bounds=None, count=None, details=None, computeOptions=None,
                 executionId=None, status_code=200):
        NexusResults.__init__(self, results=results, meta=None, stats=None, computeOptions=computeOptions,
                              status_code=status_code)
        self.__args = args
        self.__bounds = bounds
        self.__count = count
        self.__details = details
        self.__executionId = str(executionId)

    def toJson(self):
        bounds = self.__bounds.toMap() if self.__bounds is not None else {}
        return json.dumps(
            {"executionId": self.__executionId, "data": self.results(), "params": self.__args, "bounds": bounds,
             "count": self.__count, "details": self.__details}, indent=4, cls=DomsEncoder)

    def toCSV(self):
        return DomsCSVFormatter.create(self.__executionId, self.results(), self.__args, self.__details)

    def toNetCDF(self):
        return DomsNetCDFFormatter.create(self.__executionId, self.results(), self.__args, self.__details)


class DomsCSVFormatter:
    @staticmethod
    def create(executionId, results, params, details):

        csv_mem_file = StringIO.StringIO()
        try:
            DomsCSVFormatter.__addConstants(csv_mem_file)
            DomsCSVFormatter.__addDynamicAttrs(csv_mem_file, executionId, results, params, details)
            csv.writer(csv_mem_file).writerow([])

            DomsCSVFormatter.__packValues(csv_mem_file, results)

            csv_out = csv_mem_file.getvalue()
        finally:
            csv_mem_file.close()

        return csv_out

    @staticmethod
    def __packValues(csv_mem_file, results):

        writer = csv.writer(csv_mem_file)

        headers = [
            # Primary
            "id", "source", "lon", "lat", "time", "platform", "sea_water_salinity_depth", "sea_water_salinity",
            "sea_water_temperature_depth", "sea_water_temperature", "wind_speed", "wind_direction", "wind_u", "wind_v",
            # Match
            "id", "source", "lon", "lat", "time", "platform", "sea_water_salinity_depth", "sea_water_salinity",
            "sea_water_temperature_depth", "sea_water_temperature", "wind_speed", "wind_direction", "wind_u", "wind_v"
        ]

        writer.writerow(headers)

        for primaryValue in results:
            for matchup in primaryValue["matches"]:
                row = [
                    # Primary
                    primaryValue["id"], primaryValue["source"], str(primaryValue["x"]), str(primaryValue["y"]),
                    datetime.utcfromtimestamp(primaryValue["time"]).strftime(ISO_8601), primaryValue["platform"],
                    primaryValue.get("sea_water_salinity_depth", ""), primaryValue.get("sea_water_salinity", ""),
                    primaryValue.get("sea_water_temperature_depth", ""), primaryValue.get("sea_water_temperature", ""),
                    primaryValue.get("wind_speed", ""), primaryValue.get("wind_direction", ""),
                    primaryValue.get("wind_u", ""), primaryValue.get("wind_v", ""),

                    # Matchup
                    matchup["id"], matchup["source"], matchup["x"], matchup["y"],
                    datetime.utcfromtimestamp(matchup["time"]).strftime(ISO_8601), matchup["platform"],
                    matchup.get("sea_water_salinity_depth", ""), matchup.get("sea_water_salinity", ""),
                    matchup.get("sea_water_temperature_depth", ""), matchup.get("sea_water_temperature", ""),
                    matchup.get("wind_speed", ""), matchup.get("wind_direction", ""),
                    matchup.get("wind_u", ""), matchup.get("wind_v", ""),
                ]

                writer.writerow(row)

    @staticmethod
    def __addConstants(csvfile):

        global_attrs = [
            {"Global Attribute": "product_version", "Value": "1.0"},
            {"Global Attribute": "Conventions", "Value": "CF-1.6, ACDD-1.3"},
            {"Global Attribute": "title", "Value": "DOMS satellite-insitu machup output file"},
            {"Global Attribute": "history",
             "Value": "Processing_Version = V1.0, Software_Name = DOMS, Software_Version = 1.03"},
            {"Global Attribute": "institution", "Value": "JPL, FSU, NCAR"},
            {"Global Attribute": "source", "Value": "doms.jpl.nasa.gov"},
            {"Global Attribute": "standard_name_vocabulary",
             "Value": "CF Standard Name Table v27, BODC controlled vocabulary"},
            {"Global Attribute": "cdm_data_type", "Value": "Point/Profile, Swath/Grid"},
            {"Global Attribute": "processing_level", "Value": "4"},
            {"Global Attribute": "project", "Value": "Distributed Oceanographic Matchup System (DOMS)"},
            {"Global Attribute": "keywords_vocabulary",
             "Value": "NASA Global Change Master Directory (GCMD) Science Keywords"},
            # TODO What should the keywords be?
            {"Global Attribute": "keywords", "Value": ""},
            {"Global Attribute": "creator_name", "Value": "NASA PO.DAAC"},
            {"Global Attribute": "creator_email", "Value": "podaac@podaac.jpl.nasa.gov"},
            {"Global Attribute": "creator_url", "Value": "https://podaac.jpl.nasa.gov/"},
            {"Global Attribute": "publisher_name", "Value": "NASA PO.DAAC"},
            {"Global Attribute": "publisher_email", "Value": "podaac@podaac.jpl.nasa.gov"},
            {"Global Attribute": "publisher_url", "Value": "https://podaac.jpl.nasa.gov"},
            {"Global Attribute": "acknowledgment", "Value": "DOMS is a NASA/AIST-funded project. NRA NNH14ZDA001N."},
        ]

        writer = csv.DictWriter(csvfile, sorted(next(iter(global_attrs)).keys()))

        writer.writerows(global_attrs)

    @staticmethod
    def __addDynamicAttrs(csvfile, executionId, results, params, details):

        platforms = set()
        for primaryValue in results:
            platforms.add(primaryValue['platform'])
            for match in primaryValue['matches']:
                platforms.add(match['platform'])

        global_attrs = [
            {"Global Attribute": "Platform", "Value": ', '.join(platforms)},
            {"Global Attribute": "time_coverage_start",
             "Value": params["startTime"].strftime(ISO_8601)},
            {"Global Attribute": "time_coverage_end",
             "Value": params["endTime"].strftime(ISO_8601)},
            # TODO I don't think this applies
            # {"Global Attribute": "time_coverage_resolution", "Value": "point"},

            {"Global Attribute": "geospatial_lon_min", "Value": params["bbox"].split(',')[0]},
            {"Global Attribute": "geospatial_lat_min", "Value": params["bbox"].split(',')[1]},
            {"Global Attribute": "geospatial_lon_max", "Value": params["bbox"].split(',')[2]},
            {"Global Attribute": "geospatial_lat_max", "Value": params["bbox"].split(',')[3]},
            {"Global Attribute": "geospatial_lat_resolution", "Value": "point"},
            {"Global Attribute": "geospatial_lon_resolution", "Value": "point"},
            {"Global Attribute": "geospatial_lat_units", "Value": "degrees_north"},
            {"Global Attribute": "geospatial_lon_units", "Value": "degrees_east"},

            {"Global Attribute": "geospatial_vertical_min", "Value": params["depthMin"]},
            {"Global Attribute": "geospatial_vertical_max", "Value": params["depthMax"]},
            {"Global Attribute": "geospatial_vertical_units", "Value": "m"},
            {"Global Attribute": "geospatial_vertical_resolution", "Value": "point"},
            {"Global Attribute": "geospatial_vertical_positive", "Value": "down"},

            {"Global Attribute": "DOMS_matchID", "Value": executionId},
            {"Global Attribute": "DOMS_TimeWindow", "Value": params["timeTolerance"] / 60 / 60},
            {"Global Attribute": "DOMS_TimeWindow_Units", "Value": "hours"},
            # {"Global Attribute": "DOMS_depth_min", "Value": params["depthMin"]},
            # {"Global Attribute": "DOMS_depth_min_units", "Value": "m"},
            # {"Global Attribute": "DOMS_depth_max", "Value": params["depthMax"]},
            # {"Global Attribute": "DOMS_depth_max_units", "Value": "m"},

            {"Global Attribute": "DOMS_platforms", "Value": params["platforms"]},
            {"Global Attribute": "DOMS_SearchRadius", "Value": params["radiusTolerance"]},
            {"Global Attribute": "DOMS_SearchRadius_Units", "Value": "m"},
            # {"Global Attribute": "DOMS_bounding_box", "Value": params["bbox"]},

            {"Global Attribute": "DOMS_primary", "Value": params["primary"]},
            {"Global Attribute": "DOMS_match_up", "Value": params["matchup"]},
            {"Global Attribute": "DOMS_ParameterPrimary", "Value": params.get("parameter", "")},

            {"Global Attribute": "DOMS_time_to_complete", "Value": details["timeToComplete"]},
            {"Global Attribute": "DOMS_time_to_complete_units", "Value": "seconds"},
            {"Global Attribute": "DOMS_num_matchup_matched", "Value": details["numInSituMatched"]},
            {"Global Attribute": "DOMS_num_primary_matched", "Value": details["numGriddedMatched"]},
            {"Global Attribute": "DOMS_num_matchup_checked",
             "Value": details["numInSituChecked"] if details["numInSituChecked"] != 0 else "N/A"},
            {"Global Attribute": "DOMS_num_primary_checked",
             "Value": details["numGriddedChecked"] if details["numGriddedChecked"] != 0 else "N/A"},

            {"Global Attribute": "date_modified", "Value": datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)},
            {"Global Attribute": "date_created", "Value": datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)},

            {"Global Attribute": "URI_Matchup", "Value": "http://webservice matchup query request"},
            {"Global Attribute": "URI_Subset", "Value": "http://webservice subsetting query request"},
        ]

        writer = csv.DictWriter(csvfile, sorted(next(iter(global_attrs)).keys()))

        writer.writerows(global_attrs)


class DomsNetCDFFormatter:
    @staticmethod
    def create(executionId, results, params, details):

        t = tempfile.mkstemp(prefix="doms_", suffix=".nc")
        tempFileName = t[1]

        dataset = Dataset(tempFileName, "w", format="NETCDF4")
        dataset.DOMS_matchID = executionId
        DomsNetCDFFormatter.__addNetCDFConstants(dataset)

        dataset.date_modified = datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)
        dataset.date_created = datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601)
        dataset.time_coverage_start = params["startTime"].strftime('%Y%m%d %H:%M:%S')
        dataset.time_coverage_end = params["endTime"].strftime('%Y%m%d %H:%M:%S')
        dataset.time_coverage_resolution = "point"
        dataset.DOMS_match_up = params["matchup"]
        dataset.DOMS_num_matchup_matched = details["numInSituMatched"]
        dataset.DOMS_num_primary_matched = details["numGriddedMatched"]
        dataset.DOMS_num_matchup_checked = details["numInSituChecked"] if details["numInSituChecked"] != 0 else "N/A"
        dataset.DOMS_num_primary_checked = details["numGriddedChecked"] if details["numGriddedChecked"] != 0 else "N/A"

        bbox = geo.BoundingBox(asString=params["bbox"])
        dataset.geospatial_lat_max = bbox.north
        dataset.geospatial_lat_min = bbox.south
        dataset.geospatial_lon_max = bbox.east
        dataset.geospatial_lon_min = bbox.west
        dataset.geospatial_lat_resolution = "point"
        dataset.geospatial_lon_resolution = "point"
        dataset.geospatial_lat_units = "degrees_north"
        dataset.geospatial_lon_units = "degrees_east"
        dataset.geospatial_vertical_min = params["depthMin"]
        dataset.geospatial_vertical_max = params["depthMax"]
        dataset.geospatial_vertical_units = "m"
        dataset.geospatial_vertical_resolution = "point"
        dataset.geospatial_vertical_positive = "down"

        dataset.Matchup_TimeWindow = params["timeTolerance"] / 60 / 60
        dataset.Matchup_TimeWindow_Units = "hours"
        dataset.DOMS_SearchRadius = params["radiusTolerance"]
        dataset.DOMS_SearchRadius_Units = "m"
        dataset.URI_Subset = "http://webservice subsetting query request"
        dataset.URI_Matchup = "http://webservice matchup query request"
        dataset.DOMS_ParameterPrimary = params["parameter"] if "parameter" in params else ""
        dataset.DOMS_platforms = params["platforms"]
        dataset.DOMS_primary = params["primary"]
        dataset.DOMS_time_to_complete = details["timeToComplete"]
        dataset.DOMS_time_to_complete_units = "seconds"

        platforms = set()
        for primaryValue in results:
            platforms.add(primaryValue['platform'])
            for match in primaryValue['matches']:
                platforms.add(match['platform'])

        dataset.platform = ', '.join(platforms)

        #Create Satellite group, variables, and attributes
        satelliteGroup = dataset.createGroup("SatelliteData")
        satelliteWriter = DomsNetCDFValueWriter(satelliteGroup)

        # Create InSitu group, variables, and attributes
        insituGroup = dataset.createGroup("InsituData")
        insituWriter = DomsNetCDFValueWriter(insituGroup)

        # Add data to Insitu and Satellite groups, generate array of match ID pairs
        matches = DomsNetCDFFormatter.__writeResults(results, satelliteWriter, insituWriter)
        dataset.createDimension("MatchedRecords", size=None)
        dataset.createDimension("MatchedGroups", size=2)
        matchArray = dataset.createVariable("matchIDs", "f4", ("MatchedRecords", "MatchedGroups"))
        matchArray[:] = matches

        dataset.close()
        f = open(tempFileName, "rb")
        data = f.read()
        f.close()
        os.unlink(tempFileName)
        return data

    @staticmethod
    def __addNetCDFConstants(dataset):
        dataset.product_version = "1.0"
        dataset.Conventions = "CF-1.6, ACDD-1.3"
        dataset.title = "DOMS satellite-insitu machup output file"
        dataset.history = "Processing_Version = V1.0, Software_Name = DOMS, Software_Version = 1.03"
        dataset.institution = "JPL, FSU, NCAR"
        dataset.source = "doms.jpl.nasa.gov"
        dataset.standard_name_vocabulary = "CF Standard Name Table v27", "BODC controlled vocabulary"
        dataset.cdm_data_type = "Point/Profile, Swath/Grid"
        dataset.processing_level = "4"

        # dataset.platform = "Endeavor"
        dataset.instrument = "Endeavor on-board sea-bird SBE 9/11 CTD"
        dataset.project = "Distributed Oceanographic Matchup System (DOMS)"
        dataset.keywords_vocabulary = "NASA Global Change Master Directory (GCMD) Science Keywords"
        dataset.keywords = "Salinity, Upper Ocean, SPURS, CTD, Endeavor, Atlantic Ocean"
        dataset.creator_name = "NASA PO.DAAC"
        dataset.creator_email = "podaac@podaac.jpl.nasa.gov"
        dataset.creator_url = "https://podaac.jpl.nasa.gov/"
        dataset.publisher_name = "NASA PO.DAAC"
        dataset.publisher_email = "podaac@podaac.jpl.nasa.gov"
        dataset.publisher_url = "https://podaac.jpl.nasa.gov"
        dataset.acknowledgment = "DOMS is a NASA/AIST-funded project. NRA NNH14ZDA001N."

    @staticmethod
    def __writeResults(results, satelliteWriter, insituWriter):
        ids = {}
        matches = []
        insituIndex = 0

        for r in range(0, len(results)):
            result = results[r]
            satelliteWriter.write(result)
            for match in result["matches"]:
                if match["id"] not in ids:
                    ids[match["id"]] = insituIndex
                    insituIndex += 1
                    insituWriter.write(match)

                matches.append((r, ids[match["id"]]))
                #matches.append((result["id"], match["id"]))

        satelliteWriter.commit("SatelliteData")
        insituWriter.commit("InsituData")

        return matches


class DomsNetCDFValueWriter:
    def __init__(self, group):
        group.createDimension("dim", size=None)

        self.lonVar = DomsNetCDFValueWriter.__createVaraible(group, "lon", "f4")
        self.latVar = DomsNetCDFValueWriter.__createVaraible(group, "lat", "f4")
        self.timeVar = DomsNetCDFValueWriter.__createVaraible(group, "time", "f4")
        self.platformVar = DomsNetCDFValueWriter.__createVaraible(group, "PlatformType", "S1")
        if group.name == "SatelliteData":
            self.measurementVar = DomsNetCDFValueWriter.__createVaraible(group, "SatelliteMeasurements", "f4")
        if group.name == "InsituData":
            self.measurementVar = DomsNetCDFValueWriter.__createVaraible(group, "InSituMeasurements", "f4")
            self.depthVar = DomsNetCDFValueWriter.__createVaraible(group, "depth", "f4")

        self.depth = []
        self.lat = []
        self.lon = []
        self.time = []
        self.platform = []
        self.measurements = []

    def write(self, value):
        self.lat.append(value["y"])
        self.lon.append(value["x"])
        self.time.append(value["time"])
        self.platform.append(value["platform"])
        self.measurements.append(value["sea_water_salinity"])

        self.depth.append(value["sea_water_salinity_depth"])

    def commit(self, group):
        self.latVar[:] = self.lat
        self.lonVar[:] = self.lon
        self.timeVar[:] = self.time
        self.measurementVar[:] = self.measurements

        self.platform = np.asarray(self.platform)
        self.platform = self.platform.astype('S18')
        self.platformVar._Encoding = 'ascii'
        self.platformVar[:] = self.platform

        if group == "InsituData":
            self.depthVar[:] = self.depth

    @staticmethod
    def __createVaraible(group, name, type):
        if name == "PlatformType":
            group.createDimension("nchar", size=18)
            var = group.createVariable(name, type, ("dim", "nchar"), fill_value=-32767.0)
        else:
            var = group.createVariable(name, type, ("dim",), chunksizes=(2048,), fill_value=-32767.0)

        if name == "lon":
            DomsNetCDFValueWriter.__enrichLon(var)
        elif name == "lon_bnds":
            DomsNetCDFValueWriter.__enrichLonBounds(var)
        elif name == "lat":
            DomsNetCDFValueWriter.__enrichLat(var)
        elif name == "lon_bnds":
            DomsNetCDFValueWriter.__enrichLatBounds(var)
        elif name == "time":
            DomsNetCDFValueWriter.__enrichTime(var)
        elif name == "time_bnds":
            DomsNetCDFValueWriter.__enrichTimeBounds(var)
        elif name == "SatelliteMeasurements":
            DomsNetCDFValueWriter.__enrichSSSMeasurements(var)
        elif name == "InSituMeasurements":
            DomsNetCDFValueWriter.__enrichSWSMeasurements(var)
        elif name == "depth":
            DomsNetCDFValueWriter.__enrichDepth(var)
        # elif name == "MetaString":
        #     DomsNetCDFValueWriter.__enrichMetaString(var)

        return var

    @staticmethod
    def __enrichLon(var):
        var.long_name = "Longitude"
        var.standard_name = "Longitude"
        var.axis = "X"
        var.units = "degrees_east"
        var.valid_min = -180.0
        var.valid_max = 180.0

    @staticmethod
    def __enrichLat(var):
        var.long_name = "Latitude"
        var.standard_name = "Latitude"
        var.axis = "Y"
        var.units = "degrees_north"
        var.valid_min = -90.0
        var.valid_max = 90.0

    @staticmethod
    def __enrichTime(var):
        var.long_name = "Time"
        var.standard_name = "time"
        var.axis = "T"
        var.units = "seconds since 1970-01-01 00:00:00 0:00"

    @staticmethod
    def __enrichSSSMeasurements(var):
        var.long_name = "sea surface salinity"
        var.standard_name = "sea_surface_salinity"
        var.units = "1e-3"
        var.valid_min = 30.0
        var.valid_max = 40.0
        var.coordinates = "lon lat time"
        var.metadata_link = "http://..."

    @staticmethod
    def __enrichSWSMeasurements(var):
        var.long_name = "sea water salinity"
        var.standard_name = "sea_water_salinity"
        var.units = "1e-3"
        var.valid_min = 30.0
        var.valid_max = 40.0
        var.coordinates = "lon lat depth time"
        var.metadata_link = "http://..."

    @staticmethod
    def __enrichDepth(var):
        var.valid_min = 0.0
        var.valid_max = 6000.0
        var.long_name = "Depth"
        var.standard_name = "depth"
        var.axis = "Z"
        var.positive = "Down"

    # @staticmethod
    # def __enrichMetaString(var):
    #     var.grid_mapping_name = "latitude_longitude"
    #     var.longitude_of_prime_meridian = 0.0
    #     var.semi_major_axis = 6378137.0
    #     var.inverse_flattening = 298.25723
    #     var.comment = "Latitude-Longitude on WGS84 datum"
