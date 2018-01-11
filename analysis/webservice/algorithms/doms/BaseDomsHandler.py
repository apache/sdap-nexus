import StringIO
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
        return DomsNetCDFFormatterAlt.create(self.__executionId, self.results(), self.__args, self.__details)


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
                    primaryValue["time"].strftime(ISO_8601), primaryValue["platform"],
                    primaryValue.get("sea_water_salinity_depth", ""), primaryValue.get("sea_water_salinity", ""),
                    primaryValue.get("sea_water_temperature_depth", ""), primaryValue.get("sea_water_temperature", ""),
                    primaryValue.get("wind_speed", ""), primaryValue.get("wind_direction", ""),
                    primaryValue.get("wind_u", ""), primaryValue.get("wind_v", ""),

                    # Matchup
                    matchup["id"], matchup["source"], matchup["x"], matchup["y"],
                    matchup["time"].strftime(ISO_8601), matchup["platform"],
                    matchup.get("sea_water_salinity_depth", ""), matchup.get("sea_water_salinity", ""),
                    matchup.get("sea_water_temperature_depth", ""), matchup.get("sea_water_temperature", ""),
                    matchup.get("wind_speed", ""), matchup.get("wind_direction", ""),
                    matchup.get("wind_u", ""), matchup.get("wind_v", ""),
                ]

                writer.writerow(row)

    @staticmethod
    def __addConstants(csvfile):

        global_attrs = [
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
            {"Global Attribute": "DOMS_depth_min", "Value": params["depthMin"]},
            {"Global Attribute": "DOMS_depth_min_units", "Value": "m"},
            {"Global Attribute": "DOMS_depth_max", "Value": params["depthMax"]},
            {"Global Attribute": "DOMS_depth_max_units", "Value": "m"},

            {"Global Attribute": "DOMS_platforms", "Value": params["platforms"]},
            {"Global Attribute": "DOMS_SearchRadius", "Value": params["radiusTolerance"]},
            {"Global Attribute": "DOMS_SearchRadius_Units", "Value": "m"},
            {"Global Attribute": "DOMS_bounding_box", "Value": params["bbox"]},

            {"Global Attribute": "DOMS_primary", "Value": params["primary"]},
            {"Global Attribute": "DOMS_match-up", "Value": ",".join(params["matchup"])},
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
        ]

        writer = csv.DictWriter(csvfile, sorted(next(iter(global_attrs)).keys()))

        writer.writerows(global_attrs)


class DomsNetCDFFormatter:
    @staticmethod
    def create(executionId, results, params, details):
        t = tempfile.mkstemp(prefix="doms_", suffix=".nc")
        tempFileName = t[1]

        dataset = Dataset(tempFileName, "w", format="NETCDF4")

        dataset.matchID = executionId
        dataset.Matchup_TimeWindow = params["timeTolerance"]
        dataset.Matchup_TimeWindow_Units = "hours"

        dataset.time_coverage_start = datetime.fromtimestamp(params["startTime"] / 1000).strftime('%Y%m%d %H:%M:%S')
        dataset.time_coverage_end = datetime.fromtimestamp(params["endTime"] / 1000).strftime('%Y%m%d %H:%M:%S')
        dataset.depth_min = params["depthMin"]
        dataset.depth_max = params["depthMax"]
        dataset.platforms = params["platforms"]

        dataset.Matchup_SearchRadius = params["radiusTolerance"]
        dataset.Matchup_SearchRadius_Units = "m"

        dataset.bounding_box = params["bbox"]
        dataset.primary = params["primary"]
        dataset.secondary = ",".join(params["matchup"])

        dataset.Matchup_ParameterPrimary = params["parameter"] if "parameter" in params else ""

        dataset.time_coverage_resolution = "point"

        bbox = geo.BoundingBox(asString=params["bbox"])
        dataset.geospatial_lat_max = bbox.north
        dataset.geospatial_lat_min = bbox.south
        dataset.geospatial_lon_max = bbox.east
        dataset.geospatial_lon_min = bbox.west
        dataset.geospatial_lat_resolution = "point"
        dataset.geospatial_lon_resolution = "point"
        dataset.geospatial_lat_units = "degrees_north"
        dataset.geospatial_lon_units = "degrees_east"
        dataset.geospatial_vertical_min = 0.0
        dataset.geospatial_vertical_max = params["radiusTolerance"]
        dataset.geospatial_vertical_units = "m"
        dataset.geospatial_vertical_resolution = "point"
        dataset.geospatial_vertical_positive = "down"

        dataset.time_to_complete = details["timeToComplete"]
        dataset.num_insitu_matched = details["numInSituMatched"]
        dataset.num_gridded_checked = details["numGriddedChecked"]
        dataset.num_gridded_matched = details["numGriddedMatched"]
        dataset.num_insitu_checked = details["numInSituChecked"]

        dataset.date_modified = datetime.now().strftime('%Y%m%d %H:%M:%S')
        dataset.date_created = datetime.now().strftime('%Y%m%d %H:%M:%S')

        DomsNetCDFFormatter.__addNetCDFConstants(dataset)

        idList = []
        primaryIdList = []
        DomsNetCDFFormatter.__packDataIntoDimensions(idList, primaryIdList, results)

        idDim = dataset.createDimension("id", size=None)
        primaryIdDim = dataset.createDimension("primary_id", size=None)

        idVar = dataset.createVariable("id", "i4", ("id",), chunksizes=(2048,))
        primaryIdVar = dataset.createVariable("primary_id", "i4", ("primary_id",), chunksizes=(2048,))

        idVar[:] = idList
        primaryIdVar[:] = primaryIdList

        DomsNetCDFFormatter.__createDimension(dataset, results, "lat", "f4", "y")
        DomsNetCDFFormatter.__createDimension(dataset, results, "lon", "f4", "x")

        DomsNetCDFFormatter.__createDimension(dataset, results, "sea_water_temperature_depth", "f4",
                                              "sea_water_temperature_depth")
        DomsNetCDFFormatter.__createDimension(dataset, results, "sea_water_temperature", "f4", "sea_water_temperature")
        DomsNetCDFFormatter.__createDimension(dataset, results, "sea_water_salinity_depth", "f4",
                                              "sea_water_salinity_depth")
        DomsNetCDFFormatter.__createDimension(dataset, results, "sea_water_salinity", "f4", "sea_water_salinity")

        DomsNetCDFFormatter.__createDimension(dataset, results, "wind_speed", "f4", "wind_speed")
        DomsNetCDFFormatter.__createDimension(dataset, results, "wind_direction", "f4", "wind_direction")
        DomsNetCDFFormatter.__createDimension(dataset, results, "wind_u", "f4", "wind_u")
        DomsNetCDFFormatter.__createDimension(dataset, results, "wind_v", "f4", "wind_v")

        DomsNetCDFFormatter.__createDimension(dataset, results, "time", "f4", "time")
        dataset.close()

        f = open(tempFileName, "rb")
        data = f.read()
        f.close()
        os.unlink(tempFileName)
        return data

    @staticmethod
    def __packDataIntoDimensions(idVar, primaryIdVar, values, primaryValueId=None):

        for value in values:
            id = hash(value["id"])
            idVar.append(id)
            primaryIdVar.append(primaryValueId if primaryValueId is not None else -1)

            if "matches" in value and len(value["matches"]) > 0:
                DomsNetCDFFormatter.__packDataIntoDimensions(idVar, primaryIdVar, value["matches"], id)

    @staticmethod
    def __packDimensionList(values, field, varList):
        for value in values:
            if field in value:
                varList.append(value[field])
            else:
                varList.append(np.nan)
            if "matches" in value and len(value["matches"]) > 0:
                DomsNetCDFFormatter.__packDimensionList(value["matches"], field, varList)

    @staticmethod
    def __createDimension(dataset, values, name, type, arrayField):
        dim = dataset.createDimension(name, size=None)
        var = dataset.createVariable(name, type, (name,), chunksizes=(2048,), fill_value=-32767.0)

        varList = []
        DomsNetCDFFormatter.__packDimensionList(values, arrayField, varList)

        var[:] = varList

        if name == "lon":
            DomsNetCDFFormatter.__enrichLonVariable(var)
        elif name == "lat":
            DomsNetCDFFormatter.__enrichLatVariable(var)
        elif name == "time":
            DomsNetCDFFormatter.__enrichTimeVariable(var)
        elif name == "sea_water_salinity":
            DomsNetCDFFormatter.__enrichSSSVariable(var)
        elif name == "sea_water_salinity_depth":
            DomsNetCDFFormatter.__enrichSSSDepthVariable(var)
        elif name == "sea_water_temperature":
            DomsNetCDFFormatter.__enrichSSTVariable(var)
        elif name == "sea_water_temperature_depth":
            DomsNetCDFFormatter.__enrichSSTDepthVariable(var)
        elif name == "wind_direction":
            DomsNetCDFFormatter.__enrichWindDirectionVariable(var)
        elif name == "wind_speed":
            DomsNetCDFFormatter.__enrichWindSpeedVariable(var)
        elif name == "wind_u":
            DomsNetCDFFormatter.__enrichWindUVariable(var)
        elif name == "wind_v":
            DomsNetCDFFormatter.__enrichWindVVariable(var)

    @staticmethod
    def __enrichSSSVariable(var):
        var.long_name = "sea surface salinity"
        var.standard_name = "sea_surface_salinity"
        var.units = "1e-3"
        var.valid_min = 30
        var.valid_max = 40
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichSSSDepthVariable(var):
        var.long_name = "sea surface salinity_depth"
        var.standard_name = "sea_surface_salinity_depth"
        var.units = "m"
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichSSTVariable(var):
        var.long_name = "sea surface temperature"
        var.standard_name = "sea_surface_temperature"
        var.units = "c"
        var.valid_min = -3
        var.valid_max = 50
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichSSTDepthVariable(var):
        var.long_name = "sea surface temperature_depth"
        var.standard_name = "sea_surface_temperature_depth"
        var.units = "m"
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichWindDirectionVariable(var):
        var.long_name = "wind direction"
        var.standard_name = "wind_direction"
        var.units = "degrees"
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichWindSpeedVariable(var):
        var.long_name = "wind speed"
        var.standard_name = "wind_speed"
        var.units = "km/h"
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichWindUVariable(var):
        var.long_name = "wind u"
        var.standard_name = "wind_u"
        var.units = ""
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichWindVVariable(var):
        var.long_name = "wind v"
        var.standard_name = "wind_v"
        var.units = ""
        var.scale_factor = 1.0
        var.add_offset = 0.0
        var.coordinates = "lon lat time"
        var.grid_mapping = "crs"
        var.comment = ""
        var.cell_methods = ""
        var.metadata_link = ""

    @staticmethod
    def __enrichTimeVariable(var):
        var.long_name = "Time"
        var.standard_name = "time"
        var.axis = "T"
        var.units = "seconds since 1970-01-01 00:00:00 0:00"
        var.calendar = "standard"
        var.comment = "Nominal time of satellite corresponding to the start of the product time interval"

    @staticmethod
    def __enrichLonVariable(var):
        var.long_name = "Longitude"
        var.standard_name = "longitude"
        var.axis = "X"
        var.units = "degrees_east"
        var.valid_min = -180.0
        var.valid_max = 180.0
        var.comment = "Data longitude for in-situ, midpoint beam for satellite measurements."

    @staticmethod
    def __enrichLatVariable(var):
        var.long_name = "Latitude"
        var.standard_name = "latitude"
        var.axis = "Y"
        var.units = "degrees_north"
        var.valid_min = -90.0
        var.valid_max = 90.0
        var.comment = "Data latitude for in-situ, midpoint beam for satellite measurements."

    @staticmethod
    def __addNetCDFConstants(dataset):
        dataset.bnds = 2
        dataset.Conventions = "CF-1.6, ACDD-1.3"
        dataset.title = "DOMS satellite-insitu machup output file"
        dataset.history = "Processing_Version = V1.0, Software_Name = DOMS, Software_Version = 1.03"
        dataset.institution = "JPL, FSU, NCAR"
        dataset.source = "doms.jpl.nasa.gov"
        dataset.standard_name_vocabulary = "CF Standard Name Table v27", "BODC controlled vocabulary"
        dataset.cdm_data_type = "Point/Profile, Swath/Grid"
        dataset.processing_level = "4"
        dataset.platform = "Endeavor"
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
        dataset.acknowledgment = "DOMS is a NASA/AIST-funded project.  Grant number ####."


class DomsNetCDFFormatterAlt:
    @staticmethod
    def create(executionId, results, params, details):
        t = tempfile.mkstemp(prefix="doms_", suffix=".nc")
        tempFileName = t[1]

        dataset = Dataset(tempFileName, "w", format="NETCDF4")

        dataset.matchID = executionId
        dataset.Matchup_TimeWindow = params["timeTolerance"]
        dataset.Matchup_TimeWindow_Units = "hours"

        dataset.time_coverage_start = datetime.fromtimestamp(params["startTime"] / 1000).strftime('%Y%m%d %H:%M:%S')
        dataset.time_coverage_end = datetime.fromtimestamp(params["endTime"] / 1000).strftime('%Y%m%d %H:%M:%S')
        dataset.depth_min = params["depthMin"]
        dataset.depth_max = params["depthMax"]
        dataset.platforms = params["platforms"]

        dataset.Matchup_SearchRadius = params["radiusTolerance"]
        dataset.Matchup_SearchRadius_Units = "m"

        dataset.bounding_box = params["bbox"]
        dataset.primary = params["primary"]
        dataset.secondary = ",".join(params["matchup"])

        dataset.Matchup_ParameterPrimary = params["parameter"] if "parameter" in params else ""

        dataset.time_coverage_resolution = "point"

        bbox = geo.BoundingBox(asString=params["bbox"])
        dataset.geospatial_lat_max = bbox.north
        dataset.geospatial_lat_min = bbox.south
        dataset.geospatial_lon_max = bbox.east
        dataset.geospatial_lon_min = bbox.west
        dataset.geospatial_lat_resolution = "point"
        dataset.geospatial_lon_resolution = "point"
        dataset.geospatial_lat_units = "degrees_north"
        dataset.geospatial_lon_units = "degrees_east"
        dataset.geospatial_vertical_min = 0.0
        dataset.geospatial_vertical_max = params["radiusTolerance"]
        dataset.geospatial_vertical_units = "m"
        dataset.geospatial_vertical_resolution = "point"
        dataset.geospatial_vertical_positive = "down"

        dataset.time_to_complete = details["timeToComplete"]
        dataset.num_insitu_matched = details["numInSituMatched"]
        dataset.num_gridded_checked = details["numGriddedChecked"]
        dataset.num_gridded_matched = details["numGriddedMatched"]
        dataset.num_insitu_checked = details["numInSituChecked"]

        dataset.date_modified = datetime.now().strftime('%Y%m%d %H:%M:%S')
        dataset.date_created = datetime.now().strftime('%Y%m%d %H:%M:%S')

        DomsNetCDFFormatterAlt.__addNetCDFConstants(dataset)

        satelliteGroup = dataset.createGroup("SatelliteData")
        satelliteWriter = DomsNetCDFValueWriter(satelliteGroup)

        insituGroup = dataset.createGroup("InsituData")
        insituWriter = DomsNetCDFValueWriter(insituGroup)

        matches = DomsNetCDFFormatterAlt.__writeResults(results, satelliteWriter, insituWriter)

        satelliteWriter.commit()
        insituWriter.commit()

        satDim = dataset.createDimension("satellite_ids", size=None)
        satVar = dataset.createVariable("satellite_ids", "i4", ("satellite_ids",), chunksizes=(2048,),
                                        fill_value=-32767)

        satVar[:] = [f[0] for f in matches]

        insituDim = dataset.createDimension("insitu_ids", size=None)
        insituVar = dataset.createVariable("insitu_ids", "i4", ("insitu_ids",), chunksizes=(2048,),
                                           fill_value=-32767)
        insituVar[:] = [f[1] for f in matches]

        dataset.close()

        f = open(tempFileName, "rb")
        data = f.read()
        f.close()
        os.unlink(tempFileName)
        return data

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

        return matches

    @staticmethod
    def __addNetCDFConstants(dataset):
        dataset.bnds = 2
        dataset.Conventions = "CF-1.6, ACDD-1.3"
        dataset.title = "DOMS satellite-insitu machup output file"
        dataset.history = "Processing_Version = V1.0, Software_Name = DOMS, Software_Version = 1.03"
        dataset.institution = "JPL, FSU, NCAR"
        dataset.source = "doms.jpl.nasa.gov"
        dataset.standard_name_vocabulary = "CF Standard Name Table v27", "BODC controlled vocabulary"
        dataset.cdm_data_type = "Point/Profile, Swath/Grid"
        dataset.processing_level = "4"
        dataset.platform = "Endeavor"
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
        dataset.acknowledgment = "DOMS is a NASA/AIST-funded project.  Grant number ####."


class DomsNetCDFValueWriter:
    def __init__(self, group):
        self.latVar = DomsNetCDFValueWriter.__createDimension(group, "lat", "f4")
        self.lonVar = DomsNetCDFValueWriter.__createDimension(group, "lon", "f4")
        self.sstVar = DomsNetCDFValueWriter.__createDimension(group, "sea_water_temperature", "f4")
        self.timeVar = DomsNetCDFValueWriter.__createDimension(group, "time", "f4")

        self.lat = []
        self.lon = []
        self.sst = []
        self.time = []

    def write(self, value):
        self.lat.append(value["y"])
        self.lon.append(value["x"])
        self.time.append(value["time"])
        self.sst.append(value["sea_water_temperature"])

    def commit(self):
        self.latVar[:] = self.lat
        self.lonVar[:] = self.lon
        self.sstVar[:] = self.sst
        self.timeVar[:] = self.time

    @staticmethod
    def __createDimension(group, name, type):
        dim = group.createDimension(name, size=None)
        var = group.createVariable(name, type, (name,), chunksizes=(2048,), fill_value=-32767.0)
        return var
