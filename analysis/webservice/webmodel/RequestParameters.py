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

class RequestParameters(object):
    SEASONAL_CYCLE_FILTER = "seasonalFilter"
    MAX_LAT = "maxLat"
    MIN_LAT = "minLat"
    MAX_LON = "maxLon"
    MIN_LON = "minLon"
    DATASET = "ds"
    ENVIRONMENT = "env"
    OUTPUT = "output"
    START_TIME = "startTime"
    END_TIME = "endTime"
    START_YEAR = "startYear"
    END_YEAR = "endYear"
    CLIM_MONTH = "month"
    START_ROW = "start"
    ROW_COUNT = "numRows"
    APPLY_LOW_PASS = "lowPassFilter"
    LOW_CUT = "lowCut"
    ORDER = "lpOrder"
    PLOT_SERIES = "plotSeries"
    PLOT_TYPE = "plotType"
    NPARTS = "nparts"
    METADATA_FILTER = "metadataFilter"
    NORMALIZE_DATES = "normalizeDates"
