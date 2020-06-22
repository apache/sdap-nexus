
"""-----------------------------------------------------------------------------
  Script Name: NEXUS Matchup
  Description: Match measurements between two or more datasets.
  Created By:  Alice Yepremyan
  Date:        12/12/2019
-----------------------------------------------------------------------------"""

import arcpy
import json
import requests
import datetime
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

arcpy.env.overwriteOutput = True


def show_plot(x_data, y_data, x_label, y_label):
    """
    Display a simple line plot.

    :param x_data: Numpy array containing data for the X axis
    :param y_data: Numpy array containing data for the Y axis
    :param x_label: Label applied to X axis
    :param y_label: Label applied to Y axis
    """
    np.random.seed(19680801)
    plt.figure(figsize=(10, 5), dpi=100)
    plt.scatter(x_data, y_data, alpha=0.5)
    plt.grid(b=True, which='major', color='k', linestyle='-')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.show()

# Get the input parameters
host_url = arcpy.GetParameterAsText(0)
primary = arcpy.GetParameterAsText(1)
secondary = arcpy.GetParameterAsText(2)
input_feature = arcpy.GetParameter(3)
start_time = pd.to_datetime(arcpy.GetParameterAsText(4)).strftime('%Y-%m-%dT%H:%M:%SZ')
end_time = pd.to_datetime(arcpy.GetParameterAsText(5)).strftime('%Y-%m-%dT%H:%M:%SZ')
parameter = arcpy.GetParameterAsText(6)
depth_min = arcpy.GetParameterAsText(7)
depth_max = arcpy.GetParameterAsText(8)
tt = arcpy.GetParameterAsText(9)
rt = arcpy.GetParameterAsText(10)
platforms = arcpy.GetParameterAsText(11)

# get coordinates by calculating geometric attributes
arcpy.MakeFeatureLayer_management(input_feature, "layer")
arcpy.AddGeometryAttributes_management("layer", "EXTENT")

rows = arcpy.SearchCursor("layer", fields="EXT_MIN_X;EXT_MIN_Y;EXT_MAX_X;EXT_MAX_Y")
row = rows.next()
min_lon = row.getValue("EXT_MIN_X")
max_lon = row.getValue("EXT_MAX_X")
min_lat = row.getValue("EXT_MIN_Y")
max_lat = row.getValue("EXT_MAX_Y")

# Build the HTTP request
url = f"https://{host_url}/match_spark?primary={primary}&matchup={secondary}&startTime={start_time}&endTime={end_time}&tt={tt}&rt={rt}&b={max_lat},{min_lon},{min_lat},{max_lon}&platforms={platforms}&parameter={parameter}&matchOne=true&depthMin={depth_min}&depthMax={depth_max}"
# url = "https://{}/match_spark?primary=AVHRR_OI_L4_GHRSST_NCEI&matchup=spurs&startTime=2013-10-01T00:00:00Z&endTime=2013-10-30T23:59:59Z&tt=86400&rt=10000.0&b=-30,15,-45,30&platforms=1,2,3,4,5,6,7,8,9&parameter=sst&matchOne=true&depthMin=0&depthMax=5"

# Report a success message
arcpy.AddMessage("Url received, getting json")

ts = json.loads(str(requests.get(url).text))


satellite = []
in_situ = []
for data in ts ['data']:
    for matches in data ['matches']:
        satellite.append(data['sea_water_temperature'])
        in_situ.append(matches['sea_water_temperature'])

# Plot matchup
show_plot(in_situ, satellite, secondary+' (c)', primary+' (c)')