
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
# ToDO: make lat/time interactive
primary = arcpy.GetParameterAsText(0)
secondary = arcpy.GetParameterAsText(1)
parameter = arcpy.GetParameterAsText(2)
startTime = pd.to_datetime(arcpy.GetParameterAsText(3)).strftime('%Y-%m-%dT%H:%M:%SZ')
endTime = pd.to_datetime(arcpy.GetParameterAsText(4)).strftime('%Y-%m-%dT%H:%M:%SZ')
minLat = arcpy.GetParameterAsText(5)
maxLat = arcpy.GetParameterAsText(6)
minLon = arcpy.GetParameterAsText(7)
maxLon = arcpy.GetParameterAsText(8)
depthMin = arcpy.GetParameterAsText(9)
depthMax = arcpy.GetParameterAsText(10)
tt = arcpy.GetParameterAsText(11)
rt = arcpy.GetParameterAsText(12)
platforms = arcpy.GetParameterAsText(13)

# Build the HTTP request
url = f"https://oceanworks.jpl.nasa.gov/match_spark?primary={primary}&matchup={secondary}&startTime={startTime}&endTime={endTime}&tt={tt}&rt={rt}&b={maxLat},{minLon},{minLat},{maxLon}&platforms={platforms}&parameter={parameter}&matchOne=true&depthMin={depthMin}&depthMax={depthMax}"
# url = "https://oceanworks.jpl.nasa.gov/match_spark?primary=AVHRR_OI_L4_GHRSST_NCEI&matchup=spurs&startTime=2013-10-01T00:00:00Z&endTime=2013-10-30T23:59:59Z&tt=86400&rt=10000.0&b=-30,15,-45,30&platforms=1,2,3,4,5,6,7,8,9&parameter=sst&matchOne=true&depthMin=0&depthMax=5"

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