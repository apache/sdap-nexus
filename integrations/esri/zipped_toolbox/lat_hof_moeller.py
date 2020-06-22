
"""-----------------------------------------------------------------------------
  Script Name: NEXUS Latitude/Time HofMoeller Spark
  Description: Computes a latitude/time HofMoeller plot given an arbitrary
               geographical area and time range
  Created By:  Alice Yepremyan
  Date:        2/18/2020
-----------------------------------------------------------------------------"""

import arcpy
import json
import requests
import datetime
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib import cm
import numpy as np
import pandas as pd

arcpy.env.overwriteOutput = True

def createHoffmueller(data, coordSeries, timeSeries, coordName, title, interpolate='nearest'):

    cmap = cm.coolwarm
    # ls = LightSource(315, 45)
    # rgb = ls.shade(data, cmap)

    fig, ax = plt.subplots()
    fig.set_size_inches(11.0, 8.5)
    cax = ax.imshow(data, interpolation=interpolate, cmap=cmap)

    def yFormatter(y, pos):
        if y < len(coordSeries):
            return "%s $^\circ$" % (int(coordSeries[int(y)] * 100.0) / 100.)
        else:
            return ""

    def xFormatter(x, pos):
        if x < len(timeSeries):
            return timeSeries[int(x)].strftime('%b %Y')
        else:
            return ""

    ax.xaxis.set_major_formatter(FuncFormatter(xFormatter))
    ax.yaxis.set_major_formatter(FuncFormatter(yFormatter))

    ax.set_title(title)
    ax.set_ylabel(coordName)
    ax.set_xlabel('Date')

    fig.colorbar(cax)
    fig.autofmt_xdate()

    plt.show()

def createLatitudeHoffmueller(res, meta):
    latSeries = [m['latitude'] for m in res[0]['lats']]
    timeSeries = [datetime.datetime.fromtimestamp(m['time']) for m in res]

    data = np.zeros((len(latSeries), len(timeSeries)))

    for t in range(0, len(timeSeries)):
        timeSet = res[t]
        for l in range(0, len(latSeries)):
            latSet = timeSet['lats'][l]
            value = latSet['mean']
            data[len(latSeries) - l - 1][t] = value

    title = meta['shortName']
    dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

    return createHoffmueller(data, latSeries, timeSeries, "Latitude", title="%s\n%s" % (title, dateRange),
                             interpolate='nearest')

# Get the input parameters
host_url = arcpy.GetParameterAsText(0)
dataset = arcpy.GetParameterAsText(1)
input_feature = arcpy.GetParameter(2)
start_time = pd.to_datetime(arcpy.GetParameterAsText(3)).strftime('%Y-%m-%dT%H:%M:%SZ')
end_time = pd.to_datetime(arcpy.GetParameterAsText(4)).strftime('%Y-%m-%dT%H:%M:%SZ')

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
url = f"https://{host_url}/latitudeTimeHofMoellerSpark?ds={dataset}&b={max_lat},{min_lon},{min_lat},{max_lon}&startTime={start_time}&endTime={end_time}"
#url = "https://{}/latitudeTimeHofMoellerSpark?ds=TELLUS_GRACE_MASCON_CRI_GRID_RL06_V1_OCEAN&b=-30,15,-45,30&startTime=2010-02-01T00:00:00Z&endTime=2014-01-01T00:00:00Z"
arcpy.AddMessage('{}'.format(url))

# Report a success message
arcpy.AddMessage("Url received, getting json")

ts = json.loads(str(requests.get(url).text))

# Plot Hoffmueller
createLatitudeHoffmueller(ts["data"], ts["meta"])

