
"""-----------------------------------------------------------------------------
  Script Name: NEXUS Time Series
  Description: Creates time-series graph from
               a given dataset
  Created By:  Alice Yepremyan
  Date:        12/12/2019
-----------------------------------------------------------------------------"""

import arcpy
import json
import requests
import datetime
import matplotlib.pyplot as plt
import pandas as pd

arcpy.env.overwriteOutput = True

# Get the input parameters
host_url = arcpy.GetParameterAsText(0)
ds = arcpy.GetParameterAsText(1)
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
url = 'https://{}/timeSeriesSpark?ds={}&minLat={}&minLon={}&maxLat={}&maxLon={}&startTime={}&endTime={}'.format(host_url,ds, min_lat, min_lon, max_lat, max_lon, start_time, end_time)
#url = 'https://{}/timeSeriesSpark?ds=AVHRR_OI_L4_GHRSST_NCEI&minLat=45&minLon=-150&maxLat=60&maxLon=-120&startTime=2008-09-01T00:00:00Z&endTime=2015-10-01T23:59:59Z'
arcpy.AddMessage('{}'.format(url))

# Report a success message
arcpy.AddMessage("Url received, getting json")

ts = json.loads(str(requests.get(url).text))
arcpy.AddMessage('{}'.format(ts))
means = []
dates = []

# ToDo: should include a try except if no ts['data']
for data in ts['data']:
    means.append(data[0]['mean'])
    d = datetime.datetime.fromtimestamp((data[0]['time']))
    dates.append(d)

arcpy.AddMessage('This is the means: {}'.format(means))

# Plot the extracted means
plt.figure(figsize=(10, 5), dpi=100)
lines = plt.plot(dates, means)
plt.setp(lines, color='r', linewidth=1.0, linestyle='--',
         dash_capstyle='round', marker='.', markersize=5.0, mfc='r')
plt.grid(b=True, which='major', color='k', linestyle='--')
plt.xlim(dates[0], dates[-1])
plt.xlabel('Time')
plt.ylim(min(means), max(means))
plt.ylabel('Temperature (K)')
plt.title('Time Series')
plt.show()

