
"""-----------------------------------------------------------------------------
  Script Name: NEXUS Daily Difference Graph
  Description: Creates daily difference graph from
               a given dataset
  Created By:  Alice Yepremyan
  Date:        1/28/2020
-----------------------------------------------------------------------------"""

import arcpy
import json
import requests
import datetime
import matplotlib.pyplot as plt
import pandas as pd

arcpy.env.overwriteOutput = True

# Get the input parameters
# ToDO: make lat/time interactive
ds = arcpy.GetParameterAsText(0)
ds2 = arcpy.GetParameterAsText(1)
minLat = arcpy.GetParameterAsText(2)
maxLat = arcpy.GetParameterAsText(3)
minLon = arcpy.GetParameterAsText(4)
maxLon = arcpy.GetParameterAsText(5)
startTime = pd.to_datetime(arcpy.GetParameterAsText(6)).strftime('%Y-%m-%dT%H:%M:%SZ')
endTime = pd.to_datetime(arcpy.GetParameterAsText(7)).strftime('%Y-%m-%dT%H:%M:%SZ')

# Build the HTTP request
url = 'https://oceanworks.jpl.nasa.gov/dailydifferenceaverage_spark?dataset={}&climatology={}&b={},{},{},{}&startTime={}&endTime={}'.format(ds, ds2, minLat, minLon, maxLat, maxLon, startTime, endTime)
#url = 'https://oceanworks.jpl.nasa.gov/dailydifferenceaverage_spark?dataset=SMAP_SSS_L3_MONTHLY_500&climatology=SMAP_SSS_L3_MONTHLY_500_CLIM&b=-150,45,-120,60&startTime=2015-04-16T00:00:00Z&endTime=2018-04-14T00:00:00Z'
arcpy.AddMessage('{}'.format(url))

# Report a success message
arcpy.AddMessage("Url received, getting json")

# Send request to server
response = str(requests.get(url, verify=False).text)
dda_amce = json.loads(response)
arcpy.AddMessage('{}'.format(response))

means = []
dates = []
st_ds = []

# Sampling every 30th data point to reduce plot noise
for i, data in enumerate(dda_amce['data']):
    means.append(data[0]['mean'])
    dates.append(datetime.datetime.fromtimestamp((data[0]['time'])))
    st_ds.append(data[0]['std'])

# Plot the extracted means
plt.figure(figsize=(10, 5), dpi=100)
lines = plt.errorbar(dates, means, st_ds)
plt.xlim(dates[0], dates[-1])
plt.xlabel('Time', fontsize=16)
plt.ylim(min(means)-1, max(means)+1)
plt.ylabel('Temperature Difference (K)', fontsize=16)
plt.title('Temperature Difference from Average (K) with Standard Deviation Error Bars', fontsize=18)
plt.show()
