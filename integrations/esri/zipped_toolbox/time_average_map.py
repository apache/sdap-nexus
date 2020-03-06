
"""-----------------------------------------------------------------------------
  Script Name: NEXUS Time average map
  Description: Creates time-average graph from
               a given dataset
  Created By:  Alice Yepremyan
  Date:        1/20/2020
-----------------------------------------------------------------------------"""

import arcpy
import json
import requests
import datetime
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd

arcpy.env.overwriteOutput = True

# Get the input parameters
# ToDO: make lat/time interactive
ds = arcpy.GetParameterAsText(0)
minLat = arcpy.GetParameterAsText(1)
maxLat = arcpy.GetParameterAsText(2)
minLon = arcpy.GetParameterAsText(3)
maxLon = arcpy.GetParameterAsText(4)
startTime = pd.to_datetime(arcpy.GetParameterAsText(5)).strftime('%Y-%m-%dT%H:%M:%SZ')
endTime = pd.to_datetime(arcpy.GetParameterAsText(6)).strftime('%Y-%m-%dT%H:%M:%SZ')

# Build the HTTP request
# ToDO: make host url a parameter
host = "https://oceanworks.jpl.nasa.gov"
request = "{}/timeAvgMapSpark?ds={}&startTime={}&endTime={}&minLon={}&minLat={}&maxLon={}&maxLat={}&spark=local,16,32" \
         .format(host, ds, startTime, endTime, minLon, minLat, maxLon, maxLat)
#request = 'https://oceanworks.jpl.nasa.gov/timeAvgMapSpark?local,16,32&ds=AVHRR_OI_L4_GHRSST_NCEI&minLat=-5&minLon=-170&maxLat=5&maxLon=-120&startTime=1356998400&endTime=1383091200'
arcpy.AddMessage('{}'.format(request))

# Report a success message
arcpy.AddMessage("Url received, getting json")

# Send request to server
response = requests.get(request).json()
arcpy.AddMessage('{}'.format(response))

# Parse the response and create an image
lons = [point['lon'] for point in response['data'][0]]
lats = [a_list[0]['lat'] for a_list in response['data']]

my_list = numpy.ndarray((len(lats), len(lons)))
for x in range(0, len(lats)):
    for y in range(0, len(lons)):
        my_list[x][y] = response['data'][x][y]['mean']

norm = mpl.colors.Normalize(vmin=my_list.min(), vmax=my_list.max())

# Plot the time average
fig, ax1 = plt.subplots(figsize=(10, 5), dpi=100)
ax1.pcolormesh(lons, lats, my_list, vmin=my_list.min(), vmax=my_list.max(), cmap='gist_rainbow')
plt.title('Time Average Map')
plt.show()
