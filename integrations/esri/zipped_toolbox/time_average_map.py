
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
request = "https://{}/timeAvgMapSpark?ds={}&startTime={}&endTime={}&minLon={}&minLat={}&maxLon={}&maxLat={}" \
         .format(host_url, ds, start_time, end_time, min_lon, min_lat, max_lon, max_lat)
#request = 'https://{}/timeAvgMapSpark?ds=AVHRR_OI_L4_GHRSST_NCEI&minLat=-5&minLon=-170&maxLat=5&maxLon=-120&startTime=1356998400&endTime=1383091200'
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
