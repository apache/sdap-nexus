# SDAP ArcGIS Tools

Toolbox and scripts for utilizing SDAP analytics within ArcGIS

## Contents

`zipped_toolbox`: Contains the python scripts and the toolbox. 

`nexus_toolbox_embedded.tbx`: toolbox which contains the SDAP scripts embedded into the toolbox. 

## Usage

### Development
For development please use the `zipped_toolbox`. You may make your changes directly to the python scripts

Once you have downloaded the toolbox with the script you can upload the toolbox into ArcGIS pro. 
Please follow the instructions written https://pro.arcgis.com/en/pro-app/help/projects/connect-to-a-toolbox.htm to connect a toolbox.

When your toolbox is inside of your ArcGIS project, you can then proceed to utilize the tool. 

To make changes to the GUI located in the catalog window on the right of your ArcGIS project, you will right click on 
the script within the toolbox, ie `DailyDifferenceAverage`, and you will click on `properties`. A window will pop up. 
On the left column of the window you will see `Validation`. Click on `Validation`. There you can manipulate the code to 
customize the GUI and the parameters. You may also set parameters in the `parameters` pane within the `properties` window.

#### Known Issues
If you encounter any issues using the tool this can be because it is not pointing to the correct location of the script. 
To fix this, you will right click on the script within the toolbox, ie `DailyDifferenceAverage`, and you will click on 
`properties`. A window will pop and you will see a path pointing to the `daily_difference_average.py`. Ensure that this 
is the correct path for the script. 

When creating an interactive polygon in the toolbox, click on the pencil, create your shape within the ArcGIS pro map,
and double click on the red dot when finished drawing the shape. You may also right click on the final red dot and click
Finish.

### User
For usage purposes please use `nexus_toolbox_embedded.tbx`. This toolbox has the python scripts embedded within which
means that you cannot change the underlying script codes for the tools. To use, you can simply make a connection to this 
toolbox (after downloading) in the Catalog by right clicking Toolboxes > Add Toolbox > and identifying `nexus_toolbox_embedded.tbx`. 
If you are a developer, please refer to the section above.

