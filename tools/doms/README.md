# doms_reader.py
The functions in doms_reader.py read a DOMS netCDF file into memory, assemble a list of matches of satellite and in situ data, and optionally output the matches to a CSV file. Each matched pair contains one satellite data record and one in situ data record.

The DOMS netCDF files hold satellite data and in situ data in different groups (`SatelliteData` and `InsituData`). The `matchIDs` netCDF variable contains pairs of IDs (matches) which reference a satellite data record and an in situ data record in their respective groups. These records have a many-to-many relationship; one satellite record may match to many in situ records, and one in situ record may match to many satellite records. The `assemble_matches` function assembles the individual data records into pairs based on their `dim` group dimension IDs as paired in the `matchIDs` variable.

## Requirements
This tool was developed and tested with Python 2.7.5 and 3.7.0a0.
Imported packages:
* argparse
* netcdf4
* sys
* datetime
* csv
* collections
* logging
    

## Functions
### Function: `assemble_matches(filename)`
Read a DOMS netCDF file into memory and return a list of matches from the file.

#### Parameters 
- `filename` (str): the DOMS netCDF file name.
    
#### Returns
- `matches` (list): List of matches. 

Each list element in `matches` is a dictionary organized as follows:
    For match `m`, netCDF group `GROUP` ('SatelliteData' or 'InsituData'), and netCDF group variable `VARIABLE`:

`matches[m][GROUP]['matchID']`: netCDF `MatchedRecords` dimension ID for the match
`matches[m][GROUP]['GROUPID']`: GROUP netCDF `dim` dimension ID for the record
`matches[m][GROUP][VARIABLE]`: variable value 

For example, to access the timestamps of the satellite data and the in situ data of the first match in the list, along with the `MatchedRecords` dimension ID and the groups' `dim` dimension ID:
```python
matches[0]['SatelliteData']['time']
matches[0]['InsituData']['time']
matches[0]['SatelliteData']['matchID']
matches[0]['SatelliteData']['SatelliteDataID']
matches[0]['InsituData']['InsituDataID']
```

        
### Function: `matches_to_csv(matches, csvfile)`
Write the DOMS matches to a CSV file. Include a header of column names which are based on the group and variable names from the netCDF file.
    
#### Parameters:
- `matches` (list): the list of dictionaries containing the DOMS matches as returned from the `assemble_matches` function.
- `csvfile` (str): the name of the CSV output file.

## Usage
For example, to read some DOMS netCDF file called `doms_file.nc`:
### Command line
The main function for `doms_reader.py` takes one `filename` parameter (`doms_file.nc` argument in this example) for the DOMS netCDF file to read, calls the `assemble_matches` function, then calls the `matches_to_csv` function to write the matches to a CSV file `doms_matches.csv`.
```
python doms_reader.py doms_file.nc
```
```
python3 doms_reader.py doms_file.nc
```
### Importing `assemble_matches`
```python
from doms_reader import assemble_matches
matches = assemble_matches('doms_file.nc')
```
