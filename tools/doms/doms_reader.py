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

import argparse
from netCDF4 import Dataset, num2date
import sys
import datetime
import csv
from collections import OrderedDict
import logging

LOGGER = logging.getLogger("doms_reader")

def assemble_matches(filename):
    """
    Read a DOMS netCDF file and return a list of matches.
    
    Arguments: 
        filename (string): the DOMS netCDF file name.
    
    Returns:
        matches (list): List of matches. Each list element is a dictionary:
            For netCDF group GROUP (SatelliteData or InsituData) and group
                variable VARIABLE:
            matches[GROUP]['matchID']: MatchedRecords dimension ID for the match
            matches[GROUP]['GROUPID']: GROUP dim dimension ID for the record
            matches[GROUP][VARIABLE]: variable value 
    """
    # Open the netCDF file.
    try:
        doms_nc = Dataset(filename, 'r')
    except (OSError, IOError) as err:
        LOGGER.exception("Error reading netCDF file " + filename)
        raise err
    
    # Check that the number of groups is consistent w/ MatchedGroups dimension
    assert len(doms_nc.groups) == doms_nc.dimensions['MatchedGroups'].size, \
        ("Number of groups isn't the same as MatchedGroups dimension.")
    
    matches = []
    matched_records = doms_nc.dimensions['MatchedRecords'].size

    # Loop through the match IDs to assemble matches
    for match in range(0, matched_records):
        match_dict = OrderedDict()
        # Grab the data from each platform (group) in the match
        for group_num, group in enumerate(doms_nc.groups):
            match_dict[group] = OrderedDict()
            match_dict[group]['matchID'] = match
            ID = doms_nc.variables['matchIDs'][match][group_num]
            match_dict[group][group + 'ID'] = ID
            for variable in doms_nc.groups[group].variables.keys():
                match_dict[group][variable] = doms_nc.groups[group][variable][ID]
            
            # Create a UTC datetime field from timestamp
            dt = num2date(match_dict[group]['time'],
                          doms_nc.groups[group]['time'].units)
            match_dict[group]['datetime'] = dt
            print(match_dict)
        matches.append(match_dict)
    
    doms_nc.close()
    
    return matches
    
def matches_to_csv(matches, filename):
    """
    Write the DOMS matches to a CSV file. Include a header of column names
    which are based on the group and variable names from the netCDF file.
    
    Arguments:
        matches (list): the list of dictionaries containing the DOMS matches as
            returned from assemble_matches.
            
        filename (string): the name of the CSV output file.
    """
    header = []
    for key, value in matches[0].items():
        for otherkey in value.keys():
            header.append(key + "_" + otherkey)
    
    with open(filename, 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(header)
        for match in matches:
            row = []
            for group, data in match.items():
                for value in data.values():
                    row.append(value)
            csv_writer.writerow(row)

if __name__ == '__main__':
    """
    Execution:
        python doms_reader.py filename
        OR
        python3 doms_reader.py filename
    """
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

    p = argparse.ArgumentParser()
    p.add_argument('filename', help='DOMS netCDF file to read')
    args = p.parse_args()

    doms_matches = assemble_matches(args.filename)

    matches_to_csv(doms_matches, 'matches_to_csv.csv')
    
    
    
    
    
    
    
    

    
    