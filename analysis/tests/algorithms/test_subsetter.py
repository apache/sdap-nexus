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

import shutil
import tempfile
from os import listdir
from os.path import dirname, join, realpath, abspath, isfile

import pytest
from webservice.algorithms.doms.subsetter import SubsetResult


def test_to_csv(input_data):
    """
    Test that csv output contains the expected contents
    """
    result = SubsetResult(
        results=input_data,
        meta=None
    )

    csv_result = result.toCsv()

    expected_result_ascat = '''\
longitude,latitude,time,wind_speed,wind_to_direction\r
179.97830000000002,-27.6048,2018-09-24T09:10:03Z,0.32999998331069946,98.30000305175781\r
179.9642,-27.49519,2018-09-24T09:10:05Z,0.699999988079071,179.5'''

    expected_result_icoads = '''\
longitude,latitude,time,depth,platform_code,project,provider,sea_water_temperature,sea_water_temperature_quality\r
160.47,-28.38,2018-09-24T08:00:00Z,-99999.0,42,ICOADS Release 3.0,NCAR,18.6,1\r
179.88,-27.14,2018-09-24T08:00:00Z,-99999.0,42,ICOADS Release 3.0,NCAR,20.6,1'''

    assert 'ASCATB-L2-Coastal' in csv_result
    assert csv_result['ASCATB-L2-Coastal'] == expected_result_ascat

    assert 'ICOADS Release 3.0' in csv_result
    assert csv_result['ICOADS Release 3.0'] == expected_result_icoads


def test_to_zip(input_data, temp_dir):
    """
    Test that zip output contains the expected contents
    """
    result = SubsetResult(
        results=input_data,
        meta=None
    )

    zip_result = result.toZip()
    zip_location = join(temp_dir, 'response.zip')
    unzip_location = join(temp_dir, 'zip_contents')

    with open(zip_location, 'wb') as out_zip:
        out_zip.write(zip_result)

    shutil.unpack_archive(zip_location, unzip_location)

    zip_contents = [
        f for f in listdir(unzip_location) if isfile(join(unzip_location, f))
    ]

    assert len(zip_contents) == 2
    assert 'ASCATB-L2-Coastal.csv' in zip_contents
    assert 'ICOADS Release 3.0.csv' in zip_contents


@pytest.fixture
def temp_dir():
    test_dir = dirname(realpath(__file__))
    test_data_dir = abspath(join(test_dir, '..', 'data'))
    temp_data_dir = tempfile.mkdtemp(dir=test_data_dir)
    yield temp_data_dir
    shutil.rmtree(temp_data_dir)


@pytest.fixture
def input_data():
    data_dict = {
        "ASCATB-L2-Coastal": [
            {
                "latitude": -27.6048,
                "longitude": 179.97830000000002,
                "time": 1537780203,
                "data": {
                    "wind_speed": 0.32999998331069946,
                    "wind_to_direction": 98.30000305175781
                }
            },
            {
                "latitude": -27.49519,
                "longitude": 179.9642,
                "time": 1537780205,
                "data": {
                    "wind_speed": 0.699999988079071,
                    "wind_to_direction": 179.5
                }
            }
        ],
        "ICOADS Release 3.0": [
            {
                "latitude": -28.38,
                "longitude": 160.47,
                "time": 1537776000.0,
                "data": {
                    "depth": -99999.0,
                    "provider": "NCAR",
                    "project": "ICOADS Release 3.0",
                    "platform_code": "42",
                    "sea_water_temperature": 18.6,
                    "sea_water_temperature_quality": 1
                }
            },
            {
                "latitude": -27.14,
                "longitude": 179.88,
                "time": 1537776000.0,
                "data": {
                    "depth": -99999.0,
                    "provider": "NCAR",
                    "project": "ICOADS Release 3.0",
                    "platform_code": "42",
                    "sea_water_temperature": 20.6,
                    "sea_water_temperature_quality": 1
                }
            }
        ]
    }
    yield data_dict
