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

import copy
import csv
import io
import os
import warnings
from datetime import datetime
from math import cos, radians
from tempfile import NamedTemporaryFile as Temp
from urllib.parse import urljoin
from zipfile import ZipFile

import pandas as pd
import pytest
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from pytz import timezone, UTC
from shapely import wkt
from shapely.geometry import Polygon, Point, box

import cdms_reader


#########################
#
# export TEST_HOST=http://localhost:8083/
# unset TEST_HOST
#
#########################


@pytest.fixture()
def host():
    return os.getenv('TEST_HOST', 'http://doms.jpl.nasa.gov')


@pytest.fixture()
def insitu_endpoint():
    return os.getenv(
        'INSITU_ENDPOINT',
        'http://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination'
    )


@pytest.fixture()
def insitu_swagger_endpoint():
    return os.getenv(
        'INSITU_SWAGGER_ENDPOINT',
        'http://doms.jpl.nasa.gov/insitu/1.0/insitu_query_swagger/'
    )


@pytest.fixture(scope="module")
def eid():
    return {
        'successful': False,
        'eid': [],
        'params': []
    }


def skip(msg=""):
    raise pytest.skip(msg)


def b_to_polygon(b):
    west, south, east, north = [float(p) for p in b.split(",")]
    polygon = Polygon([(west, south), (east, south), (east, north), (west, north), (west, south)])
    return polygon


def iso_time_to_epoch(str_time):
    EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

    return (datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=UTC) - EPOCH).total_seconds()


def expand_by_tolerance(point, rt):
    def add_meters_to_lon_lat(point, meters):
        lon = point.x
        lat = point.y

        longitude = lon + ((meters / 111111) * cos(radians(lat)))
        latitude = lat + (meters / 111111)

        return longitude, latitude

    min_lon, min_lat = add_meters_to_lon_lat(point, -1 * rt)
    max_lon, max_lat = add_meters_to_lon_lat(point, rt)

    return box(min_lon, min_lat, max_lon, max_lat)


def translate_global_rows(rows):
    translated = {}

    for row in rows:
        parts = row.split(',', 1)
        translated[parts[0]] = parts[1]

    return translated


def translate_matchup_rows(rows):
    headers = rows[0].split(',')

    translated_rows = []

    for row in rows[1:]:
        translated_row = {}

        buf = io.StringIO(row)
        reader = csv.reader(buf)
        fields = list(reader)[0]

        assert len(headers) == len(fields)

        for i, field in enumerate(fields):
            header = headers[i]

            if header not in translated_row:
                translated_row[header] = field
            else:
                translated_row[f"{header}_secondary"] = field

        translated_rows.append(translated_row)

    return translated_rows


def lat_lon_to_point(lat, lon):
    return wkt.loads(f"Point({lon} {lat})")


def format_time(timestamp):
    t = parse(timestamp)

    ISO_8601 = '%Y-%m-%dT%H:%M:%SZ'

    return t.strftime(ISO_8601)


def verify_match(match, point, time, s_point, s_time, params, bounding_poly):
    # Check primary point is as expected
    assert match['point'] == point
    assert match['time'] == time

    # Check primary point within search bounds
    assert iso_time_to_epoch(params['startTime']) \
           <= match['time'] \
           <= iso_time_to_epoch(params['endTime'])
    assert bounding_poly.contains(wkt.loads(match['point']))

    secondary = match['matches'][0]

    # Check secondary point is as expected
    assert secondary['point'] == s_point
    assert secondary['time'] == s_time

    # Check secondary point within specified spatial & temporal tolerances for matched primary
    assert expand_by_tolerance(
        wkt.loads(match['point']),
        params['rt']
    ).contains(wkt.loads(secondary['point']))

    assert (match['time'] - params['tt']) \
           <= secondary['time'] \
           <= (match['time'] + params['tt'])


@pytest.mark.integration
def test_matchup_spark(host, eid):
    url = urljoin(host, 'match_spark')

    params = {
        "primary": "MUR25-JPL-L4-GLOB-v04.2",
        "startTime": "2018-08-01T09:00:00Z",
        "endTime": "2018-09-01T00:00:00Z",
        "tt": 43200,
        "rt": 1000,
        "b": "-100,20,-79,30",
        "depthMin": -20,
        "depthMax": 10,
        "matchOnce": True,
        "secondary": "ICOADS Release 3.0",
        "resultSizeLimit": 7000,
        "platforms": "42"
    }

    response = requests.get(url, params=params)

    assert response.status_code == 200

    bounding_poly = b_to_polygon(params['b'])

    body = response.json()
    data = body['data']

    assert body['count'] == len(data)

    data.sort(key=lambda e: e['point'])
    body['data'] = data

    eid['eid'].append(body['executionId'])
    eid['params'].append(copy.deepcopy(params))

    verify_match(
        data[0],    'Point(-86.125 27.625)',
        1535360400, 'Point(-86.13 27.63)',
        1535374800,  params, bounding_poly
    )

    verify_match(
        data[1],    'Point(-90.125 27.625)',
        1534496400, 'Point(-90.13 27.63)',
        1534491000,  params, bounding_poly
    )

    verify_match(
        data[2],    'Point(-90.125 28.125)',
        1534928400, 'Point(-90.13 28.12)',
        1534899600,  params, bounding_poly
    )

    verify_match(
        data[3],    'Point(-90.375 28.125)',
        1534842000, 'Point(-90.38 28.12)',
        1534813200,  params, bounding_poly
    )

    params['primary'] = 'JPL-L4-MRVA-CHLA-GLOB-v3.0'

    response = requests.get(url, params=params)

    assert response.status_code == 200

    body = response.json()

    data = body['data']

    assert body['count'] == len(data)

    data.sort(key=lambda e: e['point'])
    body['data'] = data

    eid['eid'].append(body['executionId'])
    eid['params'].append(copy.deepcopy(params))

    verify_match(
        data[0],    'Point(-86.125 27.625)',
        1535371200, 'Point(-86.13 27.63)',
        1535374800,  params, bounding_poly
    )

    verify_match(
        data[1],    'Point(-90.125 27.625)',
        1534507200, 'Point(-90.13 27.63)',
        1534491000,  params, bounding_poly
    )

    verify_match(
        data[2],    'Point(-90.125 28.125)',
        1534939200, 'Point(-90.13 28.12)',
        1534899600,  params, bounding_poly
    )

    verify_match(
        data[3],    'Point(-90.375 28.125)',
        1534852800, 'Point(-90.38 28.12)',
        1534813200,  params, bounding_poly
    )

    eid['successful'] = True


@pytest.mark.integration
def test_domsresults_json(host, eid):
    url = urljoin(host, 'domsresults')

    # Skip the test automatically if the matchup request was not successful
    if not eid['successful']:
        skip('Matchup request was unsuccessful so there are no results to get from domsresults')

    def fetch_result(eid, output):
        return requests.get(url, params={"id": eid, "output": output})

    eids = eid['eid']
    param_list = eid['params']

    response = fetch_result(eids[0], "JSON")

    assert response.status_code == 200

    body = response.json()

    data = body['data']
    assert len(data) == 4

    for m in data:
        m['point'] = f"Point({m['lon']} {m['lat']})"
        for s in m['matches']:
            s['point'] = f"Point({s['lon']} {s['lat']})"

    data.sort(key=lambda e: e['point'])

    params = param_list[0]
    bounding_poly = b_to_polygon(params['b'])

    verify_match(data[0],    'Point(-86.125 27.625)',
                 1535360400, 'Point(-86.13 27.63)',
                 1535374800, params, bounding_poly
                 )

    verify_match(data[1],    'Point(-90.125 27.625)',
                 1534496400, 'Point(-90.13 27.63)',
                 1534491000,  params, bounding_poly
                 )

    verify_match(data[2],    'Point(-90.125 28.125)',
                 1534928400, 'Point(-90.13 28.12)',
                 1534899600,  params, bounding_poly
                 )

    verify_match(data[3],    'Point(-90.375 28.125)',
                 1534842000, 'Point(-90.38 28.12)',
                 1534813200,  params, bounding_poly
                 )

    response = fetch_result(eids[1], "JSON")

    assert response.status_code == 200

    body = response.json()

    data = body['data']
    assert len(data) == 4

    for m in data:
        m['point'] = f"Point({m['lon']} {m['lat']})"
        for s in m['matches']:
            s['point'] = f"Point({s['lon']} {s['lat']})"

    data.sort(key=lambda e: e['point'])

    params = param_list[1]
    bounding_poly = b_to_polygon(params['b'])

    verify_match(data[0],    'Point(-86.125 27.625)',
                 1535371200, 'Point(-86.13 27.63)',
                 1535374800,  params, bounding_poly
                 )

    verify_match(data[1],    'Point(-90.125 27.625)',
                 1534507200, 'Point(-90.13 27.63)',
                 1534491000,  params, bounding_poly
                 )

    verify_match(data[2],    'Point(-90.125 28.125)',
                 1534939200, 'Point(-90.13 28.12)',
                 1534899600,  params, bounding_poly
                 )

    verify_match(data[3],    'Point(-90.375 28.125)',
                 1534852800, 'Point(-90.38 28.12)',
                 1534813200,  params, bounding_poly
                 )


@pytest.mark.integration
def test_domsresults_csv(host, eid):
    url = urljoin(host, 'domsresults')

    # Skip the test automatically if the matchup request was not successful
    if not eid['successful']:
        skip('Matchup request was unsuccessful so there are no results to get from domsresults')

    def fetch_result(eid, output):
        return requests.get(url, params={"id": eid, "output": output})

    eids = eid['eid']
    param_list = eid['params']

    response = fetch_result(eids[0], "CSV")
    params = param_list[0]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    rows = response.text.split('\r\n')
    index = rows.index('')

    global_rows = rows[:index]
    matchup_rows = rows[index + 1:-1]  # Drop trailing empty string from trailing newline

    global_rows = translate_global_rows(global_rows)
    matchup_rows = translate_matchup_rows(matchup_rows)

    assert len(matchup_rows) == int(global_rows['CDMS_num_primary_matched'])

    for row in matchup_rows:
        primary_point = lat_lon_to_point(row['lat'], row['lon'])

        assert bounding_poly.contains(primary_point)
        assert params['startTime'] <= format_time(row['time']) <= params['endTime']

        secondary_point = lat_lon_to_point(row['lat_secondary'], row['lon_secondary'])

        assert expand_by_tolerance(primary_point, params['rt']).contains(secondary_point)
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= iso_time_to_epoch(format_time(row['time_secondary'])) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])

    response = fetch_result(eids[1], "CSV")
    params = param_list[1]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    rows = response.text.split('\r\n')
    index = rows.index('')

    global_rows = rows[:index]
    matchup_rows = rows[index + 1:-1]  # Drop trailing empty string from trailing newline

    global_rows = translate_global_rows(global_rows)
    matchup_rows = translate_matchup_rows(matchup_rows)

    assert len(matchup_rows) == int(global_rows['CDMS_num_primary_matched'])

    for row in matchup_rows:
        primary_point = lat_lon_to_point(row['lat'], row['lon'])

        assert bounding_poly.contains(primary_point)
        assert params['startTime'] <= format_time(row['time']) <= params['endTime']

        secondary_point = lat_lon_to_point(row['lat_secondary'], row['lon_secondary'])

        assert expand_by_tolerance(primary_point, params['rt']).contains(secondary_point)
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= iso_time_to_epoch(format_time(row['time_secondary'])) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])


@pytest.mark.integration
@pytest.mark.xfail
def test_domsresults_netcdf(host, eid):
    warnings.filterwarnings('ignore')

    url = urljoin(host, 'domsresults')

    # Skip the test automatically if the matchup request was not successful
    if not eid['successful']:
        skip('Matchup request was unsuccessful so there are no results to get from domsresults')

    def fetch_result(eid, output):
        return requests.get(url, params={"id": eid, "output": output})

    eids = eid['eid']
    param_list = eid['params']

    temp_file = Temp(mode='wb+', suffix='.csv.tmp', prefix='CDMSReader_')

    response = fetch_result(eids[0], "NETCDF")
    params = param_list[0]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    temp_file.write(response.content)
    temp_file.flush()
    temp_file.seek(0)

    matches = cdms_reader.assemble_matches(temp_file.name)

    cdms_reader.matches_to_csv(matches, temp_file.name)

    with open(temp_file.name) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        primary_point = lat_lon_to_point(row['PrimaryData_lat'], row['PrimaryData_lon'])

        assert bounding_poly.contains(primary_point)
        assert iso_time_to_epoch(params['startTime']) \
               <= float(row['PrimaryData_time']) \
               <= iso_time_to_epoch(params['endTime'])

        secondary_point = lat_lon_to_point(row['SecondaryData_lat'], row['SecondaryData_lon'])

        assert expand_by_tolerance(primary_point, params['rt']).contains(secondary_point)
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= float(row['SecondaryData_time']) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])

    response = fetch_result(eids[1], "NETCDF")
    params = param_list[1]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    temp_file.write(response.content)
    temp_file.flush()
    temp_file.seek(0)

    matches = cdms_reader.assemble_matches(temp_file.name)

    cdms_reader.matches_to_csv(matches, temp_file.name)

    with open(temp_file.name) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        primary_point = lat_lon_to_point(row['PrimaryData_lat'], row['PrimaryData_lon'])

        assert bounding_poly.contains(primary_point)
        assert iso_time_to_epoch(params['startTime']) \
               <= float(row['PrimaryData_time']) \
               <= iso_time_to_epoch(params['endTime'])

        secondary_point = lat_lon_to_point(row['SecondaryData_lat'], row['SecondaryData_lon'])

        assert expand_by_tolerance(primary_point, params['rt']).contains(secondary_point)
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= float(row['SecondaryData_time']) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])

    temp_file.close()
    warnings.filterwarnings('default')


@pytest.mark.integration
def test_domslist(host):
    url = urljoin(host, 'domslist')

    response = requests.get(url)

    assert response.status_code == 200

    body = response.json()

    data = body['data']

    num_satellite = len(data['satellite'])
    num_insitu = len(data['insitu'])

    assert num_insitu > 0
    assert num_satellite > 0

    # assert body['count'] == num_satellite + num_insitu


@pytest.mark.integration
def test_cdmssubset(host):
    url = urljoin(host, 'cdmssubset')

    params = {
        "dataset": "MUR25-JPL-L4-GLOB-v04.2",
        "parameter": "sst",
        "startTime": "2018-09-24T00:00:00Z",
        "endTime": "2018-09-30T00:00:00Z",
        "b": "160,-30,180,-25",
        "output": "ZIP"
    }

    response = requests.get(url, params=params)

    assert response.status_code == 200

    bounding_poly = b_to_polygon(params['b'])

    response_buf = io.BytesIO(response.content)

    with ZipFile(response_buf) as data:
        namelist = data.namelist()

        assert namelist == ['MUR25-JPL-L4-GLOB-v04.2.csv']

        csv_buf = io.StringIO(data.read(namelist[0]).decode('utf-8'))
        csv_data = pd.read_csv(csv_buf)

    def validate_row_bounds(row):
        assert bounding_poly.contains(Point(row['longitude'], row['latitude']))
        assert params['startTime'] <= row['time'] <= params['endTime']

    for i in range(0, len(csv_data)):
        validate_row_bounds(csv_data.iloc[i])

    params['dataset'] = 'OISSS_L4_multimission_7day_v1'

    response = requests.get(url, params=params)

    assert response.status_code == 200

    response_buf = io.BytesIO(response.content)

    with ZipFile(response_buf) as data:
        namelist = data.namelist()

        assert namelist == ['OISSS_L4_multimission_7day_v1.csv']

        csv_buf = io.StringIO(data.read(namelist[0]).decode('utf-8'))
        csv_data = pd.read_csv(csv_buf)

    for i in range(0, len(csv_data)):
        validate_row_bounds(csv_data.iloc[i])


@pytest.mark.integration
def test_insitu(insitu_endpoint):
    params = {
        'itemsPerPage': 1000,
        'startTime': '2018-05-15T00:00:00Z',
        'endTime': '2018-06-01T00:00:00Z',
        'bbox': '-80,25,-75,30',
        'minDepth': 0.0,
        'maxDepth': 5.0,
        'provider': 'NCAR',
        'project': 'ICOADS Release 3.0',
        'platform': '42',
        'markerTime': '2018-05-15T00:00:00Z'
    }

    response = requests.get(insitu_endpoint, params=params)

    assert response.status_code == 200

    body = response.json()

    if body['total'] <= params['itemsPerPage']:
        assert body['total'] == len(body['results'])
    else:
        assert len(body['results']) == params['itemsPerPage']

    bounding_poly = b_to_polygon(params['bbox'])

    for result in body['results']:
        assert bounding_poly.contains(
            wkt.loads(f"Point({result['longitude']} {result['latitude']})")
        )

        if result['depth'] != -99999.0:
            assert params['minDepth'] <= result['depth'] <= params['maxDepth']

        assert params['startTime'] <= result['time'] <= params['endTime']


@pytest.mark.integration
def test_swaggerui_sdap(host):
    url = urljoin(host, 'apidocs/')

    response = requests.get(url)

    assert response.status_code == 200
    assert 'swagger-ui' in response.text

    try:
        # There's probably a better way to do this, but extract the .yml file for the docs from the returned text
        soup = BeautifulSoup(response.text, 'html.parser')

        script = str([tag for tag in soup.find_all('script') if tag.attrs == {}][0])

        start_index = script.find('url:')
        end_index = script.find('",\n', start_index)

        script = script[start_index:end_index]

        yml_filename = script.split('"')[1]

        url = urljoin(url, yml_filename)

        response = requests.get(url)

        assert response.status_code == 200
    except AssertionError:
        raise
    except:
        try:
            url = urljoin(url, 'openapi.yml')

            response = requests.get(url)

            assert response.status_code == 200

            warnings.warn("Could not extract documentation yaml filename from response text, "
                          "but using an assumed value worked successfully")
        except:
            raise ValueError("Could not verify documentation yaml file, assumed value also failed")


@pytest.mark.integration
def test_swaggerui_insitu(insitu_swagger_endpoint):
    response = requests.get(insitu_swagger_endpoint)

    assert response.status_code == 200
    assert 'swagger-ui' in response.text

    try:
        # There's probably a better way to do this, but extract the .yml file for the docs from the returned text
        soup = BeautifulSoup(response.text, 'html.parser')

        script = str([tag for tag in soup.find_all('script') if tag.attrs == {}][0])

        start_index = script.find('url:')
        end_index = script.find('",\n', start_index)

        script = script[start_index:end_index]

        yml_filename = script.split('"')[1]

        url = urljoin(insitu_swagger_endpoint, yml_filename)

        response = requests.get(url)

        assert response.status_code == 200
    except AssertionError:
        raise
    except:
        try:
            url = urljoin(insitu_swagger_endpoint, 'insitu-spec-0.0.1.yml')

            response = requests.get(url)

            assert response.status_code == 200

            warnings.warn("Could not extract documentation yaml filename from response text, "
                          "but using an assumed value worked successfully")
        except:
            raise ValueError("Could not verify documentation yaml file, assumed value also failed")
