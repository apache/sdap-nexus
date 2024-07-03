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
import datetime
import io
import itertools
import json
import os
import re
import warnings
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile as Temp
from time import sleep
from urllib.parse import urljoin, urlparse, urlunparse
from zipfile import ZipFile

import pandas as pd
import pytest
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from geopy.distance import geodesic
from pytz import timezone, UTC
from requests.exceptions import ConnectTimeout
from shapely import wkt
from shapely.geometry import Polygon, Point

import cdms_reader


#########################
#
# export TEST_HOST=http://localhost:8083/
# unset TEST_HOST
#
#########################


@pytest.fixture(scope="session")
def host():
    return os.getenv('TEST_HOST', 'http://localhost:8083/')


@pytest.fixture(scope="session")
def eid():
    return {
        'successful': False,
        'eid': [],
        'params': []
    }


start_time = None


@pytest.fixture(scope="session")
def start():
    global start_time

    if start_time is None:
        start_time = datetime.now().strftime("%G%m%d%H%M%S%z")

    return start_time


@pytest.fixture()
def timeouts():
    connect_timeout = 9.05  # Recommended to be just above a multiple of 3 seconds
    read_timeout = 303  # Just above current gateway timeout
    timeouts = (connect_timeout, read_timeout)

    return timeouts


@pytest.fixture()
def fail_on_miscount(request):
    return request.config.getoption('--matchup-warn-on-miscount', default=False)


@pytest.fixture(scope='session')
def distance_vs_time_query(host, start):
    result = {
        'distances': {  # Tuples: sec_lat, sec_lon, sec_time
            'min_dist': (),
            'min_time': ()
        },
        'backup': {  # Tuples: sec_lat, sec_lon, sec_time
            'min_dist': ("26.6141296", "-130.0827904", 1522637640),
            'min_time': ("26.6894016", "-130.0547072", 1522626840)
        },
        'success': False
    }

    url = urljoin(host, 'match_spark')

    params = {
        "primary": "JPL-L4-MRVA-CHLA-GLOB-v3.0",
        "secondary": "shark-2018",
        "startTime": "2018-04-01T00:00:00Z",
        "endTime": "2018-04-01T23:59:59Z",
        "b": "-131,26,-130,27",
        "depthMin": -5,
        "depthMax": 5,
        "tt": 86400,
        "rt": 10000,
        "matchOnce": False,
        "resultSizeLimit": 0,
        "platforms": "3B",
        "parameter": "mass_concentration_of_chlorophyll_in_sea_water",
    }

    try:
        body = run_matchup(url, params)

        data = body['data']

        assert body['count'] == len(data)
        check_count(len(data), 1, True)

        primary_point = data[0]

        def compute_distance(primary, secondary):
            return geodesic((primary['lat'], primary['lon']), (secondary['lat'], secondary['lon'])).m

        def compute_time(primary, secondary):
            return abs(primary['time'] - secondary['time'])

        distances = [
            (s['lat'], s['lon'], s['time'], compute_distance(primary_point, s), compute_time(primary_point, s))
            for s in primary_point['matches']
        ]

        try_save('computed_distances', start, distances)

        min_dist = min(distances, key=lambda x: x[3])
        min_time = min(distances, key=lambda x: x[4])

        result['distances']['min_dist'] = min_dist[:3]
        result['distances']['min_time'] = min_time[:3]

        result['success'] = True
    except:
        warnings.warn('Could not determine point distances for prioritization tests, using backup values instead')

    return result


@pytest.fixture()
def matchup_params():
    return {
        'gridded_to_gridded': {
            "primary": "MUR25-JPL-L4-GLOB-v04.2_test",
            "secondary": "SMAP_JPL_L3_SSS_CAP_8DAY-RUNNINGMEAN_V5_test",
            "startTime": "2018-08-01T00:00:00Z",
            "endTime": "2018-08-02T00:00:00Z",
            "b": "-100,20,-90,30",
            "depthMin": -20,
            "depthMax": 10,
            "tt": 43200,
            "rt": 1000,
            "matchOnce": True,
            "resultSizeLimit": 7000,
            "platforms": "42"
        },
        'gridded_to_swath': {
            "primary": "MUR25-JPL-L4-GLOB-v04.2_test",
            "secondary": "ASCATB-L2-Coastal_test",
            "startTime": "2018-07-05T00:00:00Z",
            "endTime": "2018-07-05T23:59:59Z",
            "b": "-127,32,-120,40",
            "depthMin": -20,
            "depthMax": 10,
            "tt": 12000,
            "rt": 1000,
            "matchOnce": True,
            "resultSizeLimit": 7000,
            "platforms": "42"
        },
        'swath_to_gridded': {
            "primary": "ASCATB-L2-Coastal_test",
            "secondary": "MUR25-JPL-L4-GLOB-v04.2_test",
            "startTime": "2018-08-01T00:00:00Z",
            "endTime": "2018-08-02T00:00:00Z",
            "b": "-100,20,-90,30",
            "depthMin": -20,
            "depthMax": 10,
            "tt": 43200,
            "rt": 1000,
            "matchOnce": True,
            "resultSizeLimit": 7000,
            "platforms": "65"
        },
        'swath_to_swath': {
            "primary": "VIIRS_NPP-2018_Heatwave_test",
            "secondary": "ASCATB-L2-Coastal_test",
            "startTime": "2018-07-05T00:00:00Z",
            "endTime": "2018-07-05T23:59:59Z",
            "b": "-120,28,-118,30",
            "depthMin": -20,
            "depthMax": 10,
            "tt": 43200,
            "rt": 1000,
            "matchOnce": True,
            "resultSizeLimit": 7000,
            "platforms": "42"
        },
        'long': {  # TODO: Find something for this; it's copied atm
            "primary": "VIIRS_NPP-2018_Heatwave_test",
            "secondary": "ASCATB-L2-Coastal_test",
            "startTime": "2018-07-05T00:00:00Z",
            "endTime": "2018-07-05T23:59:59Z",
            "b": "-120,28,-118,30",
            "depthMin": -20,
            "depthMax": 10,
            "tt": 43200,
            "rt": 1000,
            "matchOnce": True,
            "resultSizeLimit": 7000,
            "platforms": "42"
        },
    }


def skip(msg=""):
    raise pytest.skip(msg)


def b_to_polygon(b):
    west, south, east, north = [float(p) for p in b.split(",")]
    polygon = Polygon([(west, south), (east, south), (east, north), (west, north), (west, south)])
    return polygon


def iso_time_to_epoch(str_time):
    epoch = timezone('UTC').localize(datetime(1970, 1, 1))

    return (datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=UTC) - epoch).total_seconds()


def verify_secondary_in_tolerance(primary, secondary, rt):
    distance = geodesic((primary['lat'], primary['lon']), (secondary['lat'], secondary['lon'])).m

    assert distance <= rt


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
    return t.strftime('%Y-%m-%dT%H:%M:%SZ')


def verify_match(match, point, time, s_point, s_time, params, bounding_poly):
    # Check primary point is as expected
    assert match['point'] == point
    assert match['time'] == time

    # Check primary point within search bounds
    assert iso_time_to_epoch(params['startTime']) \
           <= match['time'] \
           <= iso_time_to_epoch(params['endTime'])
    assert bounding_poly.intersects(wkt.loads(match['point']))

    secondary = match['matches'][0]

    # Check secondary point is as expected
    assert secondary['point'] == s_point
    assert secondary['time'] == s_time

    # Check secondary point within specified spatial & temporal tolerances for matched primary
    verify_secondary_in_tolerance(match, secondary, params['rt'])

    assert (match['time'] - params['tt']) \
           <= secondary['time'] \
           <= (match['time'] + params['tt'])


def verify_match_consistency(match, params, bounding_poly):
    # Check primary point within search bounds
    assert iso_time_to_epoch(params['startTime']) \
           <= match['time'] \
           <= iso_time_to_epoch(params['endTime'])
    assert bounding_poly.intersects(wkt.loads(match['point']))

    for secondary in match['matches']:
        # Check secondary point within specified spatial & temporal tolerances for matched primary
        verify_secondary_in_tolerance(match, secondary, params['rt'])

        assert (match['time'] - params['tt']) \
               <= secondary['time'] \
               <= (match['time'] + params['tt'])


def validate_insitu(body, params, test):
    if body['total'] <= params['itemsPerPage']:
        assert body['total'] == len(body['results'])
    else:
        assert len(body['results']) == params['itemsPerPage']

    if len(body['results']) == 0:
        warnings.warn(f'Insitu test ({test}) returned no results!')

    bounding_poly = b_to_polygon(params['bbox'])

    for result in body['results']:
        assert bounding_poly.intersects(
            wkt.loads(f"Point({result['longitude']} {result['latitude']})")
        )

        if result['depth'] != -99999.0:
            assert params['minDepth'] <= result['depth'] <= params['maxDepth']

        assert params['startTime'] <= result['time'] <= params['endTime']


def try_save(name, time, response, ext='json', mode='w'):
    Path(f'responses/{time}/').mkdir(parents=True, exist_ok=True)

    try:
        with open(f'responses/{time}/{name}.{ext}', mode=mode) as f:
            if ext == 'json':
                json.dump(response, f, indent=4)
            elif ext == 'csv':
                f.write(response.text)
            else:
                f.write(response.content)
    except Exception as e:
        warnings.warn(f"Failed to save response for {name}\n{e}", RuntimeWarning)


def uniq_primaries(primaries, xfail=False, case=None):
    class Primary:
        def __init__(self, p):
            self.platform = p['platform']
            self.device = p['device']
            self.lon = p['lon']
            self.lat = p['lat']
            self.point = p['point']
            self.time = p['time']
            self.depth = p['depth']
            self.fileurl = p['fileurl']
            self.id = p['id']
            self.source = p['source']
            self.primary = p['primary']
            self.matches = p['matches']

        def __eq__(self, other):
            if not isinstance(other, Primary):
                return False

            return self.platform == other.platform and \
                self.device == other.device and \
                self.lon == other.lon and \
                self.lat == other.lat and \
                self.point == other.point and \
                self.time == other.time and \
                self.depth == other.depth and \
                self.fileurl == other.fileurl and \
                self.id == other.id and \
                self.source == other.source and \
                self.primary == other.primary

        def __str__(self):
            primary = {
                "platform": self.platform,
                "device": self.device,
                "lon": self.lon,
                "lat": self.lat,
                "point": self.point,
                "time": self.time,
                "depth": self.depth,
                "fileurl": self.fileurl,
                "id": self.id,
                "source": self.source,
                "primary": self.primary,
            }

            return json.dumps(primary, indent=4)

    points = [Primary(p) for p in primaries]

    checked = []
    duplicates = {}

    for p in points:
        for c in checked:
            if p == c:
                if p.id not in duplicates:
                    duplicates[p.id] = [p, c]
                else:
                    duplicates[p.id].append(p)
                break
        checked.append(p)

    if len(duplicates) > 0:
        m = print if not xfail else warnings.warn

        msg = f'Duplicate point(s) found ({len(duplicates)} total)'

        if case is not None:
            msg += f' for case {case}'

        msg += '\n\n-----\n\n'

        for d in duplicates:
            d = duplicates[d]

            msg += 'Primary point:\n' + str(d[0]) + '\n\n'

            matches = [p.matches for p in d]

            msg += f'Matches to ({len(matches)}):\n'
            msg += json.dumps(matches, indent=4)
            msg += '\n\n'

        m(msg)

        if xfail:
            pytest.xfail('Duplicate points found')
        else:
            assert False, 'Duplicate points found'


def check_count(count, expected, fail_on_mismatch):
    if count == expected:
        return
    elif fail_on_mismatch:
        raise AssertionError(f'Incorrect count: Expected {expected}, got {count}')
    else:
        warnings.warn(f'Incorrect count: Expected {expected}, got {count}')


def url_scheme(scheme, url):
    if urlparse(url).scheme == scheme:
        return url
    else:
        return urlunparse(tuple([scheme] + list(urlparse(url)[1:])))


# Run the matchup query and return json output (and eid?)
# Should be able to work if match_spark is synchronous or asynchronous
def run_matchup(url, params, page_size=3500):
    TIMEOUT = 60 * 60
    # TIMEOUT = float('inf')

    response = requests.get(url, params=params)

    scheme = urlparse(url).scheme

    assert response.status_code == 200, 'Initial match_spark query failed'
    response_json = response.json()

    asynchronous = 'status' in response_json

    if not asynchronous:
        return response_json
    else:
        start = datetime.utcnow()
        job_url = [link for link in response_json['links'] if link['rel'] == 'self'][0]['href']

        job_url = url_scheme(scheme, job_url)

        retries = 3
        timeouts = [2, 5, 10]

        while response_json['status'] == 'running' and (datetime.utcnow() - start).total_seconds() <= TIMEOUT:
            status_response = requests.get(job_url)
            status_code = response.status_code

            # /job poll may fail internally. This does not necessarily indicate job failure (ie, Cassandra read
            # timed out). Retry it a couple of times and fail the test if it persists.
            if status_code == 500 and retries > 0:
                warnings.warn('/job poll failed; retrying')
                sleep(timeouts[3 - retries])
                retries -= 1
                continue

            assert status_response.status_code == 200, '/job status polling failed'
            response_json = status_response.json()

            if response_json['status'] == 'running':
                sleep(10)

        job_status = response_json['status']

        if job_status == 'running':
            skip(f'Job has been running too long ({(datetime.utcnow() - start)}), skipping to run other tests')
        elif job_status in ['cancelled', 'failed']:
            raise ValueError(f'Async matchup job finished with incomplete status ({job_status})')
        else:
            stac_url = [
                link for link in response_json['links'] if 'STAC' in link['title']
            ][0]['href']

            stac_url = url_scheme(scheme, stac_url)

            catalogue_response = requests.get(stac_url)
            assert catalogue_response.status_code == 200, 'Catalogue fetch failed'

            catalogue_response = catalogue_response.json()

            json_cat_url = [
                link for link in catalogue_response['links'] if 'JSON' in link['title']
            ][0]['href']

            json_cat_url = url_scheme(scheme, json_cat_url)

            catalogue_response = requests.get(json_cat_url)
            assert catalogue_response.status_code == 200, 'Catalogue fetch failed'

            catalogue_response = catalogue_response.json()

            results_urls = [
                url_scheme(scheme, link['href']) for link in
                catalogue_response['links'] if 'output=JSON' in link['href']
                # link['href'] for link in response_json['links'] if link['type'] == 'application/json'
            ]

            def get_results(url):
                retries = 3
                retry_delay = 1.5

                while retries > 0:
                    response = requests.get(url)

                    try:
                        response.raise_for_status()
                        result = response.json()

                        assert result['count'] == len(result['data'])

                        return result
                    except:
                        retries -= 1
                        sleep(retry_delay)
                        retry_delay *= 2

            assert len(results_urls) > 0, 'STAC catalogue returned no result queries'

            matchup_result = get_results(results_urls[0])

            for url in results_urls[1:]:
                matchup_result['data'].extend(get_results(url)['data'])

            return matchup_result


@pytest.mark.integration
@pytest.mark.parametrize(
    ['match', 'expected'],
    list(zip(
        ['gridded_to_gridded', 'gridded_to_swath', 'swath_to_gridded', 'swath_to_swath'],
        [1058, 6, 21, 4026]
    ))
)
def test_match_spark(host, start, fail_on_miscount, matchup_params, match, expected):
    url = urljoin(host, 'match_spark')

    params = matchup_params[match]

    bounding_poly = b_to_polygon(params['b'])

    body = run_matchup(url, params)
    try_save(f"test_matchup_spark_{match}", start, body)
    data = body['data']

    for match in data:
        verify_match_consistency(match, params, bounding_poly)

    uniq_primaries(data, case=f"test_matchup_spark_{match}")
    check_count(len(data), expected, fail_on_miscount)


@pytest.mark.integration
def test_match_spark_job_cancellation(host, start, matchup_params):
    url = urljoin(host, 'match_spark')

    params = matchup_params['long']

    response = requests.get(url, params=params)

    assert response.status_code == 200, 'Initial match_spark query failed'
    response_json = response.json()

    asynchronous = 'status' in response_json

    if not asynchronous:
        skip('Deployed SDAP version does not have asynchronous matchup')
    else:
        sleep(1)  # Time to allow spark workers to start working

        if response_json['status'] != 'running':
            skip('Job finished before it could be cancelled')
        else:
            cancel_url = [link for link in response_json['links'] if link['rel'] == 'cancel'][0]['href']

            cancel_url = url_scheme(
                urlparse(url).scheme,
                cancel_url
            )

            cancel_response = requests.get(cancel_url)
            assert cancel_response.status_code == 200, 'Cancellation query failed'

            cancel_json = cancel_response.json()

            assert cancel_json['status'] != 'running', 'Job did not cancel'

            if cancel_json['status'] in ['success', 'failed']:
                warnings.warn(f'Job status after cancellation is not \'cancelled\' ({cancel_json["status"]}), passing '
                              f'case because it is no longer \'running\', but actual cancellation could not be tested '
                              f'here.')


@pytest.mark.integration
@pytest.mark.skip('Test not re-implemented yet')
def test_cdmsresults_json(host, eid, start):
    url = urljoin(host, 'cdmsresults')

    # Skip the test automatically if the matchup request was not successful
    if not eid['successful']:
        skip('Matchup request was unsuccessful so there are no results to get from domsresults')

    def fetch_result(execution_id, output):
        return requests.get(url, params={"id": execution_id, "output": output})

    eid_list = eid['eid']
    param_list = eid['params']

    response = fetch_result(eid_list[0], "JSON")

    assert response.status_code == 200

    body = response.json()
    try_save("test_cdmsresults_json_A", start, body)

    data = body['data']
    assert len(data) == 5

    for m in data:
        m['point'] = f"Point({m['lon']} {m['lat']})"
        for s in m['matches']:
            s['point'] = f"Point({s['lon']} {s['lat']})"

    data.sort(key=lambda e: e['point'])

    params = param_list[0]
    bounding_poly = b_to_polygon(params['b'])

    verify_match(
        data[0], 'Point(-86.125 27.625)',
        1535360400, 'Point(-86.13 27.63)',
        1535374800, params, bounding_poly
    )

    verify_match(
        data[1], 'Point(-88.875 27.875)',
        1534669200, 'Point(-88.88 27.88)',
        1534698000, params, bounding_poly
    )

    verify_match(
        data[2], 'Point(-90.125 27.625)',
        1534496400, 'Point(-90.13 27.63)',
        1534491000, params, bounding_poly
    )

    verify_match(
        data[3], 'Point(-90.125 28.125)',
        1534928400, 'Point(-90.13 28.12)',
        1534899600, params, bounding_poly
    )

    verify_match(
        data[4], 'Point(-90.375 28.125)',
        1534842000, 'Point(-90.38 28.12)',
        1534813200, params, bounding_poly
    )

    response = fetch_result(eid_list[1], "JSON")

    assert response.status_code == 200

    body = response.json()
    try_save("test_cdmsresults_json_B", start, body)

    data = body['data']
    assert len(data) == 5

    for m in data:
        m['point'] = f"Point({m['lon']} {m['lat']})"
        for s in m['matches']:
            s['point'] = f"Point({s['lon']} {s['lat']})"

    data.sort(key=lambda e: e['point'])

    params = param_list[1]
    bounding_poly = b_to_polygon(params['b'])

    verify_match(
        data[0], 'Point(-86.125 27.625)',
        1535371200, 'Point(-86.13 27.63)',
        1535374800, params, bounding_poly
    )

    verify_match(
        data[1], 'Point(-88.875 27.875)',
        1534680000, 'Point(-88.88 27.88)',
        1534698000, params, bounding_poly
    )

    verify_match(
        data[2], 'Point(-90.125 27.625)',
        1534507200, 'Point(-90.13 27.63)',
        1534491000, params, bounding_poly
    )

    verify_match(
        data[3], 'Point(-90.125 28.125)',
        1534939200, 'Point(-90.13 28.12)',
        1534899600, params, bounding_poly
    )

    verify_match(
        data[4], 'Point(-90.375 28.125)',
        1534852800, 'Point(-90.38 28.12)',
        1534813200, params, bounding_poly
    )


@pytest.mark.integration
@pytest.mark.skip('Test not re-implemented yet')
def test_cdmsresults_csv(host, eid, start):
    url = urljoin(host, 'cdmsresults')

    # Skip the test automatically if the matchup request was not successful
    if not eid['successful']:
        skip('Matchup request was unsuccessful so there are no results to get from domsresults')

    def fetch_result(execution_id, output):
        return requests.get(url, params={"id": execution_id, "output": output})

    eid_list = eid['eid']
    param_list = eid['params']

    response = fetch_result(eid_list[0], "CSV")
    params = param_list[0]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    try_save("test_cdmsresults_csv_A", start, response, "csv")

    rows = response.text.split('\r\n')
    index = rows.index('')

    global_rows = rows[:index]
    matchup_rows = rows[index + 1:-1]  # Drop trailing empty string from trailing newline

    global_rows = translate_global_rows(global_rows)
    matchup_rows = translate_matchup_rows(matchup_rows)

    assert len(matchup_rows) == int(global_rows['CDMS_num_primary_matched'])

    for row in matchup_rows:
        primary_point = lat_lon_to_point(row['lat'], row['lon'])

        assert bounding_poly.intersects(primary_point)
        assert params['startTime'] <= format_time(row['time']) <= params['endTime']

        verify_secondary_in_tolerance(
            {'lat': row['lat'], 'lon': row['lon']},
            {'lat': row['lat_secondary'], 'lon': row['lon_secondary']},
            params['rt']
        )
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= iso_time_to_epoch(format_time(row['time_secondary'])) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])

    response = fetch_result(eid_list[1], "CSV")
    params = param_list[1]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    try_save("test_cdmsresults_csv_B", start, response, "csv")

    rows = response.text.split('\r\n')
    index = rows.index('')

    global_rows = rows[:index]
    matchup_rows = rows[index + 1:-1]  # Drop trailing empty string from trailing newline

    global_rows = translate_global_rows(global_rows)
    matchup_rows = translate_matchup_rows(matchup_rows)

    assert len(matchup_rows) == int(global_rows['CDMS_num_primary_matched'])

    for row in matchup_rows:
        primary_point = lat_lon_to_point(row['lat'], row['lon'])

        assert bounding_poly.intersects(primary_point)
        assert params['startTime'] <= format_time(row['time']) <= params['endTime']

        verify_secondary_in_tolerance(
            {'lat': row['lat'], 'lon': row['lon']},
            {'lat': row['lat_secondary'], 'lon': row['lon_secondary']},
            params['rt']
        )
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= iso_time_to_epoch(format_time(row['time_secondary'])) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])


@pytest.mark.integration
@pytest.mark.skip('Test not re-implemented yet')
def test_cdmsresults_netcdf(host, eid, start):
    warnings.filterwarnings('ignore')

    url = urljoin(host, 'cdmsresults')

    # Skip the test automatically if the matchup request was not successful
    if not eid['successful']:
        skip('Matchup request was unsuccessful so there are no results to get from domsresults')

    def fetch_result(execution_id, output):
        return requests.get(url, params={"id": execution_id, "output": output})

    eid_list = eid['eid']
    param_list = eid['params']

    temp_file = Temp(mode='wb+', suffix='.csv.tmp', prefix='CDMSReader_')

    response = fetch_result(eid_list[0], "NETCDF")
    params = param_list[0]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    try_save("test_cdmsresults_netcdf_A", start, response, "nc", 'wb')

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

        assert bounding_poly.intersects(primary_point)
        assert iso_time_to_epoch(params['startTime']) \
               <= float(row['PrimaryData_time']) \
               <= iso_time_to_epoch(params['endTime'])

        verify_secondary_in_tolerance(
            {'lat': row['PrimaryData_lat'], 'lon': row['PrimaryData_lon']},
            {'lat': row['SecondaryData_lat'], 'lon': row['SecondaryData_lon']},
            params['rt']
        )
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= float(row['SecondaryData_time']) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])

    response = fetch_result(eid_list[1], "NETCDF")
    params = param_list[1]
    bounding_poly = b_to_polygon(params['b'])

    assert response.status_code == 200

    try_save("test_cdmsresults_netcdf_B", start, response, "nc", 'wb')

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

        assert bounding_poly.intersects(primary_point)
        assert iso_time_to_epoch(params['startTime']) \
               <= float(row['PrimaryData_time']) \
               <= iso_time_to_epoch(params['endTime'])

        verify_secondary_in_tolerance(
            {'lat': row['PrimaryData_lat'], 'lon': row['PrimaryData_lon']},
            {'lat': row['SecondaryData_lat'], 'lon': row['SecondaryData_lon']},
            params['rt']
        )
        assert (iso_time_to_epoch(params['startTime']) - params['tt']) \
               <= float(row['SecondaryData_time']) \
               <= (iso_time_to_epoch(params['endTime']) + params['tt'])

    temp_file.close()
    warnings.filterwarnings('default')


@pytest.mark.integration
def test_timeseries_spark(host, start):
    url = urljoin(host, 'timeSeriesSpark')

    params = {
        "ds": "MUR25-JPL-L4-GLOB-v04.2_test",
        "b": "-135,-10,-80,10",
        "startTime": "2018-07-05T00:00:00Z",
        "endTime": "2018-09-30T23:59:59Z",
    }

    response = requests.get(url, params=params)

    assert response.status_code == 200

    data = response.json()
    try_save('test_timeseries_spark', start, data)

    assert len(data['data']) == len(pd.date_range(params['startTime'], params['endTime'], freq='D'))

    epoch = datetime(1970, 1, 1, tzinfo=UTC)

    start = (datetime.strptime(params['startTime'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC) - epoch).total_seconds()
    end = (datetime.strptime(params['endTime'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC) - epoch).total_seconds()

    for p in data['data']:
        assert start <= p[0]['time'] <= end


@pytest.mark.integration
def test_list(host, start):
    url = urljoin(host, 'list')

    response = requests.get(url)

    assert response.status_code == 200

    body = response.json()
    try_save("test_list", start, body)

    assert isinstance(body, list)

    if len(body) == 0:
        warnings.warn('/list returned no datasets. This could be correct if SDAP has no data ingested, otherwise '
                      'this should be considered a failure')

@pytest.mark.integration
def test_cdmslist(host, start):
    url = urljoin(host, 'cdmslist')

    response = requests.get(url)

    assert response.status_code == 200

    body = response.json()
    try_save("test_cdmslist", start, body)

    data = body['data']

    num_satellite = len(data['satellite'])
    num_insitu = len(data['insitu'])

    if num_satellite == 0:
        warnings.warn('/cdmslist returned no satellite datasets. This could be correct if SDAP has no data ingested, '
                      'otherwise this should be considered a failure')

    if num_insitu == 0:
        warnings.warn('/cdmslist returned no insitu datasets. This could be correct if SDAP has no insitu data '
                      'ingested, otherwise this should be considered a failure')


@pytest.mark.integration
def test_cdmssubset_L4(host, start):
    url = urljoin(host, 'cdmssubset')

    params = {
        "dataset": "MUR25-JPL-L4-GLOB-v04.2_test",
        "parameter": "sst",
        "startTime": "2018-09-24T00:00:00Z",
        "endTime": "2018-09-30T00:00:00Z",
        "b": "160,-30,180,-25",
        "output": "ZIP"
    }

    response = requests.get(url, params=params)

    assert response.status_code == 200

    try_save("test_cdmssubset_L4_a", start, response, "zip", 'wb')

    bounding_poly = b_to_polygon(params['b'])

    response_buf = io.BytesIO(response.content)

    with ZipFile(response_buf) as data:
        namelist = data.namelist()

        assert namelist == ['MUR25-JPL-L4-GLOB-v04.2_test.csv']

        csv_buf = io.StringIO(data.read(namelist[0]).decode('utf-8'))
        csv_data = pd.read_csv(csv_buf)

    def validate_row_bounds(row):
        assert bounding_poly.intersects(Point(float(row['longitude']), float(row['latitude'])))
        assert params['startTime'] <= row['time'] <= params['endTime']

    for i in range(0, len(csv_data)):
        validate_row_bounds(csv_data.iloc[i])

    params['dataset'] = 'OISSS_L4_multimission_7day_v1_test'

    response = requests.get(url, params=params)

    assert response.status_code == 200

    try_save("test_cdmssubset_L4_b", start, response, "zip", 'wb')

    response_buf = io.BytesIO(response.content)

    with ZipFile(response_buf) as data:
        namelist = data.namelist()

        assert namelist == ['OISSS_L4_multimission_7day_v1_test.csv']

        csv_buf = io.StringIO(data.read(namelist[0]).decode('utf-8'))
        csv_data = pd.read_csv(csv_buf)

    for i in range(0, len(csv_data)):
        validate_row_bounds(csv_data.iloc[i])


@pytest.mark.integration
def test_cdmssubset_L2(host, start):
    url = urljoin(host, 'cdmssubset')

    params = {
        "dataset": "ASCATB-L2-Coastal_test",
        "startTime": "2018-09-24T00:00:00Z",
        "endTime": "2018-09-30T00:00:00Z",
        "b": "160,-30,180,-25",
        "output": "ZIP"
    }

    response = requests.get(url, params=params)

    assert response.status_code == 200

    try_save("test_cdmssubset_L2", start, response, "zip", 'wb')

    bounding_poly = b_to_polygon(params['b'])

    response_buf = io.BytesIO(response.content)

    with ZipFile(response_buf) as data:
        namelist = data.namelist()

        assert namelist == ['ASCATB-L2-Coastal_test.csv']

        csv_buf = io.StringIO(data.read(namelist[0]).decode('utf-8'))
        csv_data = pd.read_csv(csv_buf)

    def validate_row_bounds(row):
        assert bounding_poly.intersects(Point(float(row['longitude']), float(row['latitude'])))
        assert params['startTime'] <= row['time'] <= params['endTime']

    for i in range(0, len(csv_data)):
        validate_row_bounds(csv_data.iloc[i])


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
def test_version(host, start):
    url = urljoin(host, 'version')

    response = requests.get(url)

    assert response.status_code == 200
    assert re.match(r'^\d+\.\d+\.\d+(-.+)?$', response.text)


@pytest.mark.integration
def test_capabilities(host, start):
    url = urljoin(host, 'capabilities')

    response = requests.get(url)

    assert response.status_code == 200

    capabilities = response.json()

    try_save('test_capabilities', start, capabilities)

    assert len(capabilities) > 0

    for capability in capabilities:
        assert all([k in capability for k in ['name', 'path', 'description', 'parameters']])
        assert all([isinstance(k, str) for k in ['name', 'path', 'description']])

        assert isinstance(capability['parameters'], (dict, list))

        for param in capability['parameters']:
            if isinstance(capability['parameters'], dict):
                param = capability['parameters'][param]

            assert isinstance(param, dict)
            assert all([k in param and isinstance(param[k], str) for k in ['name', 'type', 'description']])


@pytest.mark.integration
def test_endpoints(host, start):
    url = urljoin(host, 'capabilities')

    response = requests.get(url)

    if response.status_code != 200:
        skip('Could not get endpoints list. Expected if test_capabilities has failed')

    capabilities = response.json()

    endpoints = [c['path'] for c in capabilities]

    non_existent_endpoints = []

    for endpoint in endpoints:
        status = requests.head(urljoin(host, endpoint)).status_code

        if status == 404:
            # Strip special characters because some endpoints have wildcards/regex characters
            # This may not work forever though
            stripped_endpoint = re.sub(r'[^a-zA-Z0-9/_-]', '', endpoint)

            status = requests.head(urljoin(host, stripped_endpoint)).status_code

            if status == 404:
                non_existent_endpoints.append(([endpoint, stripped_endpoint], status))

    assert len(non_existent_endpoints) == 0, non_existent_endpoints


@pytest.mark.integration
def test_heartbeat(host, start):
    url = urljoin(host, 'heartbeat')

    response = requests.get(url)

    assert response.status_code == 200
    heartbeat = response.json()

    assert isinstance(heartbeat, dict)
    assert all(heartbeat.values())


