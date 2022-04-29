import datetime
import uuid
import csv
import pytest
from netCDF4 import Dataset
import io
from webservice.algorithms.doms.BaseDomsHandler import DomsCSVFormatter, DomsNetCDFFormatter


@pytest.fixture()
def test_input():
    results = [
        {
            "id": "9c08c026-eff7-30a7-ab1e-413a64f507ff[[0 0 3]]",
            "x": 173.375,
            "y": -29.875,
            "source": "MUR25-JPL-L4-GLOB-v04.2",
            "device": "radiometers",
            "platform": "orbiting satellite",
            "time": datetime.datetime(2018, 9, 27, 9, 0),
            "analysed_sst": 18.069000244140625,
            "analysis_error": -272.7799987792969,
            "sst_anomaly": 1.0,
            "matches": [
                {
                    "id": "PCEWYL",
                    "x": 173.38,
                    "y": -29.88,
                    "source": "icoads",
                    "device": None,
                    "platform": "drifting surface float",
                    "time": datetime.datetime(2018, 10, 18, 20, 0),
                    "sea_water_temperature": 19.6
                }
            ]
        },
        {
            "id": "8ff1b246-16de-34e2-87bb-600c4107a7f8[[ 0  8 15]]",
            "x": 161.375,
            "y": -27.875,
            "source": "MUR25-JPL-L4-GLOB-v04.2",
            "device": "radiometers",
            "platform": "orbiting satellite",
            "time": datetime.datetime(2018, 9, 28, 9, 0, ),
            "analysed_sst": 19.891998291015625,
            "analysis_error": -272.7799987792969,
            "sst_anomaly": 1.0,
            "matches": [
                {
                    "id": "PCY3CI",
                    "x": 161.38,
                    "y": -27.88,
                    "source": "icoads",
                    "device": None,
                    "platform": "drifting surface float",
                    "time": datetime.datetime(2018, 10, 23, 10, 0, ),
                    "sea_water_temperature": 20.0
                }
            ]
        },
    ]
    params = {
        'primary': 'MUR25-JPL-L4-GLOB-v04.2',
        'matchup': ['icoads'],
        'depthMin': 0.0,
        'depthMax': 5.0,
        'timeTolerance': 2592000,
        'radiusTolerance': 1000.0,
        'startTime': datetime.datetime(2018, 9, 24, 0, 0, ),
        'endTime': datetime.datetime(2018, 9, 30, 0, 0, ),
        'platforms': '1,2,3,4,5,6,7,8,9',
        'bbox': '160,-30,180,-25',
        'parameter': 'sst'
    }
    details = {
        'numGriddedMatched': 54,
        'numGriddedChecked': 0,
        'numInSituMatched': 54,
        'numInSituChecked': 0,
        'timeToComplete': 26
    }

    yield results, params, details


def test_csv(test_input):
    """
    Test that CSV is constructed properly given result, params, and
    details.
    """
    test_execution_id = str(uuid.uuid4())

    results, params, details = test_input

    csv_formatter = DomsCSVFormatter()
    csv_result = csv_formatter.create(
        executionId=test_execution_id,
        results=results,
        params=params,
        details=details
    )

    csv_reader = csv.reader(csv_result.split('\n'), delimiter=',')
    header = None
    for row in csv_reader:
        if not row:
            continue

        if header:
            # Expected science vars should all contain data
            expected_var_names = [
                'analysed_sst',
                'analysis_error',
                'sst_anomaly',
                'sea_water_temperature'
            ]
            for var_name in expected_var_names:
                assert var_name in header
                assert len(header) == len(row)
                index = header.index(var_name)
                assert row[index] is not None

        if 'id' == row[0]:
            header = row


def test_netcdf(test_input):
    """
    Test that the /domsresults endpoint results in a properly
    structured NetCDF file.
    """
    test_execution_id = str(uuid.uuid4())

    results, params, details = test_input

    nc_formatter = DomsNetCDFFormatter()
    nc_result = nc_formatter.create(
        executionId=test_execution_id,
        results=results,
        params=params,
        details=details
    )

    ds = Dataset('test', memory=nc_result)

    # with open('small_matchup.nc', 'wb') as f:
    #     f.write(nc_result)

    assert 'PrimaryData' in ds.groups
    assert 'SecondaryData' in ds.groups

    assert 'sst_anomaly' in ds.groups['PrimaryData'].variables
    assert 'analysis_error' in ds.groups['PrimaryData'].variables
    assert 'analysed_sst' in ds.groups['PrimaryData'].variables

    assert 'sea_water_temperature' in ds.groups['SecondaryData'].variables

def test_netcdf_big(test_input): # TODO remove this. jUst for testing.
    test_execution_id = str(uuid.uuid4())

    results, params, details = test_input

    results = big_results

    nc_formatter = DomsNetCDFFormatter()
    nc_result = nc_formatter.create(
        executionId=test_execution_id,
        results=results,
        params=params,
        details=details
    )

    ds = Dataset('test', memory=nc_result)



big_results = [
    {
        'id': '163a81c4-bbf1-3ba4-85c9-e1ce645dcb9a[[0, 2, 17]]',
        'lon': -123.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 27.90301513671875,
        'matches': [{
            'id': 'Buoys_20171003(7172)',
            'lon': -123.119,
            'lat': 15.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 10, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': 'd75bd15c-967e-33a4-8485-d3b466f1709d[[0, 23, 13]]',
        'lon': -139.125,
        'lat': 20.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 25.00201416015625,
        'matches': [{
            'id': 'argo_spurs_20171006(1250)',
            'lon': -139.129,
            'lat': 20.867,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting subsurface profiling float',
            'time': datetime.datetime(2017, 10, 5, 23, 19, 47),
            'sea_water_salinity': 34.97,
            'sea_water_salinity_depth': 4.47223342927,
            'sea_water_temperature': 24.78,
            'sea_water_temperature_depth': 4.47223342927
        }]
    }, {
        'id': '7deab803-ca0e-3887-a4f7-09081c35d9fa[[0, 23, 4]]',
        'lon': -148.875,
        'lat': 13.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.147003173828125,
        'matches': [{
            'id': 'Buoys_20170918(1164)',
            'lon': -148.875,
            'lat': 13.376,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 18, 7, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': 'e5f90fe2-0de1-323e-9bed-a26011d52cbf[[0, 4, 14]]',
        'lon': -146.375,
        'lat': 23.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 25.4849853515625,
        'matches': [{
            'id': 'Buoys_20170903(742)',
            'lon': -146.378,
            'lat': 23.617,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 3, 6, 0),
            'sea_water_temperature': 25.6
        }]
    }, {
        'id': 'ba867010-507b-3c62-8893-140ae07da30b[[0, 18, 24]]',
        'lon': -143.875,
        'lat': 12.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.625,
        'matches': [{
            'id': 'Buoys_20170829(1970)',
            'lon': -143.879,
            'lat': 12.126,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 8, 29, 6, 0),
            'sea_water_temperature': 28.8
        }]
    }, {
        'id': 'eccb9006-463b-3554-aa25-0de14224e0c4[[0, 23, 4]]',
        'lon': -148.875,
        'lat': 13.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.1669921875,
        'matches': [{
            'id': 'Buoys_20170918(1164)',
            'lon': -148.875,
            'lat': 13.376,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 18, 7, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': '7deab803-ca0e-3887-a4f7-09081c35d9fa[[0, 13, 12]]',
        'lon': -146.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.441009521484375,
        'matches': [{
            'id': 'Buoys_20171003(1210)',
            'lon': -146.874,
            'lat': 10.871,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 7, 0),
            'sea_water_temperature': 28.6
        }]
    }, {
        'id': 'f790a842-600a-3225-810d-1ac2c47d2945[[0, 8, 25]]',
        'lon': -151.125,
        'lat': 17.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 27.3330078125,
        'matches': [{
            'id': 'Buoys_20171022(485)',
            'lon': -151.13,
            'lat': 17.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 22, 6, 50),
            'sea_water_temperature': 26.4
        }]
    }, {
        'id': 'c9cb10ab-ebae-3c8b-a498-d7aef5cf53ed[[0, 2, 23]]',
        'lon': -144.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.798004150390625,
        'matches': [{
            'id': 'Buoys_20171005(7542)',
            'lon': -144.13,
            'lat': 15.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 5, 16, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '1bfa3b4a-a310-3661-b1d9-2648c0d61a9a[[0, 5, 8]]',
        'lon': -147.875,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 27.47100830078125,
        'matches': [{
            'id': 'Buoys_20171025(7730)',
            'lon': -147.881,
            'lat': 16.375,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 25, 17, 0),
            'sea_water_temperature': 27.1
        }]
    }, {
        'id': '47693152-084f-3a92-b74c-3cef41a2cc36[[0, 14, 7]]',
        'lon': -140.625,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.592987060546875,
        'matches': [{
            'id': 'Buoys_20171015(1659)',
            'lon': -140.626,
            'lat': 11.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 15, 3, 0),
            'sea_water_temperature': 28.3
        }]
    }, {
        'id': 'c63f3a16-42ac-3185-b8d2-25e26bf9401c[[0, 23, 13]]',
        'lon': -139.125,
        'lat': 20.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 25.2760009765625,
        'matches': [{
            'id': 'argo_spurs_20171006(1250)',
            'lon': -139.129,
            'lat': 20.867,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting subsurface profiling float',
            'time': datetime.datetime(2017, 10, 5, 23, 19, 47),
            'sea_water_salinity': 34.97,
            'sea_water_salinity_depth': 4.47223342927,
            'sea_water_temperature': 24.78,
            'sea_water_temperature_depth': 4.47223342927
        }]
    }, {
        'id': '1bfa3b4a-a310-3661-b1d9-2648c0d61a9a[[0, 20, 7]]',
        'lon': -148.125,
        'lat': 20.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 26.246002197265625,
        'matches': [{
            'id': 'Buoys_20170920(7966)',
            'lon': -148.131,
            'lat': 20.124,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 20, 12, 0),
            'sea_water_temperature': 26.3
        }]
    }, {
        'id': 'db173d93-895d-357d-8d30-fee57540a2ef[[0, 14, 14]]',
        'lon': -138.875,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 27.91400146484375,
        'matches': [{
            'id': 'Buoys_20171008(1812)',
            'lon': -138.874,
            'lat': 11.117,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 4, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '1bfa3b4a-a310-3661-b1d9-2648c0d61a9a[[0, 5, 10]]',
        'lon': -147.375,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 27.490997314453125,
        'matches': [{
            'id': 'Buoys_20171024(7720)',
            'lon': -147.376,
            'lat': 16.383,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 24, 6, 0),
            'sea_water_temperature': 27.2
        }]
    }, {
        'id': 'ba867010-507b-3c62-8893-140ae07da30b[[0, 13, 12]]',
        'lon': -146.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.4639892578125,
        'matches': [{
            'id': 'Buoys_20171003(1210)',
            'lon': -146.874,
            'lat': 10.871,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 7, 0),
            'sea_water_temperature': 28.6
        }]
    }, {
        'id': 'db173d93-895d-357d-8d30-fee57540a2ef[[0, 14, 7]]',
        'lon': -140.625,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.670989990234375,
        'matches': [{
            'id': 'Buoys_20171015(1659)',
            'lon': -140.626,
            'lat': 11.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 15, 3, 0),
            'sea_water_temperature': 28.3
        }]
    }, {
        'id': '2d66d136-c883-366a-9559-fd1336a3551d[[0, 8, 25]]',
        'lon': -151.125,
        'lat': 17.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.352996826171875,
        'matches': [{
            'id': 'Buoys_20171022(485)',
            'lon': -151.13,
            'lat': 17.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 22, 6, 50),
            'sea_water_temperature': 26.4
        }]
    }, {
        'id': '38e08fcc-e6ed-3fc7-aac7-efd3ca86dbb6[[0, 5, 10]]',
        'lon': -147.375,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 27.433990478515625,
        'matches': [{
            'id': 'Buoys_20171024(7720)',
            'lon': -147.376,
            'lat': 16.383,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 24, 6, 0),
            'sea_water_temperature': 27.2
        }]
    }, {
        'id': '3a59959f-953c-31ea-a677-d9c8da5807fe[[0, 8, 25]]',
        'lon': -151.125,
        'lat': 17.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 27.37200927734375,
        'matches': [{
            'id': 'Buoys_20171022(485)',
            'lon': -151.13,
            'lat': 17.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 22, 6, 50),
            'sea_water_temperature': 26.4
        }]
    }, {
        'id': 'c1a4e4aa-ecf3-33ff-b1f3-a1329d8626d9[[0, 8, 25]]',
        'lon': -151.125,
        'lat': 17.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 27.352996826171875,
        'matches': [{
            'id': 'Buoys_20171022(485)',
            'lon': -151.13,
            'lat': 17.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 22, 6, 50),
            'sea_water_temperature': 26.4
        }]
    }, {
        'id': '00a42504-45ef-3e6a-b863-7c6c1cb746d7[[0, 2, 11]]',
        'lon': -139.625,
        'lat': 23.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 24.571014404296875,
        'matches': [{
            'id': 'Buoys_20171010(8065)',
            'lon': -139.634,
            'lat': 23.125,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 24.0
        }]
    }, {
        'id': 'be519f66-de2e-3ffe-a929-be8033717020[[0, 13, 12]]',
        'lon': -146.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.28399658203125,
        'matches': [{
            'id': 'Buoys_20171003(1210)',
            'lon': -146.874,
            'lat': 10.871,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 7, 0),
            'sea_water_temperature': 28.6
        }]
    }, {
        'id': '7deab803-ca0e-3887-a4f7-09081c35d9fa[[0, 16, 19]]',
        'lon': -145.125,
        'lat': 11.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.730987548828125,
        'matches': [{
            'id': 'Buoys_20171009(942)',
            'lon': -145.129,
            'lat': 11.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 9, 6, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'ac22e67b-95f5-3c51-ae05-1396b838276c[[0, 13, 16]]',
        'lon': -138.375,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.295013427734375,
        'matches': [{
            'id': 'Buoys_20170930(1889)',
            'lon': -138.367,
            'lat': 10.872,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 30, 15, 0),
            'sea_water_temperature': 28.1
        }]
    }, {
        'id': '3c1e5e37-8bf2-3928-bd60-0384cbcfe405[[0, 14, 14]]',
        'lon': -138.875,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.87799072265625,
        'matches': [{
            'id': 'Buoys_20171008(1812)',
            'lon': -138.874,
            'lat': 11.117,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 4, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'ba867010-507b-3c62-8893-140ae07da30b[[0, 16, 19]]',
        'lon': -145.125,
        'lat': 11.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.597015380859375,
        'matches': [{
            'id': 'Buoys_20171009(942)',
            'lon': -145.129,
            'lat': 11.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 9, 6, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '47693152-084f-3a92-b74c-3cef41a2cc36[[0, 13, 16]]',
        'lon': -138.375,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.22601318359375,
        'matches': [{
            'id': 'Buoys_20170930(1889)',
            'lon': -138.367,
            'lat': 10.872,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 30, 15, 0),
            'sea_water_temperature': 28.1
        }]
    }, {
        'id': 'eccb9006-463b-3554-aa25-0de14224e0c4[[0, 15, 16]]',
        'lon': -145.875,
        'lat': 11.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.60198974609375,
        'matches': [{
            'id': 'Buoys_20171010(715)',
            'lon': -145.868,
            'lat': 11.378,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 28.2
        }]
    }, {
        'id': '4c3b1af8-7818-3f6c-9f51-8abecee7ecb6[[0, 2, 11]]',
        'lon': -139.625,
        'lat': 23.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 24.433990478515625,
        'matches': [{
            'id': 'Buoys_20171010(8065)',
            'lon': -139.634,
            'lat': 23.125,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 24.0
        }]
    }, {
        'id': '7f58e6e6-aae9-32af-8bcd-586875f55ef8[[0, 14, 14]]',
        'lon': -138.875,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.1719970703125,
        'matches': [{
            'id': 'Buoys_20171008(1812)',
            'lon': -138.874,
            'lat': 11.117,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 4, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'eccb9006-463b-3554-aa25-0de14224e0c4[[0, 13, 12]]',
        'lon': -146.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.454010009765625,
        'matches': [{
            'id': 'Buoys_20171003(1210)',
            'lon': -146.874,
            'lat': 10.871,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 7, 0),
            'sea_water_temperature': 28.6
        }]
    }, {
        'id': '715cd33c-0c17-398f-8d90-9c545d038e76[[0, 4, 14]]',
        'lon': -146.375,
        'lat': 23.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 25.529998779296875,
        'matches': [{
            'id': 'Buoys_20170903(742)',
            'lon': -146.378,
            'lat': 23.617,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 3, 6, 0),
            'sea_water_temperature': 25.6
        }]
    }, {
        'id': '1bfa3b4a-a310-3661-b1d9-2648c0d61a9a[[0, 2, 22]]',
        'lon': -144.375,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 27.81201171875,
        'matches': [{
            'id': 'Buoys_20171008(7269)',
            'lon': -144.371,
            'lat': 15.624,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 3, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': 'fc6f3c08-cefc-36a8-8b2e-9212fe2b4afe[[0, 13, 12]]',
        'lon': -146.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.399993896484375,
        'matches': [{
            'id': 'Buoys_20171003(1210)',
            'lon': -146.874,
            'lat': 10.871,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 7, 0),
            'sea_water_temperature': 28.6
        }]
    }, {
        'id': '96b91d8e-74d9-3856-9e55-c8d4e36dda4a[[0, 2, 17]]',
        'lon': -123.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 27.885009765625,
        'matches': [{
            'id': 'Buoys_20171003(7172)',
            'lon': -123.119,
            'lat': 15.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 10, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': 'ba867010-507b-3c62-8893-140ae07da30b[[0, 15, 16]]',
        'lon': -145.875,
        'lat': 11.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.649993896484375,
        'matches': [{
            'id': 'Buoys_20171010(715)',
            'lon': -145.868,
            'lat': 11.378,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 28.2
        }]
    }, {
        'id': 'c9cb10ab-ebae-3c8b-a498-d7aef5cf53ed[[0, 2, 22]]',
        'lon': -144.375,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.602996826171875,
        'matches': [{
            'id': 'Buoys_20171008(7269)',
            'lon': -144.371,
            'lat': 15.624,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 3, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': 'e812f91f-4796-3109-90a7-e20834b2a4e9[[0, 2, 23]]',
        'lon': -144.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 27.843994140625,
        'matches': [{
            'id': 'Buoys_20171005(7542)',
            'lon': -144.13,
            'lat': 15.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 5, 16, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': 'c9cb10ab-ebae-3c8b-a498-d7aef5cf53ed[[0, 20, 7]]',
        'lon': -148.125,
        'lat': 20.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 26.3070068359375,
        'matches': [{
            'id': 'Buoys_20170920(7966)',
            'lon': -148.131,
            'lat': 20.124,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 20, 12, 0),
            'sea_water_temperature': 26.3
        }]
    }, {
        'id': 'be519f66-de2e-3ffe-a929-be8033717020[[0, 13, 8]]',
        'lon': -147.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.430999755859375,
        'matches': [{
            'id': 'Buoys_20171013(1175)',
            'lon': -147.87,
            'lat': 10.879,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 13, 5, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '8532e3e5-3fdd-304f-a9d7-1382d9658b36[[0, 13, 12]]',
        'lon': -146.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.764007568359375,
        'matches': [{
            'id': 'Buoys_20171003(1210)',
            'lon': -146.874,
            'lat': 10.871,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 7, 0),
            'sea_water_temperature': 28.6
        }]
    }, {
        'id': 'fc6f3c08-cefc-36a8-8b2e-9212fe2b4afe[[0, 23, 4]]',
        'lon': -148.875,
        'lat': 13.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.0989990234375,
        'matches': [{
            'id': 'Buoys_20170918(1164)',
            'lon': -148.875,
            'lat': 13.376,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 18, 7, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': 'e812f91f-4796-3109-90a7-e20834b2a4e9[[0, 2, 22]]',
        'lon': -144.375,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 27.881011962890625,
        'matches': [{
            'id': 'Buoys_20171008(7269)',
            'lon': -144.371,
            'lat': 15.624,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 3, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '35263cc2-f801-3954-b769-38f150ae09bf[[0, 23, 13]]',
        'lon': -139.125,
        'lat': 20.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 24.983001708984375,
        'matches': [{
            'id': 'argo_spurs_20171006(1250)',
            'lon': -139.129,
            'lat': 20.867,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting subsurface profiling float',
            'time': datetime.datetime(2017, 10, 5, 23, 19, 47),
            'sea_water_salinity': 34.97,
            'sea_water_salinity_depth': 4.47223342927,
            'sea_water_temperature': 24.78,
            'sea_water_temperature_depth': 4.47223342927
        }]
    }, {
        'id': '365730d1-98f0-36cb-b78b-44ca18fc9e0d[[0, 2, 11]]',
        'lon': -139.625,
        'lat': 23.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 24.493988037109375,
        'matches': [{
            'id': 'Buoys_20171010(8065)',
            'lon': -139.634,
            'lat': 23.125,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 24.0
        }]
    }, {
        'id': '575795cc-14e5-3ee3-a53d-89edf8f5d018[[0, 14, 14]]',
        'lon': -138.875,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.27301025390625,
        'matches': [{
            'id': 'Buoys_20171008(1812)',
            'lon': -138.874,
            'lat': 11.117,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 4, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'a499e09d-dad6-338a-83ca-499268d58661[[0, 23, 13]]',
        'lon': -139.125,
        'lat': 20.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 25.274993896484375,
        'matches': [{
            'id': 'argo_spurs_20171006(1250)',
            'lon': -139.129,
            'lat': 20.867,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting subsurface profiling float',
            'time': datetime.datetime(2017, 10, 5, 23, 19, 47),
            'sea_water_salinity': 34.97,
            'sea_water_salinity_depth': 4.47223342927,
            'sea_water_temperature': 24.78,
            'sea_water_temperature_depth': 4.47223342927
        }]
    }, {
        'id': 'b6553b38-05fa-3e15-a1ff-f655f2edd0de[[0, 20, 7]]',
        'lon': -148.125,
        'lat': 20.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 26.24798583984375,
        'matches': [{
            'id': 'Buoys_20170920(7966)',
            'lon': -148.131,
            'lat': 20.124,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 20, 12, 0),
            'sea_water_temperature': 26.3
        }]
    }, {
        'id': '38e08fcc-e6ed-3fc7-aac7-efd3ca86dbb6[[0, 5, 8]]',
        'lon': -147.875,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 27.34600830078125,
        'matches': [{
            'id': 'Buoys_20171025(7730)',
            'lon': -147.881,
            'lat': 16.375,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 25, 17, 0),
            'sea_water_temperature': 27.1
        }]
    }, {
        'id': 'be519f66-de2e-3ffe-a929-be8033717020[[0, 18, 24]]',
        'lon': -143.875,
        'lat': 12.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.803009033203125,
        'matches': [{
            'id': 'Buoys_20170829(1970)',
            'lon': -143.879,
            'lat': 12.126,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 8, 29, 6, 0),
            'sea_water_temperature': 28.8
        }]
    }, {
        'id': '38e08fcc-e6ed-3fc7-aac7-efd3ca86dbb6[[0, 2, 23]]',
        'lon': -144.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 27.855987548828125,
        'matches': [{
            'id': 'Buoys_20171005(7542)',
            'lon': -144.13,
            'lat': 15.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 5, 16, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '7415e02d-3eb3-3ab3-b462-22d510016cce[[0, 2, 11]]',
        'lon': -139.625,
        'lat': 23.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 24.3900146484375,
        'matches': [{
            'id': 'Buoys_20171010(8065)',
            'lon': -139.634,
            'lat': 23.125,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 24.0
        }]
    }, {
        'id': '14950099-66a6-31c7-912d-ab4369a7c28e[[0, 2, 17]]',
        'lon': -123.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 27.79901123046875,
        'matches': [{
            'id': 'Buoys_20171003(7172)',
            'lon': -123.119,
            'lat': 15.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 10, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': 'cfa7f14e-8676-3421-94f4-5c9bf14dd8dc[[0, 4, 14]]',
        'lon': -146.375,
        'lat': 23.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 25.3489990234375,
        'matches': [{
            'id': 'Buoys_20170903(742)',
            'lon': -146.378,
            'lat': 23.617,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 3, 6, 0),
            'sea_water_temperature': 25.6
        }]
    }, {
        'id': 'b6553b38-05fa-3e15-a1ff-f655f2edd0de[[0, 2, 22]]',
        'lon': -144.375,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 27.779998779296875,
        'matches': [{
            'id': 'Buoys_20171008(7269)',
            'lon': -144.371,
            'lat': 15.624,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 3, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '1bfa3b4a-a310-3661-b1d9-2648c0d61a9a[[0, 2, 23]]',
        'lon': -144.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 27.847015380859375,
        'matches': [{
            'id': 'Buoys_20171005(7542)',
            'lon': -144.13,
            'lat': 15.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 5, 16, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': 'eccb9006-463b-3554-aa25-0de14224e0c4[[0, 18, 24]]',
        'lon': -143.875,
        'lat': 12.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.81201171875,
        'matches': [{
            'id': 'Buoys_20170829(1970)',
            'lon': -143.879,
            'lat': 12.126,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 8, 29, 6, 0),
            'sea_water_temperature': 28.8
        }]
    }, {
        'id': 'b2143b7a-715f-3f77-97e5-27d714762d03[[0, 23, 13]]',
        'lon': -139.125,
        'lat': 20.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 25.108001708984375,
        'matches': [{
            'id': 'argo_spurs_20171006(1250)',
            'lon': -139.129,
            'lat': 20.867,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting subsurface profiling float',
            'time': datetime.datetime(2017, 10, 5, 23, 19, 47),
            'sea_water_salinity': 34.97,
            'sea_water_salinity_depth': 4.47223342927,
            'sea_water_temperature': 24.78,
            'sea_water_temperature_depth': 4.47223342927
        }]
    }, {
        'id': '7deab803-ca0e-3887-a4f7-09081c35d9fa[[0, 18, 24]]',
        'lon': -143.875,
        'lat': 12.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.76800537109375,
        'matches': [{
            'id': 'Buoys_20170829(1970)',
            'lon': -143.879,
            'lat': 12.126,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 8, 29, 6, 0),
            'sea_water_temperature': 28.8
        }]
    }, {
        'id': 'a499e09d-dad6-338a-83ca-499268d58661[[0, 22, 21]]',
        'lon': -137.125,
        'lat': 20.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 24.839996337890625,
        'matches': [{
            'id': 'Buoys_20170908(558)',
            'lon': -137.13,
            'lat': 20.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 8, 10, 50),
            'sea_water_temperature': 25.0
        }]
    }, {
        'id': '8532e3e5-3fdd-304f-a9d7-1382d9658b36[[0, 10, 4]]',
        'lon': -148.875,
        'lat': 10.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 28.62701416015625,
        'matches': [{
            'id': 'Buoys_20171028(1182)',
            'lon': -148.874,
            'lat': 10.13,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 28, 4, 0),
            'sea_water_temperature': 28.5
        }]
    }, {
        'id': 'fc6f3c08-cefc-36a8-8b2e-9212fe2b4afe[[0, 13, 8]]',
        'lon': -147.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.62200927734375,
        'matches': [{
            'id': 'Buoys_20171013(1175)',
            'lon': -147.87,
            'lat': 10.879,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 13, 5, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '38e08fcc-e6ed-3fc7-aac7-efd3ca86dbb6[[0, 20, 7]]',
        'lon': -148.125,
        'lat': 20.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 26.279998779296875,
        'matches': [{
            'id': 'Buoys_20170920(7966)',
            'lon': -148.131,
            'lat': 20.124,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 20, 12, 0),
            'sea_water_temperature': 26.3
        }]
    }, {
        'id': 'be519f66-de2e-3ffe-a929-be8033717020[[0, 16, 19]]',
        'lon': -145.125,
        'lat': 11.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.87701416015625,
        'matches': [{
            'id': 'Buoys_20171009(942)',
            'lon': -145.129,
            'lat': 11.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 9, 6, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '47693152-084f-3a92-b74c-3cef41a2cc36[[0, 14, 14]]',
        'lon': -138.875,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.3179931640625,
        'matches': [{
            'id': 'Buoys_20171008(1812)',
            'lon': -138.874,
            'lat': 11.117,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 4, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'fc6f3c08-cefc-36a8-8b2e-9212fe2b4afe[[0, 15, 16]]',
        'lon': -145.875,
        'lat': 11.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.615997314453125,
        'matches': [{
            'id': 'Buoys_20171010(715)',
            'lon': -145.868,
            'lat': 11.378,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 28.2
        }]
    }, {
        'id': '9a82c664-0022-3605-a335-22dce8b59608[[0, 5, 10]]',
        'lon': -147.375,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 27.467987060546875,
        'matches': [{
            'id': 'Buoys_20171024(7720)',
            'lon': -147.376,
            'lat': 16.383,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 24, 6, 0),
            'sea_water_temperature': 27.2
        }]
    }, {
        'id': '3c1e5e37-8bf2-3928-bd60-0384cbcfe405[[0, 14, 7]]',
        'lon': -140.625,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 28.657989501953125,
        'matches': [{
            'id': 'Buoys_20171015(1659)',
            'lon': -140.626,
            'lat': 11.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 15, 3, 0),
            'sea_water_temperature': 28.3
        }]
    }, {
        'id': '32cadb31-89e1-3290-b1b6-2c214e9c4af9[[0, 4, 14]]',
        'lon': -146.375,
        'lat': 23.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 25.584014892578125,
        'matches': [{
            'id': 'Buoys_20170903(742)',
            'lon': -146.378,
            'lat': 23.617,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 3, 6, 0),
            'sea_water_temperature': 25.6
        }]
    }, {
        'id': 'd75bd15c-967e-33a4-8485-d3b466f1709d[[0, 22, 21]]',
        'lon': -137.125,
        'lat': 20.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 24.800994873046875,
        'matches': [{
            'id': 'Buoys_20170908(558)',
            'lon': -137.13,
            'lat': 20.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 8, 10, 50),
            'sea_water_temperature': 25.0
        }]
    }, {
        'id': '7f58e6e6-aae9-32af-8bcd-586875f55ef8[[0, 14, 7]]',
        'lon': -140.625,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.677001953125,
        'matches': [{
            'id': 'Buoys_20171015(1659)',
            'lon': -140.626,
            'lat': 11.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 15, 3, 0),
            'sea_water_temperature': 28.3
        }]
    }, {
        'id': '7deab803-ca0e-3887-a4f7-09081c35d9fa[[0, 15, 16]]',
        'lon': -145.875,
        'lat': 11.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.641998291015625,
        'matches': [{
            'id': 'Buoys_20171010(715)',
            'lon': -145.868,
            'lat': 11.378,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 28.2
        }]
    }, {
        'id': 'fc6f3c08-cefc-36a8-8b2e-9212fe2b4afe[[0, 16, 19]]',
        'lon': -145.125,
        'lat': 11.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.78399658203125,
        'matches': [{
            'id': 'Buoys_20171009(942)',
            'lon': -145.129,
            'lat': 11.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 9, 6, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '1c0fc0f8-5007-3806-9f71-068ee7073974[[0, 8, 25]]',
        'lon': -151.125,
        'lat': 17.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 27.3389892578125,
        'matches': [{
            'id': 'Buoys_20171022(485)',
            'lon': -151.13,
            'lat': 17.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 22, 6, 50),
            'sea_water_temperature': 26.4
        }]
    }, {
        'id': 'be519f66-de2e-3ffe-a929-be8033717020[[0, 15, 16]]',
        'lon': -145.875,
        'lat': 11.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.57598876953125,
        'matches': [{
            'id': 'Buoys_20171010(715)',
            'lon': -145.868,
            'lat': 11.378,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 28.2
        }]
    }, {
        'id': 'ecfdf049-4d0d-3e0f-a435-a106dcee89b1[[0, 2, 17]]',
        'lon': -123.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 27.87298583984375,
        'matches': [{
            'id': 'Buoys_20171003(7172)',
            'lon': -123.119,
            'lat': 15.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 10, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': '202574c8-0266-3947-8f35-12c7ed733cd7[[0, 2, 11]]',
        'lon': -139.625,
        'lat': 23.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 24.58099365234375,
        'matches': [{
            'id': 'Buoys_20171010(8065)',
            'lon': -139.634,
            'lat': 23.125,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 24.0
        }]
    }, {
        'id': 'b6553b38-05fa-3e15-a1ff-f655f2edd0de[[0, 5, 10]]',
        'lon': -147.375,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 27.4630126953125,
        'matches': [{
            'id': 'Buoys_20171024(7720)',
            'lon': -147.376,
            'lat': 16.383,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 24, 6, 0),
            'sea_water_temperature': 27.2
        }]
    }, {
        'id': 'ba867010-507b-3c62-8893-140ae07da30b[[0, 23, 4]]',
        'lon': -148.875,
        'lat': 13.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.131988525390625,
        'matches': [{
            'id': 'Buoys_20170918(1164)',
            'lon': -148.875,
            'lat': 13.376,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 18, 7, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': '9a82c664-0022-3605-a335-22dce8b59608[[0, 2, 22]]',
        'lon': -144.375,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 27.88299560546875,
        'matches': [{
            'id': 'Buoys_20171008(7269)',
            'lon': -144.371,
            'lat': 15.624,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 3, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '7f58e6e6-aae9-32af-8bcd-586875f55ef8[[0, 13, 16]]',
        'lon': -138.375,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.191986083984375,
        'matches': [{
            'id': 'Buoys_20170930(1889)',
            'lon': -138.367,
            'lat': 10.872,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 30, 15, 0),
            'sea_water_temperature': 28.1
        }]
    }, {
        'id': 'b6553b38-05fa-3e15-a1ff-f655f2edd0de[[0, 2, 23]]',
        'lon': -144.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 27.845001220703125,
        'matches': [{
            'id': 'Buoys_20171005(7542)',
            'lon': -144.13,
            'lat': 15.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 5, 16, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '575795cc-14e5-3ee3-a53d-89edf8f5d018[[0, 14, 7]]',
        'lon': -140.625,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.635009765625,
        'matches': [{
            'id': 'Buoys_20171015(1659)',
            'lon': -140.626,
            'lat': 11.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 15, 3, 0),
            'sea_water_temperature': 28.3
        }]
    }, {
        'id': 'db173d93-895d-357d-8d30-fee57540a2ef[[0, 13, 16]]',
        'lon': -138.375,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.139007568359375,
        'matches': [{
            'id': 'Buoys_20170930(1889)',
            'lon': -138.367,
            'lat': 10.872,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 30, 15, 0),
            'sea_water_temperature': 28.1
        }]
    }, {
        'id': '38e08fcc-e6ed-3fc7-aac7-efd3ca86dbb6[[0, 2, 22]]',
        'lon': -144.375,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 27.68499755859375,
        'matches': [{
            'id': 'Buoys_20171008(7269)',
            'lon': -144.371,
            'lat': 15.624,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 3, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '976b8267-6c3e-3154-8354-a7bd47d98bfe[[0, 22, 21]]',
        'lon': -137.125,
        'lat': 20.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 25.180999755859375,
        'matches': [{
            'id': 'Buoys_20170908(558)',
            'lon': -137.13,
            'lat': 20.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 8, 10, 50),
            'sea_water_temperature': 25.0
        }]
    }, {
        'id': '3c1e5e37-8bf2-3928-bd60-0384cbcfe405[[0, 13, 16]]',
        'lon': -138.375,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.985992431640625,
        'matches': [{
            'id': 'Buoys_20170930(1889)',
            'lon': -138.367,
            'lat': 10.872,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 30, 15, 0),
            'sea_water_temperature': 28.1
        }]
    }, {
        'id': 'ba867010-507b-3c62-8893-140ae07da30b[[0, 13, 8]]',
        'lon': -147.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.553985595703125,
        'matches': [{
            'id': 'Buoys_20171013(1175)',
            'lon': -147.87,
            'lat': 10.879,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 13, 5, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '8532e3e5-3fdd-304f-a9d7-1382d9658b36[[0, 13, 8]]',
        'lon': -147.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 28.847991943359375,
        'matches': [{
            'id': 'Buoys_20171013(1175)',
            'lon': -147.87,
            'lat': 10.879,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 13, 5, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '7deab803-ca0e-3887-a4f7-09081c35d9fa[[0, 13, 8]]',
        'lon': -147.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 28.480010986328125,
        'matches': [{
            'id': 'Buoys_20171013(1175)',
            'lon': -147.87,
            'lat': 10.879,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 13, 5, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'ac22e67b-95f5-3c51-ae05-1396b838276c[[0, 14, 14]]',
        'lon': -138.875,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.13800048828125,
        'matches': [{
            'id': 'Buoys_20171008(1812)',
            'lon': -138.874,
            'lat': 11.117,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 8, 4, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'b6553b38-05fa-3e15-a1ff-f655f2edd0de[[0, 5, 8]]',
        'lon': -147.875,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 27.39801025390625,
        'matches': [{
            'id': 'Buoys_20171025(7730)',
            'lon': -147.881,
            'lat': 16.375,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 25, 17, 0),
            'sea_water_temperature': 27.1
        }]
    }, {
        'id': 'c9cb10ab-ebae-3c8b-a498-d7aef5cf53ed[[0, 5, 10]]',
        'lon': -147.375,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.39898681640625,
        'matches': [{
            'id': 'Buoys_20171024(7720)',
            'lon': -147.376,
            'lat': 16.383,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 24, 6, 0),
            'sea_water_temperature': 27.2
        }]
    }, {
        'id': '1b345618-e825-3add-8c32-4cd537603c62[[0, 2, 11]]',
        'lon': -139.625,
        'lat': 23.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 24.555999755859375,
        'matches': [{
            'id': 'Buoys_20171010(8065)',
            'lon': -139.634,
            'lat': 23.125,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 24.0
        }]
    }, {
        'id': 'e812f91f-4796-3109-90a7-e20834b2a4e9[[0, 5, 10]]',
        'lon': -147.375,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 27.47100830078125,
        'matches': [{
            'id': 'Buoys_20171024(7720)',
            'lon': -147.376,
            'lat': 16.383,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 24, 6, 0),
            'sea_water_temperature': 27.2
        }]
    }, {
        'id': 'b2143b7a-715f-3f77-97e5-27d714762d03[[0, 22, 21]]',
        'lon': -137.125,
        'lat': 20.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 25.0260009765625,
        'matches': [{
            'id': 'Buoys_20170908(558)',
            'lon': -137.13,
            'lat': 20.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 8, 10, 50),
            'sea_water_temperature': 25.0
        }]
    }, {
        'id': '9a82c664-0022-3605-a335-22dce8b59608[[0, 2, 23]]',
        'lon': -144.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 27.967987060546875,
        'matches': [{
            'id': 'Buoys_20171005(7542)',
            'lon': -144.13,
            'lat': 15.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 5, 16, 0),
            'sea_water_temperature': 27.7
        }]
    }, {
        'id': '976b8267-6c3e-3154-8354-a7bd47d98bfe[[0, 23, 13]]',
        'lon': -139.125,
        'lat': 20.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 25.36700439453125,
        'matches': [{
            'id': 'argo_spurs_20171006(1250)',
            'lon': -139.129,
            'lat': 20.867,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting subsurface profiling float',
            'time': datetime.datetime(2017, 10, 5, 23, 19, 47),
            'sea_water_salinity': 34.97,
            'sea_water_salinity_depth': 4.47223342927,
            'sea_water_temperature': 24.78,
            'sea_water_temperature_depth': 4.47223342927
        }]
    }, {
        'id': '2bb065b9-a57f-3c02-bd07-88a5859a08c8[[0, 2, 17]]',
        'lon': -123.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.902008056640625,
        'matches': [{
            'id': 'Buoys_20171003(7172)',
            'lon': -123.119,
            'lat': 15.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 10, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': '8532e3e5-3fdd-304f-a9d7-1382d9658b36[[0, 16, 19]]',
        'lon': -145.125,
        'lat': 11.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 28.813995361328125,
        'matches': [{
            'id': 'Buoys_20171009(942)',
            'lon': -145.129,
            'lat': 11.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 9, 6, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '9a82c664-0022-3605-a335-22dce8b59608[[0, 20, 7]]',
        'lon': -148.125,
        'lat': 20.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 26.532989501953125,
        'matches': [{
            'id': 'Buoys_20170920(7966)',
            'lon': -148.131,
            'lat': 20.124,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 20, 12, 0),
            'sea_water_temperature': 26.3
        }]
    }, {
        'id': 'eccb9006-463b-3554-aa25-0de14224e0c4[[0, 16, 19]]',
        'lon': -145.125,
        'lat': 11.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.891998291015625,
        'matches': [{
            'id': 'Buoys_20171009(942)',
            'lon': -145.129,
            'lat': 11.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 9, 6, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': 'c63f3a16-42ac-3185-b8d2-25e26bf9401c[[0, 22, 21]]',
        'lon': -137.125,
        'lat': 20.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 25.13299560546875,
        'matches': [{
            'id': 'Buoys_20170908(558)',
            'lon': -137.13,
            'lat': 20.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 8, 10, 50),
            'sea_water_temperature': 25.0
        }]
    }, {
        'id': '575795cc-14e5-3ee3-a53d-89edf8f5d018[[0, 13, 16]]',
        'lon': -138.375,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 28.272003173828125,
        'matches': [{
            'id': 'Buoys_20170930(1889)',
            'lon': -138.367,
            'lat': 10.872,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 30, 15, 0),
            'sea_water_temperature': 28.1
        }]
    }, {
        'id': 'eccb9006-463b-3554-aa25-0de14224e0c4[[0, 13, 8]]',
        'lon': -147.875,
        'lat': 10.875,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 28.447998046875,
        'matches': [{
            'id': 'Buoys_20171013(1175)',
            'lon': -147.87,
            'lat': 10.879,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 13, 5, 0),
            'sea_water_temperature': 28.4
        }]
    }, {
        'id': '8532e3e5-3fdd-304f-a9d7-1382d9658b36[[0, 15, 16]]',
        'lon': -145.875,
        'lat': 11.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 28.597015380859375,
        'matches': [{
            'id': 'Buoys_20171010(715)',
            'lon': -145.868,
            'lat': 11.378,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 10, 16, 0),
            'sea_water_temperature': 28.2
        }]
    }, {
        'id': '35263cc2-f801-3954-b769-38f150ae09bf[[0, 22, 21]]',
        'lon': -137.125,
        'lat': 20.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 24.865997314453125,
        'matches': [{
            'id': 'Buoys_20170908(558)',
            'lon': -137.13,
            'lat': 20.632,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 8, 10, 50),
            'sea_water_temperature': 25.0
        }]
    }, {
        'id': 'd14b58a0-ed21-3ed9-a95e-2915ffc93676[[0, 8, 25]]',
        'lon': -151.125,
        'lat': 17.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 27.37298583984375,
        'matches': [{
            'id': 'Buoys_20171022(485)',
            'lon': -151.13,
            'lat': 17.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 22, 6, 50),
            'sea_water_temperature': 26.4
        }]
    }, {
        'id': 'c9cb10ab-ebae-3c8b-a498-d7aef5cf53ed[[0, 5, 8]]',
        'lon': -147.875,
        'lat': 16.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 27.2869873046875,
        'matches': [{
            'id': 'Buoys_20171025(7730)',
            'lon': -147.881,
            'lat': 16.375,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 25, 17, 0),
            'sea_water_temperature': 27.1
        }]
    }, {
        'id': '8532e3e5-3fdd-304f-a9d7-1382d9658b36[[0, 23, 4]]',
        'lon': -148.875,
        'lat': 13.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 28.196990966796875,
        'matches': [{
            'id': 'Buoys_20170918(1164)',
            'lon': -148.875,
            'lat': 13.376,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 18, 7, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': '17905d2a-ccdd-3d7b-91a7-388c0cc6ef58[[0, 2, 17]]',
        'lon': -123.125,
        'lat': 15.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 27.8389892578125,
        'matches': [{
            'id': 'Buoys_20171003(7172)',
            'lon': -123.119,
            'lat': 15.629,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 3, 10, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': 'be519f66-de2e-3ffe-a929-be8033717020[[0, 23, 4]]',
        'lon': -148.875,
        'lat': 13.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 24, 9, 0),
        'sea_surface_foundation_temperature': 28.1669921875,
        'matches': [{
            'id': 'Buoys_20170918(1164)',
            'lon': -148.875,
            'lat': 13.376,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 18, 7, 0),
            'sea_water_temperature': 27.9
        }]
    }, {
        'id': 'dfee49fc-11d5-3bf3-b515-50e698a80f55[[0, 4, 14]]',
        'lon': -146.375,
        'lat': 23.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 27, 9, 0),
        'sea_surface_foundation_temperature': 25.6090087890625,
        'matches': [{
            'id': 'Buoys_20170903(742)',
            'lon': -146.378,
            'lat': 23.617,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 3, 6, 0),
            'sea_water_temperature': 25.6
        }]
    }, {
        'id': 'e812f91f-4796-3109-90a7-e20834b2a4e9[[0, 20, 7]]',
        'lon': -148.125,
        'lat': 20.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 25, 9, 0),
        'sea_surface_foundation_temperature': 26.364013671875,
        'matches': [{
            'id': 'Buoys_20170920(7966)',
            'lon': -148.131,
            'lat': 20.124,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 20, 12, 0),
            'sea_water_temperature': 26.3
        }]
    }, {
        'id': 'e5f90fe2-0de1-323e-9bed-a26011d52cbf[[0, 3, 25]]',
        'lon': -143.625,
        'lat': 23.375,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 29, 9, 0),
        'sea_surface_foundation_temperature': 25.22198486328125,
        'matches': [{
            'id': 'Buoys_20171029(225)',
            'lon': -143.62,
            'lat': 23.372,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 29, 3, 0),
            'sea_water_temperature': 24.0
        }]
    }, {
        'id': 'fc6f3c08-cefc-36a8-8b2e-9212fe2b4afe[[0, 10, 4]]',
        'lon': -148.875,
        'lat': 10.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.670013427734375,
        'matches': [{
            'id': 'Buoys_20171028(1182)',
            'lon': -148.874,
            'lat': 10.13,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 28, 4, 0),
            'sea_water_temperature': 28.5
        }]
    }, {
        'id': 'ac22e67b-95f5-3c51-ae05-1396b838276c[[0, 14, 7]]',
        'lon': -140.625,
        'lat': 11.125,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 28, 9, 0),
        'sea_surface_foundation_temperature': 28.699005126953125,
        'matches': [{
            'id': 'Buoys_20171015(1659)',
            'lon': -140.626,
            'lat': 11.127,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 10, 15, 3, 0),
            'sea_water_temperature': 28.3
        }]
    }, {
        'id': '83c72b56-824b-3dcd-be76-9bb25927c6ce[[0, 4, 14]]',
        'lon': -146.375,
        'lat': 23.625,
        'source': 'MUR25-JPL-L4-GLOB-v04.2',
        'device': 'radiometers',
        'platform': 'orbiting satellite',
        'time': datetime.datetime(2017, 9, 26, 9, 0),
        'sea_surface_foundation_temperature': 25.64300537109375,
        'matches': [{
            'id': 'Buoys_20170903(742)',
            'lon': -146.378,
            'lat': 23.617,
            'source': 'spurs2',
            'device': 'CTD',
            'platform': 'drifting surface float',
            'time': datetime.datetime(2017, 9, 3, 6, 0),
            'sea_water_temperature': 25.6
        }]
    }
]
