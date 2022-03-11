import datetime
import uuid
import csv
from webservice.algorithms.doms.BaseDomsHandler import DomsCSVFormatter


def test_csv():
    """
    Test that CSV is constructed properly given result, params, and
    details.
    """
    test_execution_id = str(uuid.uuid4())
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
