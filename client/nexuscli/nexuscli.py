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
"""
This module provides a native python client interface to the NEXUS (https://github.com/dataplumber/nexus) 
webservice API.

Usage:

    import nexuscli
    
    nexuscli.set_target("http://nexus-webapp:8083")
    nexuscli.dataset_list()
    
"""
import requests
import numpy as np
from datetime import datetime
from collections import namedtuple, OrderedDict
from pytz import UTC

__pdoc__ = {}
TimeSeries = namedtuple('TimeSeries', ('dataset', 'time', 'mean', 'standard_deviation', 'count', 'minimum', 'maximum'))
TimeSeries.__doc__ = '''\
An object containing Time Series arrays.
'''
__pdoc__['TimeSeries.dataset'] = "Name of the Dataset"
__pdoc__['TimeSeries.time'] = "`numpy` array containing times as `datetime` objects"
__pdoc__['TimeSeries.mean'] = "`numpy` array containing means"
__pdoc__['TimeSeries.standard_deviation'] = "`numpy` array containing standard deviations"
__pdoc__['TimeSeries.count'] = "`numpy` array containing counts"
__pdoc__['TimeSeries.minimum'] = "`numpy` array containing minimums"
__pdoc__['TimeSeries.maximum'] = "`numpy` array containing maximums"

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

target = 'http://localhost:8083'

session = requests.session()


def set_target(url):
    """
    Set the URL for the NEXUS webapp endpoint.  
    
    __url__ URL for NEXUS webapp endpoint   
    __return__ None
    """
    global target
    target = url


def dataset_list():
    """
    Get a list of datasets and the start and end time for each.
    
    __return__ list of datasets. Each entry in the list contains `shortname`, `start`, and `end`
    """
    response = session.get("{}/list".format(target))
    data = response.json()

    list_response = []
    for dataset in data:
        dataset['start'] = datetime.utcfromtimestamp(dataset['start'] / 1000).strftime(ISO_FORMAT)
        dataset['end'] = datetime.utcfromtimestamp(dataset['end'] / 1000).strftime(ISO_FORMAT)

        ordered_dict = OrderedDict()
        ordered_dict['shortName'] = dataset['shortName']
        ordered_dict['start'] = dataset['start']
        ordered_dict['end'] = dataset['end']
        list_response.append(ordered_dict)

    return list_response


def daily_difference_average(dataset, bounding_box, start_datetime, end_datetime):
    """
    Generate an anomaly Time series for a given dataset, bounding box, and timeframe.
    
    __dataset__ Name of the dataset as a String  
    __bounding_box__ Bounding box for area of interest as a `shapely.geometry.polygon.Polygon`  
    __start_datetime__ Start time as a `datetime.datetime`  
    __end_datetime__ End time as a `datetime.datetime`  
    
    __return__ List of `nexuscli.nexuscli.TimeSeries` namedtuples
    """
    url = "{}/dailydifferenceaverage_spark?".format(target)

    params = {
        'dataset': dataset,
        'climatology': "{}_CLIM".format(dataset),
        'b': ','.join(str(b) for b in bounding_box.bounds),
        'startTime': start_datetime.strftime(ISO_FORMAT),
        'endTime': end_datetime.strftime(ISO_FORMAT),
    }

    response = session.get(url, params=params)
    response.raise_for_status()
    response = response.json()

    data = np.array(response['data']).flatten()

    assert len(data) > 0, "No data found in {} between {} and {} for Datasets {}.".format(bounding_box.wkt,
                                                                                          start_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          end_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          dataset)

    time_series_result = []

    key_to_index = {k: x for x, k in enumerate(data[0].keys())}

    time_series_data = np.array([tuple(each.values()) for each in [entry for entry in data]])

    if len(time_series_data) > 0:
        time_series_result.append(
            TimeSeries(
                dataset=dataset,
                time=np.array([datetime.utcfromtimestamp(t).replace(tzinfo=UTC) for t in
                               time_series_data[:, key_to_index['time']]]),
                mean=time_series_data[:, key_to_index['mean']],
                standard_deviation=time_series_data[:, key_to_index['std']],
                count=None,
                minimum=None,
                maximum=None,
            )
        )

    return time_series_result


def time_series(datasets, bounding_box, start_datetime, end_datetime, spark=False):
    """
    Send a request to NEXUS to calculate a time series.
    
    __datasets__ Sequence (max length 2) of the name of the dataset(s)  
    __bounding_box__ Bounding box for area of interest as a `shapely.geometry.polygon.Polygon`  
    __start_datetime__ Start time as a `datetime.datetime`  
    __end_datetime__ End time as a `datetime.datetime`  
    __spark__ Optionally use spark. Default: `False`
    
    __return__ List of `nexuscli.nexuscli.TimeSeries` namedtuples
    """

    if isinstance(datasets, str):
        datasets = [datasets]

    assert 0 < len(datasets) <= 2, "datasets must be a sequence of 1 or 2 items"

    params = {
        'ds': ','.join(datasets),
        'b': ','.join(str(b) for b in bounding_box.bounds),
        'startTime': start_datetime.strftime(ISO_FORMAT),
        'endTime': end_datetime.strftime(ISO_FORMAT),
    }

    if spark:
        url = "{}/timeSeriesSpark?".format(target)
        params['spark'] = "mesos,16,32"
    else:
        url = "{}/stats?".format(target)

    response = session.get(url, params=params)
    response.raise_for_status()
    response = response.json()

    data = np.array(response['data']).flatten()

    assert len(data) > 0, "No data found in {} between {} and {} for Datasets {}.".format(bounding_box.wkt,
                                                                                          start_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          end_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          datasets)

    time_series_result = []

    for i in range(0, len(response['meta'])):
        key_to_index = {k: x for x, k in enumerate(data[0].keys())}

        time_series_data = np.array([tuple(each.values()) for each in [entry for entry in data if entry['ds'] == i]])

        if len(time_series_data) > 0:
            time_series_result.append(
                TimeSeries(
                    dataset=response['meta'][i]['shortName'],
                    time=np.array([datetime.utcfromtimestamp(t).replace(tzinfo=UTC) for t in
                                   time_series_data[:, key_to_index['time']]]),
                    mean=time_series_data[:, key_to_index['mean']],
                    standard_deviation=time_series_data[:, key_to_index['std']],
                    count=time_series_data[:, key_to_index['cnt']],
                    minimum=time_series_data[:, key_to_index['min']],
                    maximum=time_series_data[:, key_to_index['max']],
                )
            )

    return time_series_result
