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
This module provides a native python client interface to the NEXUS (https://github.com/apache/incubator-sdap-nexus)
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

Point = namedtuple('Point', ('time', 'latitude', 'longitude', 'variable'))
Point.__doc__ = '''\
An object containing Point attributes.
'''
__pdoc__['Point.time'] = "time value as `datetime` object"
__pdoc__['Point.latitude'] = "latitude value"
__pdoc__['Point.longitude'] = "longitude value"
__pdoc__['Point.variable'] = "dictionary of variable values"

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PYTHON32_ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

target = 'http://localhost:8083'

session = requests.session()


def set_target(url, use_session=True):
    """
    Set the URL for the NEXUS webapp endpoint.  
    
    __url__ URL for NEXUS webapp endpoint   
    __return__ None
    """
    global target
    target = url
    print("Target set to {}".format(target))

    if not use_session:
        global session
        session = requests


def dataset_list():
    """
    Get a list of datasets and the start and end time for each.
    
    __return__ list of datasets. Each entry in the list contains `shortname`, `start`, and `end`
    """
    response = session.get("{}/list".format(target))
    data = response.json()

    list_response = []
    for dataset in data:
        dataset['start'] = dataset['iso_start']
        dataset['end'] = dataset['iso_end']

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


def time_series(datasets, bounding_box, start_datetime, end_datetime, spark=True):
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
            if 'iso_time' in key_to_index:
                time_series_result.append(
                    TimeSeries(
                        dataset=response['meta'][i]['shortName'],
                        time=np.array([datetime.strptime(t, PYTHON32_ISO_FORMAT) for t in
                                       time_series_data[:, key_to_index['iso_time']]]),
                        mean=np.array(time_series_data[:, key_to_index['mean']], dtype=float),
                        standard_deviation=np.array(time_series_data[:, key_to_index['std']], dtype=float),
                        count=np.array(time_series_data[:, key_to_index['cnt']], dtype=int),
                        minimum=np.array(time_series_data[:, key_to_index['min']], dtype=float),
                        maximum=np.array(time_series_data[:, key_to_index['max']], dtype=float),
                    )
                )
            else:
                time_series_result.append(
                    TimeSeries(
                        dataset=response['meta'][i]['shortName'],
                        time=np.array([datetime.utcfromtimestamp(int(t)).replace(tzinfo=UTC) for t in
                                       time_series_data[:, key_to_index['time']]]),
                        mean=np.array(time_series_data[:, key_to_index['mean']], dtype=float),
                        standard_deviation=np.array(time_series_data[:, key_to_index['std']], dtype=float),
                        count=np.array(time_series_data[:, key_to_index['cnt']], dtype=int),
                        minimum=np.array(time_series_data[:, key_to_index['min']], dtype=float),
                        maximum=np.array(time_series_data[:, key_to_index['max']], dtype=float),
                    )
                )

    return time_series_result


def subset(dataset, bounding_box, start_datetime, end_datetime, parameter, metadata_filter):
    """
    Fetches point values for a given dataset and geographical area or metadata criteria and time range.

    __dataset__ Name of the dataset as a String  
    __bounding_box__ Bounding box for area of interest as a `shapely.geometry.polygon.Polygon`  
    __start_datetime__ Start time as a `datetime.datetime`  
    __end_datetime__ End time as a `datetime.datetime`  
    __parameter__ The parameter of interest. One of 'sst', 'sss', 'wind' or None  
    __metadata_filter__ List of key:value String metadata criteria  

    __return__ List of `nexuscli.nexuscli.Point` namedtuples
    """
    url = "{}/datainbounds?".format(target)

    params = {
        'ds': dataset,
        'startTime': start_datetime.strftime(ISO_FORMAT),
        'endTime': end_datetime.strftime(ISO_FORMAT),
        'parameter': parameter,
    }
    if bounding_box:
        params['b'] = ','.join(str(b) for b in bounding_box.bounds)
    else:
        if metadata_filter and len(metadata_filter) > 0:
            params['metadataFilter'] = metadata_filter

    response = session.get(url, params=params)
    response.raise_for_status()
    response = response.json()

    data = np.array(response['data']).flatten()

    assert len(data) > 0, "No data found in {} between {} and {} for Datasets {}.".format(bounding_box.wkt if bounding_box is not None else metadata_filter,
                                                                                          start_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          end_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          dataset)

    subset_result = []
    for d in data:
        subset_result.append(
            Point(
                time=datetime.utcfromtimestamp(d['time']).replace(tzinfo=UTC),
                longitude=d['longitude'],
                latitude=d['latitude'],
                variable=d['data'][0]
            )
        )

    return subset_result
