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
import fetchedgeimpl


def getCount(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms=None):
    return fetchedgeimpl.getCount(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms)


def __fetchSingleDataSource(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms=None):
    return fetchedgeimpl.fetch(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms)


def __fetchMultipleDataSource(endpoints, startTime, endTime, bbox, depth_min, depth_max, platforms=None):
    data = []
    for endpoint in endpoints:
        dataSingleSource = __fetchSingleDataSource(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms)
        data = data + dataSingleSource
    return data


def fetchData(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms=None):
    if type(endpoint) == list:
        return __fetchMultipleDataSource(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms)
    else:
        return __fetchSingleDataSource(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms)


def getValues(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms=None, placeholders=False):
    return fetchedgeimpl.getValues(endpoint, startTime, endTime, bbox, depth_min, depth_max, platforms, placeholders)


if __name__ == "__main__":
    pass
