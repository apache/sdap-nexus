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
Module for querying CDMS In-Situ API
"""
import logging
import requests
from datetime import datetime
from webservice.algorithms.doms import config as insitu_endpoints
from pydantic import BaseModel, Field, AliasChoices, ConfigDict, field_validator
from typing import List, Optional, Dict, Union


class InsituRecord(BaseModel):
    time: str
    latitude: float
    longitude: float
    depth: float
    platform: Optional[int] = None
    device: Optional[float]
    mission: Optional[str] = None
    metadata: str = Field(validation_alias=AliasChoices('meta', 'metadata'))
    provider: Optional[str] = None
    project: Optional[str] = None
    platform_code: Optional[str] = None
    job_id: Optional[str] = None

    model_config = ConfigDict(
        extra='allow',
    )

    @field_validator('platform', mode='before')
    @classmethod
    def transform(cls, raw_platform: Union[int, Dict[str, str]]) -> Optional[int]:
        if isinstance(raw_platform, dict):
            return raw_platform.get('code')
        return raw_platform

    def get_variables(self):
        return self.model_extra


class InsituResult(BaseModel):
    last: str = str
    prev: str = str
    next: str = str
    first: str = str
    total_results: int = Field(validation_alias=AliasChoices('total', 'totalResults'))
    results: List[InsituRecord]


def query_insitu_edge(dataset, params, session=None, stats=True):
    params.update({
        'stats': str(stats).lower(),
    })

    return query_insitu_api(session, insitu_endpoints.getEndpointByName(dataset)['url'], params)  # TODO convert to shared entity


def query_insitu_parquet(dataset, params, session=None, **kwargs):
    provider = insitu_endpoints.get_provider_name(dataset)
    project = insitu_endpoints.get_project_name(dataset)

    params.update({
        'provider': provider,
        'project': project
    })

    return query_insitu_api(session, insitu_endpoints.getEndpoint(provider, dataset), params)  # TODO convert to shared entity


def query_insitu_api(session, endpoint, params):
    insitu_response = {}

    # Page through all insitu results
    next_page_url = endpoint
    while next_page_url is not None and next_page_url != 'NA':
        if session is not None:
            response = session.get(next_page_url, params=params)
        else:
            response = requests.get(next_page_url, params=params)

        logging.info(f'Insitu request {response.url}')
        print(f'Insitu request {response.url}')

        response.raise_for_status()
        insitu_page_response = response.json()

        if not insitu_response:
            insitu_response = insitu_page_response
        else:
            insitu_response['results'].extend(insitu_page_response['results'])

        next_page_url = insitu_page_response.get('next', None)
        params = {}  # Remove params, they are already included in above URL

    return insitu_response


def get_query_insitu_func():
    # return query_insitu_parquet  # TODO upgrade this logic -- how to determine which API to query? Could add a new field to config
    return query_insitu_edge  # TODO upgrade this logic -- how to determine which API to query? Could add a new field to config


def query_insitu_schema():
    """
    Query the "cdms_schema" insitu endpoint. This will return the JSON
    schema used to construct the data, which will contain useful
    metadata
    """
    schema_endpoint = insitu_endpoints.getSchemaEndpoint()
    logging.info("Querying schema")
    response = requests.get(schema_endpoint)
    response.raise_for_status()
    return response.json()


def query_insitu(dataset, variable, start_time, end_time, bbox, platform, depth_min, depth_max, items_per_page=20000, session=None):
    """
    Query insitu API, page through results, and aggregate
    """
    try:
        start_time = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        end_time = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    params = {
        'startTime': start_time,
        'endTime': end_time,
        'bbox': bbox,
        'minDepth': depth_min,
        'maxDepth': depth_max,
        'platform': platform,
        'itemsPerPage': items_per_page
    }

    if variable is not None:
        params['variable'] = variable

    query_insitu_func = get_query_insitu_func()

    return query_insitu_func(
        dataset=dataset,
        params=params,
        session=session
    )
