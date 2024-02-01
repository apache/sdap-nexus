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

import json
from enum import Enum


ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


class ExecutionStatus(Enum):
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


def construct_job_status(job_state, created, updated, execution_id, params, host, message='',
                         filename=None):
    filename_param = f'&filename={filename}' if filename else ''
    return {
        'status': job_state.value,
        'message': message,
        'createdAt': created,
        'updatedAt': updated,
        'links': [{
            'href': f'{host}/job?id={execution_id}{filename_param}',
            'title': 'Get job status - the current page',
            'type': 'application/json',
            'rel': 'self'
        }],
        'params': params,
        'executionID': execution_id
    }


def construct_done(status, created, completed, execution_id, params, host,
                   num_primary_matched, num_secondary_matched, num_unique_secondaries, filename):
    job_body = construct_job_status(
        status,
        created,
        completed,
        execution_id,
        params,
        host,
        filename=filename
    )
    # Add stats to body
    job_body['totalPrimaryMatched'] = num_primary_matched
    job_body['totalSecondaryMatched'] = num_secondary_matched
    job_body['averageSecondaryMatched'] = round(num_secondary_matched/num_primary_matched) \
        if num_primary_matched > 0 else 0
    job_body['totalUniqueSecondaryMatched'] = num_unique_secondaries

    filename_param = f'&filename={filename}' if filename else ''

    # Construct urls
    formats = [
        ('CSV', 'text/csv'),
        ('JSON', 'application/json'),
        ('NETCDF', 'binary/octet-stream')
    ]
    job_body['links'].append({
        'href': f'{host}/cdmscatalog/{execution_id}',
        'title': 'STAC Catalog for execution results',
        'type': 'application/json',
        'rel': 'stac'
    })
    data_links = [{
        'href': f'{host}/cdmsresults?id={execution_id}&output={output_format}{filename_param}',
        'title': f'Download {output_format} results',
        'type': mime,
        'rel': 'data'
    } for output_format, mime in formats]
    job_body['links'].extend(data_links)
    return job_body


def construct_running(status, created, execution_id, params, host, filename):
    job_body = construct_job_status(
        status,
        created,
        None,
        execution_id,
        params,
        host,
        filename=filename
    )
    job_body['links'].append({
        'href': f'{host}/job/cancel?id={execution_id}',
        'title': 'Cancel the job',
        'rel': 'cancel'
    })
    return job_body


def construct_error(status, created, completed, execution_id, message, params, host, filename):
    return construct_job_status(
        status,
        created,
        completed,
        execution_id,
        params,
        host,
        message,
        filename=filename
    )


def construct_cancelled(status, created, completed, execution_id, params, host, filename):
    return construct_job_status(
        status,
        created,
        completed,
        execution_id,
        params,
        host,
        filename=filename
    )


class NexusExecutionResults:
    def __init__(self, status=None, created=None, completed=None, execution_id=None, message='',
                 params=None, host=None, status_code=200, num_primary_matched=None,
                 num_secondary_matched=None, num_unique_secondaries=None, filename=None):
        self.status_code = status_code
        self.status = status
        self.created = created
        self.completed = completed
        self.execution_id = execution_id
        self.message = message
        self.execution_params = params
        self.host = host
        self.num_primary_matched = num_primary_matched
        self.num_secondary_matched = num_secondary_matched
        self.num_unique_secondaries = num_unique_secondaries
        self.filename = filename

    def toJson(self):
        params = {
            'status': self.status,
            'created': self.created,
            'execution_id': self.execution_id,
            'params': self.execution_params,
            'host': self.host,
            'filename': self.filename
        }
        if self.status == ExecutionStatus.SUCCESS:
            params['completed'] = self.completed
            params['num_primary_matched'] = self.num_primary_matched
            params['num_secondary_matched'] = self.num_secondary_matched
            params['num_unique_secondaries'] = self.num_unique_secondaries
            construct = construct_done
        elif self.status == ExecutionStatus.RUNNING:
            construct = construct_running
        elif self.status == ExecutionStatus.FAILED:
            params['completed'] = self.completed
            params['message'] = self.message
            construct = construct_error
        elif self.status == ExecutionStatus.CANCELLED:
            params['completed'] = self.completed
            construct = construct_cancelled
        else:
            # Raise error -- job state is invalid
            raise ValueError('Unable to fetch status for execution {}', self.execution_id)

        job_details = construct(**params)
        return json.dumps(job_details, indent=4, default=str)
