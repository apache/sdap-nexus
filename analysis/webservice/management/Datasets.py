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

from yaml import load
import json
from webservice.NexusHandler import nexus_handler
from nexustiles.nexustiles import NexusTileService
from webservice.webmodel import NexusRequestObject, NexusProcessingException
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class DatasetManagement:
    @classmethod
    def validate(cls):
        pass

    @staticmethod
    def parse_config(request: NexusRequestObject):
        content_type = request.get_headers()['Content-Type']

        if content_type in ['application/json', 'application/x-json']:
            return json.loads(request.get_request_body())
        elif content_type == 'application/yaml':
            return load(request.get_request_body(), Loader=Loader)
        else:
            raise NexusProcessingException(reason='Invalid Content-Type header', code=400)


@nexus_handler
class DatasetAdd(DatasetManagement):
    name = 'Add dataset'
    path = '/datasets/add'
    description = "Add new dataset to running SDAP instance"

    def __init__(self, **args):
        pass

    def calc(self, request: NexusRequestObject, **args):
        # print('CALC')
        try:
            config = DatasetManagement.parse_config(request)
        except Exception as e:
            raise NexusProcessingException(
                reason=repr(e),
                code=400
            )

        name = request.get_argument('name')

        if name is None:
            raise NexusProcessingException(
                reason='Name argument must be provided',
                code=400
            )

        try:
            NexusTileService.user_ds_add(name, config)
        except Exception as e:
            raise NexusProcessingException(
                reason=repr(e),
                code=500
            )

