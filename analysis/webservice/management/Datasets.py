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

from schema import Schema, Or, SchemaError
from schema import Optional as Opt

from urllib.parse import urlparse
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


CONFIG_SCHEMA = Schema({
    Or('variable', 'variables'): Or(str, [str]),
    'coords': {
        'latitude': str,
        'longitude': str,
        'time': str,
        Opt('depth'): str
    },
    Opt('aws'): {
        Opt('accessKeyID'): str,
        Opt('secretAccessKey'): str,
        'public': bool,
        Opt('region'): str
    }
})


class DatasetManagement:
    @classmethod
    def validate(cls):
        pass

    @staticmethod
    def parse_config(request: NexusRequestObject):
        content_type = request.get_headers()['Content-Type']

        if content_type in ['application/json', 'application/x-json']:
            config_dict = json.loads(request.get_request_body())
        elif content_type == 'application/yaml':
            config_dict = load(request.get_request_body(), Loader=Loader)
        else:
            raise NexusProcessingException(reason='Invalid Content-Type header', code=400)

        try:
            CONFIG_SCHEMA.validate(config_dict)

            if 'aws' in config_dict:
                if not config_dict['aws']['public']:
                    if 'accessKeyID' not in config_dict['aws'] or 'secretAccessKey' not in config_dict['aws']:
                        raise NexusProcessingException(
                            reason='Must provide AWS creds for non-public bucket',
                            code=400
                        )
        except SchemaError as e:
            raise NexusProcessingException(
                reason=str(e),
                code=400
            )

        return config_dict


class Response:
    def __init__(self, response):
        self.response = response if response is not None else {}

    def toJson(self):
        return json.dumps(self.response)


@nexus_handler
class DatasetAdd(DatasetManagement):
    name = 'Add dataset'
    path = '/datasets/add'
    description = "Add new Zarr dataset to running SDAP instance"
    params = {
        "name": {
            "name": "Dataset name",
            "type": "string",
            "description": "Name of new dataset to add"
        },
        "path": {
            "name": "Path or URL",
            "type": "string",
            "description": "Path/URL of Zarr group"
        },
        "body": {
            "name": "Request body",
            "type": "application/json OR application/yaml",
            "description": "POST request body. Config options for Zarr (variabe, coords, aws (if applicable))"
        }
    }

    def __init__(self, **args):
        pass

    def calc(self, request: NexusRequestObject, **args):
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

        path = request.get_argument('path')

        if path is None:
            raise NexusProcessingException(
                reason='Path argument must be provided',
                code=400
            )

        try:
            if urlparse(path).scheme not in ['file','','s3']:
                raise NexusProcessingException(
                    reason='Dataset URL must be for a local file or S3 URL',
                    code=400
                )
        except ValueError:
            raise NexusProcessingException(
                reason='Could not parse path URL', code=400
            )

        try:
            NexusTileService.user_ds_add(name, path, config)
        except Exception as e:
            raise NexusProcessingException(
                reason=repr(e),
                code=500
            )


@nexus_handler
class DatasetUpdate(DatasetManagement):
    name = 'Update dynamically added dataset'
    path = '/datasets/update'
    description = "Update Zarr dataset in running SDAP instance"
    params = {
        "name": {
            "name": "Dataset name",
            "type": "string",
            "description": "Name of dataset to update"
        },
        "body": {
            "name": "Request body",
            "type": "application/json OR application/yaml",
            "description": "POST request body. Config options for Zarr (variabe, coords, aws (if applicable))"
        }
    }

    def __init__(self, **args):
        pass

    def calc(self, request: NexusRequestObject, **args):
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
            return Response(NexusTileService.user_ds_update(name, config))
        except Exception as e:
            raise NexusProcessingException(
                reason=repr(e),
                code=500
            )


@nexus_handler
class DatasetDelete(DatasetManagement):
    name = 'Remove dataset'
    path = '/datasets/remove'
    description = "Remove Zarr dataset from running SDAP instance"
    params = {
        "name": {
            "name": "Dataset name",
            "type": "string",
            "description": "Name of dataset to remove"
        }
    }

    def __init__(self, **args):
        pass

    def calc(self, request: NexusRequestObject, **args):
        name = request.get_argument('name')

        if name is None:
            raise NexusProcessingException(
                reason='Name argument must be provided',
                code=400
            )

        try:
            return Response(NexusTileService.user_ds_delete(name))
        except Exception as e:
            raise NexusProcessingException(
                reason=repr(e),
                code=500
            )

