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
import ipaddress
import os
from abc import ABC, abstractmethod, abstractclassmethod
from typing import Literal
from botocore.config import Config
import boto3
import logging
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
import requests
import requests_mock
import base64
from requests.cookies import RequestsCookieJar


logger = logging.getLogger(__name__)

DT_FORMAT = '%Y-%m-%d %H:%M:%S%z'

EOSDIS_DAAC_ENDPOINTS = {
    'podaac':            'https://archive.podaac.earthdata.nasa.gov/s3credentials',
    'podaac-swot':       'https://archive.swot.podaac.earthdata.nasa.gov/s3credentials',
    'gesdisc':           'https://data.gesdisc.earthdatacloud.nasa.gov/s3credentials',
    'lpdaac':            'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials',
    # 'ornldaac':          'https://data.ornldaac.earthdatacloud.nasa.gov/s3credentials',
    # 'ghrc':              'https://data.ghrc.earthdatacloud.nasa.gov/s3credentials',
    'obdaac':            'https://obdaac-tea.earthdatacloud.nasa.gov/s3credentials',
    'nsidc':             'https://data.nsidc.earthdatacloud.nasa.gov/s3credentials',
    # 'asdc':              'https://data.asdc.earthdatacloud.nasa.gov/s3credentials',
    'laads':             'https://data.laadsdaac.earthdatacloud.nasa.gov/s3credentials',
    'asfdaac':           'https://cumulus.asf.alaska.edu/s3credentials',
    'asfdaac-sentinel1': 'https://sentinel1.asf.alaska.edu/s3credentials',
    'notadaac':          'mock://data.notadaac.earthdata.nasa.gov/s3credentials'
}

EDL_TOKEN_COOKIE_NAMES = [
    'accessToken',
    'pure-cookie'  # https://api.giovanni.earthdata.nasa.gov/s3credentials
]


class CredentialHandler(ABC):
    credentials = {}

    def __init__(self, key):
        self.__key = key
        self.__cred_data = {}

        CredentialHandler.credentials[key] = self

    @property
    def cred_data(self):
        return self.__cred_data

    @cred_data.setter
    def cred_data(self, creds):
        self.__cred_data = creds

    @property
    def key(self):
        return self.__key

    @abstractmethod
    def renew(self) -> bool:
        ...

    @abstractmethod
    def is_valid(self) -> bool:
        ...

    @staticmethod
    def __boto_format(cred_dict):
        creds = dict(
            aws_access_key_id=cred_dict['access_key_id'],
            aws_secret_access_key=cred_dict['secret_access_key'],
            aws_session_token=cred_dict.get('token')
        )

        if 'region_name' in cred_dict:
            creds['config'] = Config(region_name=cred_dict['region_name'])

        return creds

    @staticmethod
    def __s3fs_format(cred_dict):
        creds = dict(
            key=cred_dict['access_key_id'],
            secret=cred_dict['secret_access_key'],
            token=cred_dict.get('token')
        )

        if 'region_name' in cred_dict:
            creds['client_kwargs'] = dict(region_name=cred_dict['region_name'])

        return creds

    def get_credentials(self, fmt: Literal['boto3', 's3fs'] = 'boto3'):
        if not self.is_valid():
            if not self.renew():
                return None

        cred_dict = self.cred_data

        if fmt == 'boto3':
            return CredentialHandler.__boto_format(cred_dict)
        elif fmt == 's3fs':
            return CredentialHandler.__s3fs_format(cred_dict)
        else:
            raise ValueError(fmt)


class FixedAWSCredentialHandler(CredentialHandler):
    def __init__(self, collection, config):
        super().__init__(collection)

        access_key_id = config['aws'].get('accessKeyID')
        secret_access_key = config['aws'].get('secretAccessKey')

        region = config['aws'].get('region')

        if any([v is None for v in [access_key_id, secret_access_key]]):
            raise ValueError('Missing credential')

        self.cred_data = dict(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )

        if region is not None:
            self.cred_data['region_name'] = region

    def renew(self) -> bool:
        return True

    def is_valid(self) -> bool:
        return True


class FixedAWSProfileCredentialHandler(CredentialHandler):
    def __init__(self, collection, config):
        super().__init__(collection)

        self.__profile = config['aws']['profile']
        self.__region = config['aws'].get('region')

        if self.__region is not None:
            self.cred_data['region_name'] = self.__region

        try:
            self.__session = boto3.Session(profile_name=self.__profile, region_name=self.__region)
        except:
            raise ValueError(f'Profile {self.__profile} not found')

        self.renew()

    def renew(self) -> bool:
        try:
            logger.info(f'Trying to get credentials from profile {self.__profile}')
            self.__session = boto3.Session(profile_name=self.__profile, region_name=self.__region)
        except:
            logger.error(f'AWS profile {self.__profile} not found')
            return False

        cred: Credentials = self.__session.get_credentials()

        self.cred_data = dict(
            access_key_id=cred.access_key,
            secret_access_key=cred.secret_key,
            token=cred.token
        )

        if self.__region is not None:
            self.cred_data['region_name'] = self.__region

        return True

    def is_valid(self) -> bool:
        try:
            self.__session.client('sts').get_caller_identity()
            return True
        except ClientError as err:
            err_code = err.response['Error']['Code']
            err_msg = err.response['Error']['Message']
            logger.error(f'Credentials in profile {self.__profile} are invalid. Code: {err_code}, Message: {err_msg}')
            return False


class DAACTemporaryAWSCredentialHandler(CredentialHandler):
    __ip_valid = None

    def __init__(self, endpoint, config, mock_auth=False):
        super().__init__(endpoint)

        if DAACTemporaryAWSCredentialHandler.__ip_valid is None:
            r = requests.get('https://ifconfig.me')
            r.raise_for_status()

            current_ip = ipaddress.ip_address(r.text)

            r = requests.get('https://ip-ranges.amazonaws.com/ip-ranges.json')
            r.raise_for_status()

            prefixes = [i['ip_prefix'] for i in r.json()['prefixes'] if i['region'] == 'us-west-2']

            valid = any([current_ip in ipaddress.ip_network(p) for p in prefixes])
            DAACTemporaryAWSCredentialHandler.__ip_valid = valid

            if valid:
                logger.info('Detected IP is within the AWS us-west-2 region')
            else:
                logger.warning('Detected IP is not within the AWS us-west-2 region')

        if not mock_auth and not DAACTemporaryAWSCredentialHandler.__ip_valid:
            raise RuntimeError('Current runtime IP not identified as one in the AWS us-west-2 region')

        self.expiration = None

        user = None
        password = None

        if not mock_auth:
            if 'edl_username' in config['earthdata']:
                user = config['earthdata']['edl_username']
            else:
                user = os.getenv('EDL_USERNAME')

            if 'edl_password' in config['earthdata']:
                password = config['earthdata']['edl_password']
            else:
                password = os.getenv('EDL_PASSWORD')

            if user is None or password is None:
                raise ValueError('Missing EDL username or password!')

        self.__auth = f'{user}:{password}'
        self.__region = 'us-west-2'

    def renew(self) -> bool:
        logger.info(f'Getting DAAC S3 credentials from {self.key}')

        try:
            login = requests.get(self.key, allow_redirects=False)
            logger.debug(f'[{login.status_code}] {login.request.url}')
            login.raise_for_status()

            auth = requests.post(
                login.headers['location'],
                data=dict(credentials=base64.b64encode(self.__auth.encode('ascii'))),
                headers=dict(Origin=self.key),
                allow_redirects=False
            )
            logger.debug(f'[{auth.status_code}] {auth.request.url}')
            auth.raise_for_status()

            final = requests.get(auth.headers['location'], allow_redirects=False)
            logger.debug(f'[{final.status_code}] {final.request.url}')
            final.raise_for_status()

            cookie_jar: RequestsCookieJar = final.cookies
            cookie = None

            for valid_name in EDL_TOKEN_COOKIE_NAMES:
                if valid_name in cookie_jar:
                    cookie = valid_name
                    break

            if cookie is None:
                logger.error(f'No auth cookie found in EDL login response. Got: {cookie_jar.keys()}, expected one of '
                             f'{EDL_TOKEN_COOKIE_NAMES}')
                return False

            response = requests.get(
                self.key,
                cookies={cookie: cookie_jar[cookie]}
            )
            logger.debug(f'[{response.status_code}] {response.request.url}')
            response.raise_for_status()
        except Exception as e:
            logger.error('Failed to get DAAC credentials')
            logger.exception(e)
            return False

        response = response.json()
        # DAACs are not consistent in the cases of the keys of the creds object so force them all to be lower
        response = {k.lower(): response[k] for k in response}

        self.cred_data = dict(
            access_key_id=response['accesskeyid'],
            secret_access_key=response['secretaccesskey'],
            token=response['sessiontoken']
        )

        if self.__region:
            self.cred_data['region_name'] = self.__region

        self.expiration = datetime.strptime(response['expiration'], DT_FORMAT)

        logger.info(f'Retrieved AWS credentials from {self.key}, exp: {response["expiration"]}')

        return True

    def is_valid(self) -> bool:
        if self.expiration is not None and self.expiration - datetime.now(timezone.utc) < timedelta(minutes=5):
            logger.info(f'Temporary S3 credentials from {self.key} are past or near expiry and will be renewed')
            return False
        elif self.expiration is not None:
            logger.debug(f'Temporary S3 credentials from {self.key} are unexpired. '
                         f'{self.expiration - datetime.now(timezone.utc)} remaining')

        if 'access_key_id' not in self.cred_data:
            return False

        session = boto3.Session(
            aws_access_key_id=self.cred_data['access_key_id'],
            aws_secret_access_key=self.cred_data['secret_access_key'],
            aws_session_token=self.cred_data['token'],
            region_name=self.cred_data.get('region_name')
        )

        try:
            session.client('sts').get_caller_identity()
            return True
        except ClientError as err:
            err_code = err.response['Error']['Code']
            err_msg = err.response['Error']['Message']
            logger.error(f'Temp credentials are invalid. Code: {err_code}, Message: {err_msg}')
            return False


class FixedAWSMockCredentialHandler(DAACTemporaryAWSCredentialHandler):  # For Zarr testing, remove before merge
    def __init__(self, mock_endpoint, config):
        super().__init__(mock_endpoint, config, True)

        access_key_id = config['mock'].get('accessKeyID')
        secret_access_key = config['mock'].get('secretAccessKey')

        region = config['mock'].get('region')
        self.__region = region

        if any([v is None for v in [access_key_id, secret_access_key]]):
            raise ValueError('Missing credential')

        def mock_response(r, c):
            creds = dict(
                accessKeyId=access_key_id,
                secretAccessKey=secret_access_key,
                sessionToken=None,
                expiration=(datetime.now(timezone.utc) + timedelta(minutes=30)).strftime(DT_FORMAT)
            )

            return creds

        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock://', adapter)

        adapter.register_uri('GET', mock_endpoint, json=mock_response)

        self.__session = session
        self.expiration = None

    def renew(self) -> bool:
        logger.info(f'Getting mock credentials from {self.key}')

        try:
            response = self.__session.get(self.key)
            logger.debug(f'[{response.status_code}] {response.request.url}')
            response.raise_for_status()
        except Exception as e:
            logger.error('Mock request failed')
            logger.exception(e)
            return False

        response = response.json()

        self.cred_data = dict(
            access_key_id=response['accessKeyId'],
            secret_access_key=response['secretAccessKey'],
            token=response['sessionToken']
        )

        if self.__region:
            self.cred_data['region_name'] = self.__region

        self.expiration = datetime.strptime(response['expiration'], DT_FORMAT)

        logger.info(f'Retrieved AWS credentials from {self.key}, exp: {response["expiration"]}')

        return True


def get_handler(collection: str, config) -> CredentialHandler:
    existing_collections = CredentialHandler.credentials

    handler = None
    clazz = None

    if 'aws' in config:
        clazz = FixedAWSCredentialHandler if 'profile' not in config['aws'] else FixedAWSProfileCredentialHandler
    elif 'earthdata' in config:
        ed_config = config['earthdata']
        mock = 'mock' in config

        if 'endpoint' in ed_config:
            endpoint = ed_config['endpoint']
        elif 'daac' in ed_config and ed_config['daac'].lower() in EOSDIS_DAAC_ENDPOINTS:
            endpoint = EOSDIS_DAAC_ENDPOINTS[ed_config['daac'].lower()]
        else:
            raise ValueError('Credential endpoint missing or an unknown DAAC name was provided')

        collection = endpoint
        clazz = DAACTemporaryAWSCredentialHandler if not mock else FixedAWSMockCredentialHandler

        if endpoint in existing_collections:
            handler = existing_collections[endpoint]

    if handler is None:
        logger.debug(f'Creating new handler of type {clazz} for {collection}')
        handler = clazz(collection, config)

    return handler




