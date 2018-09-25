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

import os
from webservice.NexusHandler import NexusHandler as BaseHandler

import boto3
import botocore

import tempfile
import ConfigParser
import pkg_resources


class S3CachingHandler(BaseHandler):

    def __init__(self):
        BaseHandler.__init__(self)
        self.__imagery_config = ConfigParser.RawConfigParser()
        self.__imagery_config.readfp(pkg_resources.resource_stream(__name__, "config.ini"), filename='config.ini')

        self.__s3_bucketname = self.__imagery_config.get("s3", "s3.bucket")
        self.__s3_region = self.__imagery_config.get("s3", "s3.region")
        self.__s3 = boto3.resource('s3')

    def _upload_tile_to_s3(self, key, img):
        temp = tempfile.NamedTemporaryFile(delete=False)
        temp.write(img)
        temp.close()

        s3 = boto3.client('s3')
        s3.upload_file(temp.name, self.__s3_bucketname, key)
        os.unlink(temp.name)

    def _fetch_tile_from_s3(self, key):
        s3 = boto3.client('s3')

        temp = tempfile.TemporaryFile()

        try:
            s3.download_fileobj(self.__s3_bucketname, key, temp)
            temp.seek(0)
            data = temp.read()
            return data
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None
            else:
                raise