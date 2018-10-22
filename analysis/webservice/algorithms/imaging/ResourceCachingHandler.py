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


class AbstractCacheManager:

    def __init__(self):
        self._imagery_config = ConfigParser.RawConfigParser()
        self._imagery_config.readfp(pkg_resources.resource_stream(__name__, "config.ini"), filename='config.ini')

    def put(self, key, data):
        raise Exception("Not implemented")

    def get(self, key):
        raise Exception("Not Implemented")


class InMemoryCacheManager(AbstractCacheManager):
    def __init__(self):
        AbstractCacheManager.__init__(self)
        self.__cache = {}

    def put(self, key, data):
        self.__cache[key] = data

    def get(self, key):
        if key in self.__cache:
            return self.__cache[key]
        else:
            return None


class S3CacheManager(AbstractCacheManager):

    def __init__(self):
        AbstractCacheManager.__init__(self)
        self.__s3_bucketname = self._imagery_config.get("s3", "s3.bucket")
        self.__s3_region = self._imagery_config.get("s3", "s3.region")
        self.__s3_enabled = self._imagery_config.get("s3", "s3.enabled")

        self.__s3_enabled = True if self.__s3_enabled.upper() in ("TRUE", "YES") else False

    def put(self, key, data):
        if not self.__s3_enabled:
            return False

        temp = tempfile.NamedTemporaryFile(delete=False)
        temp.write(data)
        temp.close()

        s3 = boto3.client('s3')
        s3.upload_file(temp.name, self.__s3_bucketname, key)
        os.unlink(temp.name)

        return True

    def get(self, key):
        if not self.__s3_enabled:
            return None

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


class FilesystemCacheManager(AbstractCacheManager):

    def __init__(self):
        AbstractCacheManager.__init__(self)
        self.__fs_base = self._imagery_config.get("filesystem", "fs.base")

    def __exists(self, key):
        full_path = "%s/%s" % (self.__fs_base, key)
        return os.path.exists(full_path)

    def put(self, key, data):
        full_path = "%s/%s" % (self.__fs_base, key)
        bn = os.path.dirname(full_path)
        if not os.path.exists(bn):
            os.makedirs(bn)
        f = open(full_path, "wb")
        f.write(data)
        f.close()
        return True

    def get(self, key):
        if not self.__exists(key):
            return None
        full_path = "%s/%s" % (self.__fs_base, key)
        f = open(full_path, "rb")
        d = f.read()
        f.close()
        return d



class ResourceCachingHandler(BaseHandler):

    def __init__(self):
        BaseHandler.__init__(self)
        self.__imagery_config = ConfigParser.RawConfigParser()
        self.__imagery_config.readfp(pkg_resources.resource_stream(__name__, "config.ini"), filename='config.ini')

        self.__enabled = self.__imagery_config.get("caching", "cache.enabled")
        self.__enabled = True if self.__enabled.upper() in ("TRUE", "YES") else False

        datastore = self.__imagery_config.get("caching", "cache.store")

        self.__store = None
        if datastore == "s3":
            self.__store = S3CacheManager()
        elif datastore == "memory":
            self.__store = InMemoryCacheManager()
        elif datastore == "filesystem":
            self.__store = FilesystemCacheManager()
        else:
            raise Exception("Missing or invalid cache store specified in the imagery configuration")

    def _put_to_cache(self, key, data):
        if not self.__enabled or self.__store is None:
            return False
        return self.__store.put(key, data)

    def _get_from_cache(self, key):
        if not self.__enabled or self.__store is None:
            return None
        return self.__store.get(key)
