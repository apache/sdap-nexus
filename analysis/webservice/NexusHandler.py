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


import logging
import types

AVAILABLE_HANDLERS = []
AVAILABLE_INITIALIZERS = []


def nexus_initializer(clazz):
    log = logging.getLogger(__name__)
    try:
        wrapper = NexusInitializerWrapper(clazz)
        log.info("Adding initializer '%s'" % wrapper.clazz())
        AVAILABLE_INITIALIZERS.append(wrapper)
    except Exception as ex:
        log.warn("Initializer '%s' failed to load (reason: %s)" % (clazz, ex.message), exc_info=True)
    return clazz


def nexus_handler(clazz):
    log = logging.getLogger(__name__)
    try:
        clazz.validate()
        log.info("Adding algorithm module '%s' with path '%s' (%s)" % (clazz.name, clazz.path, clazz))
        AVAILABLE_HANDLERS.append(clazz)
    except Exception as ex:
        log.warn("Handler '%s' is invalid and will be skipped (reason: %s)" % (clazz, ex.message), exc_info=True)
    return clazz


DEFAULT_PARAMETERS_SPEC = {
    "ds": {
        "name": "Dataset",
        "type": "string",
        "description": "One or more comma-separated dataset shortnames"
    },
    "minLat": {
        "name": "Minimum Latitude",
        "type": "float",
        "description": "Minimum (Southern) bounding box Latitude"
    },
    "maxLat": {
        "name": "Maximum Latitude",
        "type": "float",
        "description": "Maximum (Northern) bounding box Latitude"
    },
    "minLon": {
        "name": "Minimum Longitude",
        "type": "float",
        "description": "Minimum (Western) bounding box Longitude"
    },
    "maxLon": {
        "name": "Maximum Longitude",
        "type": "float",
        "description": "Maximum (Eastern) bounding box Longitude"
    },
    "startTime": {
        "name": "Start Time",
        "type": "long integer",
        "description": "Starting time in milliseconds since midnight Jan. 1st, 1970 UTC"
    },
    "endTime": {
        "name": "End Time",
        "type": "long integer",
        "description": "Ending time in milliseconds since midnight Jan. 1st, 1970 UTC"
    },
    "lowPassFilter": {
        "name": "Apply Low Pass Filter",
        "type": "boolean",
        "description": "Specifies whether to apply a low pass filter on the analytics results"
    },
    "seasonalFilter": {
        "name": "Apply Seasonal Filter",
        "type": "boolean",
        "description": "Specified whether to apply a seasonal cycle filter on the analytics results"
    }
}


class NexusInitializerWrapper:
    def __init__(self, clazz):
        self.__log = logging.getLogger(__name__)
        self.__hasBeenRun = False
        self.__clazz = clazz
        self.validate()

    def validate(self):
        if "init" not in self.__clazz.__dict__ or not type(self.__clazz.__dict__["init"]) == types.FunctionType:
            raise Exception("Method 'init' has not been declared")

    def clazz(self):
        return self.__clazz

    def hasBeenRun(self):
        return self.__hasBeenRun

    def init(self, config):
        if not self.__hasBeenRun:
            self.__hasBeenRun = True
            instance = self.__clazz()
            instance.init(config)
        else:
            self.log("Initializer '%s' has already been run" % self.__clazz)


def executeInitializers(config):
    [wrapper.init(config) for wrapper in AVAILABLE_INITIALIZERS]
