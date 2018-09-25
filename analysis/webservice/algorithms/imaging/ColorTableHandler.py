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
import time
import colortables
import numpy as np
import os
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusResults
import io
import mapprocessing
import tilespecs
import datetime
import tempfile
from pytz import UTC, timezone


class ColorTableResponse:

    def __init__(self):
        pass

    def default_results_type(self):
        return "JSON"



@nexus_handler
class ColorTableHandler(BaseHandler):
    name = "ColorTableHandler"
    path = "/colortable"
    description = "Provides imagery color table specifications"
    params = {}

    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("layer")

        color_table_identifier = computeOptions.get_argument("ct", "rainbow")
        color_table = colortables.get_color_table(color_table_identifier)

