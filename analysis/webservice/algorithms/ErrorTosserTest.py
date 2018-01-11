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
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from webservice.NexusHandler import CalcHandler, nexus_handler


@nexus_handler
class ErrorTosserHandler(CalcHandler):
    name = "MakeError"
    path = "/makeerror"
    description = "Causes an error"
    params = {}
    singleton = True

    def __init__(self):
        CalcHandler.__init__(self)

    def calc(self, computeOptions, **args):
        a = 100 / 0.0
        # raise Exception("I'm Mad!")
        # raise NexusProcessingException.NexusProcessingException(NexusProcessingException.StandardNexusErrors.UNKNOWN, "I'm Mad!")
        return {}, None, None
