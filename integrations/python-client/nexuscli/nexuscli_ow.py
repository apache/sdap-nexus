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
This module provides a native python client interface to the NEXUS Livy
webservice API.

Usage:

    from nexuscli import nexuscli_ow
    
    nexuscli_ow.set_target("http://host:port")

    filename = "mycode.py"
    nexuscli_ow.run_file(filename)

    The code in the file passed to run_file must be valid pyspark code.
    Furthermore, it must have a main function that takes exactly one
    argument, the SparkContext.  The code can make use of that SparkContext
    variable, but should not create the SparkContext.

    code = "1+1"
    nexuscli_ow.run_str(code)

    The code passed to run_str can also be a multi-line string containing 
    valid python code.  It can also be a multi-line string containing 
    valid pyspark code.  For pyspark code the variable sc may be used to 
    access the SparkContext,  but it should not create the SparkContext.
"""
import ast

import requests

from nexuscli import nexuscli

session = requests.session()

set_target = nexuscli.set_target


def run_file(fname):
    files = {'file': open(fname, 'rb')}
    response = session.post(nexuscli.target + '/run_file', files=files)
    print(response.text)
    return response.text


def run_str(code):
    response = requests.post(nexuscli.target + '/run_str', data=code)
    ans = ast.literal_eval(response.text)['text/plain']
    for line in ans:
        print(line, end=" ")
    return ans
