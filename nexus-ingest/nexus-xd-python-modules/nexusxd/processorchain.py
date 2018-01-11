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

import importlib
from os import environ

from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

try:
    processing_chain = str(environ['CHAIN'])
except KeyError as e:
    raise EnvironmentError("Environment variable %s is required" % e.args[0])

try:
    message_generator_methods = [
        getattr(importlib.import_module('.'.join(method.split(".")[0:-1])), method.split(".")[-1]) for
        method in processing_chain.split(":")]
except KeyError as e:
    raise EnvironmentError("Environment variable %s is required" % e.args[0])


def run_chain(self, input_data):

    def recursive_processing_chain(gen_index, message):

        next_gen = message_generator_methods[gen_index + 1](None, message)
        for next_message in next_gen:
            if gen_index + 1 == len(message_generator_methods) - 1:
                yield next_message
            else:
                for result in recursive_processing_chain(gen_index + 1, next_message):
                    yield result

    return recursive_processing_chain(-1, input_data)


def start():
    start_server(run_chain, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
