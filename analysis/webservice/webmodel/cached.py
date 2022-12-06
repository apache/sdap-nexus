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

import hashlib
import inspect
import time

__CACHE = {}

def cached(ttl=60000):
    def _hash_function_signature(func):
        hash_object = hashlib.md5(str(inspect.getargspec(func)).encode("utf8") + str(func).encode("utf8"))
        return hash_object.hexdigest()

    def _now():
        return int(round(time.time() * 1000))

    def _expired(t):
        if t is None or _now() - t > ttl:
            return True
        else:
            return False

    def _cached_decorator(func):

        def func_wrapper(self, computeOptions, **args):
            hash = _hash_function_signature(func)
            force = computeOptions.get_boolean_arg("nocached", default=False)

            if force or hash not in __CACHE or (hash in __CACHE and _expired(__CACHE[hash]["time"])):
                result = func(self, computeOptions, **args)
                __CACHE[hash] = {
                    "time": _now(),
                    "result": result
                }

            return __CACHE[hash]["result"]

        return func_wrapper

    return _cached_decorator
