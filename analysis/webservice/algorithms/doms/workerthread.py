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

import threading


class WorkerThread(threading.Thread):

    def __init__(self, method, params):
        threading.Thread.__init__(self)
        self.method = method
        self.params = params
        self.completed = False
        self.results = None

    def run(self):
        self.results = self.method(*self.params)
        self.completed = True


def __areAllComplete(threads):
    for thread in threads:
        if not thread.completed:
            return False

    return True


def wait(threads, startFirst=False, poll=0.5):
    if startFirst:
        for thread in threads:
            thread.start()

    while not __areAllComplete(threads):
        threading._sleep(poll)


def foo(param1, param2):
    print(param1, param2)
    return "c"


if __name__ == "__main__":

    thread = WorkerThread(foo, params=("a", "b"))
    thread.start()
    while not thread.completed:
        threading._sleep(0.5)
    print(thread.results)
