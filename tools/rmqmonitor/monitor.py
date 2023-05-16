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

import argparse
import sys
import time

import requests
from requests.auth import HTTPBasicAuth

parser = argparse.ArgumentParser()

parser.add_argument('--username', action='store', default='user', help='RabbitMQ username')
parser.add_argument('--password', action='store', default='bitnami', help='RabbitMQ password')

args = vars(parser.parse_args())

auth = HTTPBasicAuth(args['username'], args['password'])

waiting = True

req_number = 0

print()


def delete_last_line():
    sys.stdout.write('\x1b[1A')
    sys.stdout.write('\x1b[2K')


while True:
    try:
        req_number += 1
        
        response = requests.get("http://localhost:15672/api/queues/%2f/nexus", auth=auth)

        if response.ok:
            data = response.json()

            delete_last_line()
            print(f"{data['messages_unacknowledged']} in progress, {data['messages_ready']} waiting.", end='\n', flush=True)

            if data['messages_unacknowledged'] == 0 and data['messages_ready'] == 0 and not waiting:
                print()
                break
            elif data['messages_unacknowledged'] > 0 or data['messages_ready'] > 0:
                waiting = False
        else:
            delete_last_line()
            print(f"RMQ request {req_number} failed: {response.status_code}", end='\n', flush=True)
    except Exception as err:
        delete_last_line()
        print(f"An exception occurred: Request {req_number}, error: {type(err)}", end='\n', flush=True)

    time.sleep(5)
