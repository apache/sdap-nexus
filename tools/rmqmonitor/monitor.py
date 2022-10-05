
import requests
from requests.auth import HTTPBasicAuth
import time
import sys

auth = HTTPBasicAuth('user', 'bitnami')

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
        print(f"An exception occurred: Request {req_number}, error: {type(err)=}", end='\n', flush=True)

    time.sleep(5)
