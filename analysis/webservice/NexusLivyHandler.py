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

import time, json, requests, textwrap
from importlib import import_module
from os import environ
from os.path import basename, splitext, abspath
from livy.client import HttpClient

class LivyHandler:

    def __init__(self, host='http://localhost:8998'):
        self._headers = {'Content-Type': 'application/json'}
        if host is not None:
            self.create_pyspark_session(host)

    def _wait_for_state(self, url, desired_state):
        while True:
            r = requests.get(url, headers=self._headers)
            if r.json()['state'] == desired_state:
                break
            time.sleep(1)
        return r

    def create_pyspark_session(self, host):
        self._host = host
        data = {'kind': 'pyspark'}

        # Create a Spark session
        print('Creating Spark session...')
        r = requests.post(host + '/sessions', data=json.dumps(data), 
                          headers=self._headers)

        # Wait until the new Spark session is ready to use
        self._session_url = host + r.headers['location']

        r = self._wait_for_state(self._session_url, 'idle')

        # Create client for Livy batch jobs
        self._lc = HttpClient(self._session_url)

    def exec_str (self, code):
        print('Submitting code...')
        statements_url = self._session_url + '/statements'
        data = {'code': code}
        r = requests.post(statements_url, data=json.dumps(data), 
                          headers=self._headers)
        
        # Wait until the code completes
        print('Running code...')
        status_url = self._host + r.headers['location']
    
        r = self._wait_for_state(status_url, 'available')
        output = r.json()['output']
        print('output=',output)
        if output['status'] == 'error':
            ans = {'text/plain': output['traceback']}
        else:
            ans = {'text/plain': [output['data']['text/plain']]}
        return ans

    def exec_file(self, py_uri):
        py_uri_abs = abspath(py_uri)
        self._lc.upload_pyfile(py_uri_abs)
        m = splitext(basename(py_uri_abs))[0]
        try:
            m_imp = import_module(m)
        except ImportError:
            raise

        def upload_pyfile_job(jc):
            return m_imp.main(jc.sc)

        return self._lc.submit(upload_pyfile_job).result()

    def close(self):
        print('Closing Spark session...')
        requests.delete(self._session_url, headers=self._headers)


def main():
    try:
        livy_host = environ['LIVY_HOST']
    except:
        livy_host = "http://localhost:8998"
    print('Using Livy at {}'.format(livy_host))
    lh = LivyHandler(host=livy_host)

    # Run some pyspark code.
    code = textwrap.dedent("""
    1 + 1
    """)
    ans = lh.exec_str(code)
    print('The answer is {}'.format(ans))

    # Run some more pyspark code.
    code = textwrap.dedent("""
    import random
    NUM_SAMPLES = 100000
    def sample(p):
      x, y = random.random(), random.random()
      return 1 if x*x + y*y < 1 else 0

    count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
    print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)
    """)
    ans = lh.exec_str(code)
    print('The answer is {}'.format(ans))
    
    # Run a batch job
    py_uri = 'test_code_nexus_laptop.py'
    print('Submitting batch job from {}'.format(py_uri))
    ans = lh.exec_file(py_uri)
    print('The answer is {}'.format(ans))

    # Close the Spark session.
    lh.close()

if __name__ == "__main__":
    main()
