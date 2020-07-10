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


import ConfigParser
import logging
import sys
import os
import pkg_resources
import nexus_tornado.web
from nexus_tornado.options import define, options, parse_command_line
from webservice.NexusLivyHandler import LivyHandler

class RunFileHandler(nexus_tornado.web.RequestHandler):

    _id = 0
            
    def __init__(self, *args, **kwargs):
        self._lh = kwargs.pop('lh', None)
        super(RunFileHandler, self).__init__(*args, **kwargs)

    def post(self):
        self._upload_file = self.request.files['file'][0]
        upload_fname = 'upload_'+str(RunFileHandler._id)+'.py'
        while os.path.exists(upload_fname):
            RunFileHandler._id += 1
            upload_fname = 'upload_'+str(RunFileHandler._id)+'.py'
        RunFileHandler._id += 1
        with open(upload_fname, 'w') as f:
            f.write(self._upload_file['body'])
        try:
            ans = self._lh.exec_file(upload_fname)
        except Exception, e:
            ans = str(e)
        self.write(str(ans))


class RunStrHandler(nexus_tornado.web.RequestHandler):
            
    def __init__(self, *args, **kwargs):
        self._lh = kwargs.pop('lh', None)
        super(RunStrHandler, self).__init__(*args, **kwargs)

    def post(self):
        self._upload_str = self.request.body
        ans = self._lh.exec_str(self._upload_str)
        self.write(str(ans))


if __name__ == "__main__":

    # Configure logger.
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
    log = logging.getLogger(__name__)

    # Configure tornado.
    webconfig = ConfigParser.RawConfigParser()
    webconfig.readfp(pkg_resources.resource_stream(__name__, "config/web.ini"), filename='web.ini')
    define("debug", default=False, help="run in debug mode")
    define("port", default=webconfig.get("livy", "server.socket_port"), help="run on the given port", type=int)
    define("address", default=webconfig.get("livy", "server.socket_host"), help="Bind to the given address")
    parse_command_line()
    log.info("Initializing on host address '%s'" % options.address)
    log.info("Initializing on port '%s'" % options.port)
    log.info("Starting web server in debug mode: %s" % options.debug)

    # Start up Livy Spark session.
    livy_host = webconfig.get("livy", "livy_host")
    livy_port = webconfig.get("livy", "livy_port")
    livy_url = 'http://' + livy_host + ':' + livy_port
    lh = LivyHandler(host=livy_url)

    # Define tornado job handlers
    handlers = []
    handlers.append((r"/run_file", RunFileHandler, dict(lh=lh)))
    handlers.append((r"/run_str", RunStrHandler, dict(lh=lh)))

    # Start listening for job requests.
    app = nexus_tornado.web.Application(
        handlers,
        default_host=options.address,
        debug=options.debug
    )
    app.listen(options.port)
    log.info("Started HTTP listener...")
    nexus_tornado.ioloop.IOLoop.current().start()
