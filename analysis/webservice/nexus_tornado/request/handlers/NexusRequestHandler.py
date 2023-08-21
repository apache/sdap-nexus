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
import logging
import tornado.gen
import tornado.ioloop

from webservice.nexus_tornado.request.renderers import NexusRendererFactory
from webservice.webmodel import NexusRequestObjectTornadoFree, NexusRequestObject, NexusProcessingException
from webservice.algorithms_spark.NexusCalcSparkTornadoHandler import NexusCalcSparkTornadoHandler


class NexusRequestHandler(tornado.web.RequestHandler):
    def initialize(self, thread_pool, clazz=None, **kargs):
        self.logger = logging.getLogger('nexus')
        self.executor = thread_pool
        self.__clazz = clazz
        self._clazz_init_args = kargs # 'algorithm_config', 'sc' for spark handler

    @tornado.gen.coroutine
    def get(self):
        self.logger.info("Received request %s" % self._request_summary())

        # temporary hack to use a NexusRequestObject without tornado request references
        # this object only supports timeAvgMapSpark yet.
        # Will be extended to replace the historical object in the next pull request related to ticket SDAP-252
        if self.request.path == '/timeAvgMapSpark':
            request = NexusRequestObjectTornadoFree(self)
        else:
            request = NexusRequestObject(self)

        # create NexusCalcHandler which will process the request
        instance = self.__clazz(**self._clazz_init_args)

        io_loop = tornado.ioloop.IOLoop.current()

        try:
            if isinstance(instance, NexusCalcSparkTornadoHandler):
                results = instance.calc(request, io_loop)
            else:
                results = yield io_loop.run_in_executor(
                    self.executor,
                    instance.calc,
                    request
                )

            try:
                self.set_status(results.status_code)
            except AttributeError:
                pass

            # Only render results if there are results to render.
            # "NexusCalcSparkTornadoHandler" endpoints redirectm so no
            # need to render.
            if not isinstance(instance, NexusCalcSparkTornadoHandler):
                renderer = NexusRendererFactory.get_renderer(request)
                renderer.render(self, results)

        except NexusProcessingException as e:
            self.async_onerror_callback(e.reason, e.code)

        except Exception as e:
            self.async_onerror_callback(str(e), 500)

    def async_onerror_callback(self, reason, code=500):
        self.logger.error("Error processing request", exc_info=True)

        self.set_header("Content-Type", "application/json")
        self.set_status(code)

        response = {
            "error": reason,
            "code": code
        }

        self.write(json.dumps(response, indent=5))
        self.finish()