import json
import logging
import tornado.gen
import tornado.ioloop

from webservice.nexus_tornado.request.renderers import NexusRendererFactory
from webservice.webmodel import NexusRequestObjectTornadoFree, NexusRequestObject, NexusProcessingException


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

        try:
            # process the request asynchronously on a different thread,
            # the current tornado handler is still available to get other user requests
            results = yield tornado.ioloop.IOLoop.current().run_in_executor(self.executor, instance.calc, request)

            try:
                self.set_status(results.status_code)
            except AttributeError:
                pass

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