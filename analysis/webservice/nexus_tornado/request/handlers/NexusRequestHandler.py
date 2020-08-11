import json
import datetime
import logging
import functools
import tornado.gen
import tornado.ioloop
import tornado.util
import uuid
from datetime import datetime, timedelta
from webservice.jobs import Job

from webservice.nexus_tornado.request.renderers import NexusRendererFactory
from webservice.nexus_tornado.request.handlers.NexusAsyncJobHandler import NexusAsyncJobHandler
from webservice.webmodel import NexusRequestObjectTornadoFree, NexusRequestObject, NexusProcessingException


class NexusRequestHandler(tornado.web.RequestHandler):

    def initialize(self, thread_pool, clazz=None, **kargs):
        self.logger = logging.getLogger('nexus')
        self.executor = thread_pool
        self.__clazz = clazz
        self.__synchronous_time_out_seconds = timedelta(seconds=30)
        self._clazz_init_args = kargs  # 'algorithm_config', 'sc' for spark handler



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
            result_future = tornado.ioloop.IOLoop.current().run_in_executor(self.executor,
                                                                            instance.calc,
                                                                            request)
            if self.__synchronous_time_out_seconds:
                results = yield tornado.gen.with_timeout(self.__synchronous_time_out_seconds,
                                                         result_future)
            else:
                results = yield result_future

            try:
                self.set_status(results.status_code)
            except AttributeError:
                pass

            renderer = NexusRendererFactory.get_renderer(request)
            renderer.render(self, results)

        except tornado.gen.TimeoutError as e:
            self.logger.info("synchronous time out reached, switch to async mode")

            self._switch_to_async(request, result_future)

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

    def _switch_to_async(self, request, result_future):
        job = Job()
        job.request = request

        def set_job_done_datetime(job, future):
            job.time_done = datetime.now()

        result_future.add_done_callback(functools.partial(set_job_done_datetime, job))
        job.result_future = result_future
        job_id = NexusAsyncJobHandler.get_short_job_id()
        NexusAsyncJobHandler.get_job_pool()[job_id] = job
        self.async_onsynctimeout_callback(job_id)


    def async_onsynctimeout_callback(self, job_id, code=202):
        message = "Processing request is taking more than {} s, switch to async mode, check status at /jobs/{}".format(
            self.__synchronous_time_out_seconds, job_id)
        self.logger.info(message,
                         exc_info=True)

        self.set_header("Content-Type", "application/json")
        self.set_status(code)

        response = {
            "error": message,
            "code": code,
            "job_id": job_id
        }

        self.write(json.dumps(response, indent=5))
        self.finish()

