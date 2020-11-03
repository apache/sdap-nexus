import logging
import json
import uuid
from datetime import datetime, timedelta
import tornado.web
import tornado.ioloop
from webservice.nexus_tornado.request.renderers import NexusRendererFactory


class NexusAsyncJobHandler(tornado.web.RequestHandler):

    _job_pool = {}
    __logger = logging.getLogger('nexus')

    obsolete_after = timedelta(hours=12)
    clean_obsolete_every = timedelta(minutes=15)

    @classmethod
    def get_job_pool(cls):
        return cls._job_pool

    @classmethod
    def start_jobs_cleaner(cls):

        def clean():
            for key, job in cls._job_pool.iteritems():
                if datetime.now() - job.time_done > cls.obsolete_after:
                    cls.__logger.info("clean job {}".format(key))
                    del cls._job_pool[key]

        tornado.ioloop.IOLoop.current().call_later(cls.clean_obsolete_every.seconds, clean)

    def get(self, job_id):
        self.__logger.info("get job among {}".format(self._job_pool))
        if job_id in self._job_pool:
            job = self._job_pool[job_id]
            if job.result_future.done():
                renderer = NexusRendererFactory.get_renderer(job.request)
                renderer.render(self, job.result_future.result())
            else:
                self._non_completed_job_callback(job_id)

        else:
            self._non_existing_job_callback(job_id)

    def _non_existing_job_callback(self, job_id, code=404):
        message = "Job {} does not exist".format(job_id)
        self._error_callback(message, code)

    def _non_completed_job_callback(self, job_id, code=202):
        message = "Job {} is being processed".format(job_id)
        self._error_callback(message, code)

    def _error_callback(self, message, code):
        self.__logger.info(message, exc_info=True)

        self.set_header("Content-Type", "application/json")
        self.set_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.set_header("Pragma", "no-cache")
        self.set_header("Expires", 0)
        self.set_status(code)

        response = {
            "error": message,
            "code": code
        }

        self.write(json.dumps(response, indent=5))
        self.finish()

    def data_received(self, chunk):
        pass

    @classmethod
    def get_short_job_id(cls):
        while True:
            job_id = str(uuid.uuid4())[:6]
            if job_id not in cls._job_pool:
                return job_id


