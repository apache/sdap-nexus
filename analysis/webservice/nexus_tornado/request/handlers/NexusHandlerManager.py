import os
import logging
import sys
import importlib
import pkg_resources
import tornado.web
from webservice import NexusHandler
from webservice.nexus_tornado.request.handlers import NexusRequestHandler
from webservice.nexus_tornado.request.handlers import NexusAsyncJobHandler
import webservice.algorithms_spark.NexusCalcSparkHandler

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger(__name__)


class VersionHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(pkg_resources.get_distribution("nexusanalysis").version)


class NexusHandlerManager(object):
    _spark_context = None

    def __init__(self, module_dirs,
                 algorithm_config, tile_service_factory,
                 max_request_threads=1,
                 static_dir=None):

        for moduleDir in module_dirs:
            logger.info("Loading modules from %s" % moduleDir)
            importlib.import_module(moduleDir)

        logger.info("Running Nexus Initializers")
        NexusHandler.executeInitializers(algorithm_config)

        self._tile_service_factory = tile_service_factory

        logger.info("Initializing request ThreadPool to %s" % max_request_threads)
        self._request_thread_pool = tornado.concurrent.futures.ThreadPoolExecutor(max_request_threads)

        self._static_dir = static_dir

    def get_handlers(self):
        handlers = self._get_legacy_handlers()
        handlers.extend(self._get_restapi_algorithm_handlers())

        handlers.append((r"/version", VersionHandler))

        NexusAsyncJobHandler.start_jobs_cleaner()
        handlers.append((r"/jobs/(.*)", NexusAsyncJobHandler))

        if self._static_dir:
            handlers.append(
                (r'/(.*)', tornado.web.StaticFileHandler, {'path': self._static_dir, "default_filename": "index.html"}))

        return handlers

    def _get_legacy_handlers(self):
        return self.__get_tornado_handlers(NexusHandler.AVAILABLE_LEGACY_HANDLERS, lambda x: x)

    def _get_restapi_algorithm_handlers(self):

        def path_spark_to_restapi(s):
            i_spark = s.find('Spark')
            return '/algorithms' + s[:i_spark]

        return self.__get_tornado_handlers(NexusHandler.AVAILABLE_RESTAPI_HANDLERS, path_spark_to_restapi)

    def _get_restapi_job_handler(self):
        pass

    def __get_tornado_handlers(self, wrappers, path_func):
        handlers = []

        for clazzWrapper in wrappers:
            path = path_func(clazzWrapper.path)
            logger.info("adding request handler for class {} on path {}".format(clazzWrapper, path))
            if issubclass(clazzWrapper, webservice.algorithms_spark.NexusCalcSparkHandler.NexusCalcSparkHandler):
                spark_context = self._get_spark_context()
                handlers.append((path,
                                 NexusRequestHandler,
                                 dict(clazz=clazzWrapper,
                                      tile_service_factory=self._tile_service_factory,
                                      sc=spark_context,
                                      thread_pool=self._request_thread_pool)))
            else:
                handlers.append((path,
                                 NexusRequestHandler,
                                 dict(clazz=clazzWrapper,
                                      tile_service_factory=self._tile_service_factory,
                                      thread_pool=self._request_thread_pool)))

        return handlers

    def _get_spark_context(self):
        if self._spark_context is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.appName("nexus-analysis") \
                .config("spark.scheduler.mode", "FAIR") \
                .config("spark.scheduler.allocation.file", os.path.abspath("webservice/config/spark_pools.xml")) \
                .getOrCreate()
            self._spark_context = spark.sparkContext

        return self._spark_context
