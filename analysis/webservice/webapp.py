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
import importlib
import json
import logging
import signal
import sys
import traceback
from functools import partial

import matplotlib
import pkg_resources
import tornado.process
import tornado.web
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line

from webservice import NexusHandler
from webservice.webmodel import NexusRequestObject, NexusProcessingException

matplotlib.use('Agg')


class ContentTypes(object):
    CSV = "CSV"
    JSON = "JSON"
    XML = "XML"
    PNG = "PNG"
    NETCDF = "NETCDF"
    ZIP = "ZIP"


class BaseHandler(tornado.web.RequestHandler):
    path = r"/"

    def initialize(self):
        self.logger = logging.getLogger('nexus')

    @tornado.web.asynchronous
    def get(self):
        self.run()

    def run(self):
        reqObject = NexusRequestObject(self)
        try:
            result = self.do_get(reqObject)
            self.async_callback(result)
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

    def async_callback(self, result):
        self.finish()

    ''' Override me for standard handlers! '''

    def do_get(self, reqObject):
        pass


class ModularNexusHandlerWrapper(BaseHandler):
    def initialize(self, clazz=None, algorithm_config=None):
        BaseHandler.initialize(self)
        self.__algorithm_config = algorithm_config
        self.__clazz = clazz

    def do_get(self, request):
        instance = self.__clazz.instance(algorithm_config=self.__algorithm_config)

        results = instance.calc(request)

        try:
            self.set_status(results.status_code)
        except AttributeError:
            pass

        if request.get_content_type() == ContentTypes.JSON:
            self.set_header("Content-Type", "application/json")
            try:
                self.write(results.toJson())
            except AttributeError:
                traceback.print_exc(file=sys.stdout)
                self.write(json.dumps(results, indent=4))
        elif request.get_content_type() == ContentTypes.PNG:
            self.set_header("Content-Type", "image/png")
            try:
                self.write(results.toImage())
            except AttributeError:
                traceback.print_exc(file=sys.stdout)
                raise NexusProcessingException(reason="Unable to convert results to an Image.")
        elif request.get_content_type() == ContentTypes.CSV:
            self.set_header("Content-Type", "text/csv")
            self.set_header("Content-Disposition", "filename=\"%s\"" % request.get_argument('filename', "download.csv"))
            try:
                self.write(results.toCSV())
            except:
                traceback.print_exc(file=sys.stdout)
                raise NexusProcessingException(reason="Unable to convert results to CSV.")
        elif request.get_content_type() == ContentTypes.NETCDF:
            self.set_header("Content-Type", "application/x-netcdf")
            self.set_header("Content-Disposition", "filename=\"%s\"" % request.get_argument('filename', "download.nc"))
            try:
                self.write(results.toNetCDF())
            except:
                traceback.print_exc(file=sys.stdout)
                raise NexusProcessingException(reason="Unable to convert results to NetCDF.")
        elif request.get_content_type() == ContentTypes.ZIP:
            self.set_header("Content-Type", "application/zip")
            self.set_header("Content-Disposition", "filename=\"%s\"" % request.get_argument('filename', "download.zip"))
            try:
                self.write(results.toZip())
            except:
                traceback.print_exc(file=sys.stdout)
                raise NexusProcessingException(reason="Unable to convert results to Zip.")

        return results

    def async_callback(self, result):
        super(ModularNexusHandlerWrapper, self).async_callback(result)
        if hasattr(result, 'cleanup'):
            result.cleanup()


def init_spark():
    from pyspark import SparkContext, SparkConf

    # Configure Spark
    start_port = 4040
    __sp_conf = SparkConf()
    __sp_conf.set("spark.executor.memory", "6g")
    if tornado.process.task_id():
        logging.info("Configuring spark context for task {}".format(tornado.process.task_id()))
        __sp_conf.setAppName("nexus-analysis-{}".format(tornado.process.task_id()))
        __sp_conf.set("spark.ui.port", "{}".format(start_port + tornado.process.task_id()))
    else:
        logging.info("Configuring spark context")
        __sp_conf.setAppName("nexus-analysis")

    sc = SparkContext(conf=__sp_conf)
    sc.setLogLevel("INFO")

    for wrapper in NexusHandler.AVAILABLE_HANDLERS:
        if issubclass(wrapper.clazz(), NexusHandler.SparkHandler):
            try:
                wrapper.instance().set_spark_context(sc)
            except AttributeError as e:
                raise e
    return sc


def shutdown(sig, frame, sserver=None, sc=None):
    """
    Graceful shutdown of all processes.

    :param sserver:
    :param sc:
    :param frame:
    :param sig:
    :return:
    """
    # Stop spark context
    if sc:
        sc.stop()

    # Parent process
    if tornado.process.task_id() is None:
        logging.info("Received Signal %s Stopping server...", sig)
        # Stop accepting new connections
        sserver.stop()
        # Stop the IO Loop
        IOLoop.current().stop()
    # Child Process(es)
    else:
        # Exit normally
        sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)

    log = logging.getLogger(__name__)

    webconfig = ConfigParser.RawConfigParser()
    webconfig.readfp(pkg_resources.resource_stream(__name__, "config/web.ini"), filename='web.ini')

    algorithm_config = ConfigParser.RawConfigParser()
    algorithm_config.readfp(pkg_resources.resource_stream(__name__, "config/algorithms.ini"), filename='algorithms.ini')

    define("debug", default=False, help="run in debug mode")
    define("port", default=webconfig.get("global", "server.socket_port"), help="run on the given port", type=int)
    define("address", default=webconfig.get("global", "server.socket_host"), help="Bind to the given address")
    define("subprocesses", default=webconfig.get("global", "server.num_sub_processes"),
           help="Number of http server subprocesses", type=int)
    parse_command_line()

    moduleDirs = webconfig.get("modules", "module_dirs").split(",")
    for moduleDir in moduleDirs:
        log.info("Loading modules from %s" % moduleDir)
        importlib.import_module(moduleDir)

    staticDir = webconfig.get("static", "static_dir")
    staticEnabled = webconfig.get("static", "static_enabled") == "true"

    log.info("Initializing on host address '%s'" % options.address)
    log.info("Initializing on port '%s'" % options.port)
    log.info("Starting web server in debug mode: %s" % options.debug)
    if staticEnabled:
        log.info("Using static root path '%s'" % staticDir)
    else:
        log.info("Static resources disabled")

    handlers = []

    log.info("Running Nexus Initializers")
    NexusHandler.executeInitializers(algorithm_config)

    spark_handlers_loaded = False
    for clazzWrapper in NexusHandler.AVAILABLE_HANDLERS:
        handlers.append(
            (clazzWrapper.path(), ModularNexusHandlerWrapper,
             dict(clazz=clazzWrapper, algorithm_config=algorithm_config)))
        if issubclass(clazzWrapper.clazz(), NexusHandler.SparkHandler):
            spark_handlers_loaded = True


    class VersionHandler(tornado.web.RequestHandler):

        def get(self):
            self.write(pkg_resources.get_distribution("nexusanalysis").version)


    handlers.append((r"/version", VersionHandler))

    if staticEnabled:
        handlers.append(
            (r'/(.*)', tornado.web.StaticFileHandler, {'path': staticDir, "default_filename": "index.html"}))

    app = tornado.web.Application(
        handlers,
        debug=options.debug
    )
    log.info("Starting HTTP listener...")

    server = HTTPServer(app)
    server.bind(options.port, address=options.address)

    signal.signal(signal.SIGTERM, partial(shutdown, sserver=server, sc=None))
    signal.signal(signal.SIGINT, partial(shutdown, sserver=server, sc=None))

    server.start(int(options.subprocesses))  # Forks multiple sub-processes

    # Needs to be after fork
    if spark_handlers_loaded:
        _sc = init_spark()
        # Spark overrides the SIGINT handler from earlier, reset it
        signal.signal(signal.SIGINT, partial(shutdown, sserver=server, sc=_sc))

    IOLoop.current().start()
