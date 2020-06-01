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
import sys
import traceback
import os

import matplotlib
import pkg_resources
import tornado.web
from tornado.options import define, options, parse_command_line
from tornado.concurrent import Future

from webservice import NexusHandler
from webservice.webmodel import NexusRequestObject, NexusRequestObjectTornadoFree, NexusProcessingException

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

    def initialize(self, thread_pool):
        self.logger = logging.getLogger('nexus')
        self.executor = thread_pool

    @tornado.gen.coroutine
    def get(self):
        self.logger.info("Received request %s" % self._request_summary())

        reqObject = NexusRequestObjectTornadoFree(self)
        yield self.do_get(reqObject)



    # def run(self):
    #
    #     try:
    #         result = self.do_get(reqObject)
    #         return result
    #     except NexusProcessingException as e:
    #         self.async_onerror_callback(e.reason, e.code)
    #     except Exception as e:
    #         self.async_onerror_callback(str(e), 500)

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

    def async_callback(self, args):
        pass

    # def do_get(self, reqObject):
    #     license = ''
    #     for root, dirs, files in os.walk("."):
    #         for pyfile in [afile for afile in files if afile.endswith(".py")]:
    #             print(os.path.join(root, pyfile))
    #             with open(os.path.join(root, pyfile), 'r') as original: data = original.read()
    #             with open(os.path.join(root, pyfile), 'w') as modified: modified.write(license + "\n" + data)
    #     pass

    def render_nexus_response(self, result_arg):
        request = result_arg['request']
        results = result_arg['result']
        if request.get_content_type() == ContentTypes.JSON:
            self.set_header("Content-Type", "application/json")
            try:
                result_str = results.toJson()
                self.logger.info("request result {}".format(json.loads(result_str)['data'][0:3]))
                self.write(result_str)
                self.finish()
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


class ModularNexusHandlerWrapper(BaseHandler):
    def initialize(self, thread_pool, clazz=None, algorithm_config=None, sc=None):
        BaseHandler.initialize(self, thread_pool)
        self.__algorithm_config = algorithm_config
        self.__clazz = clazz
        self.__sc = sc

    @tornado.gen.coroutine
    def do_get(self, request):
        log.info("do_get function called in ModularNexusHandlerWrapper")
        instance = self.__clazz.instance(algorithm_config=self.__algorithm_config, sc=self.__sc)

        try:
            results = yield tornado.ioloop.IOLoop.current().run_in_executor(None, instance.calc, request)
            #results = yield instance.calc(request)

            try:
                self.set_status(results.status_code)
            except AttributeError:
                pass

            result = {'request': request,
                      'result': results}
            self.render_nexus_response(result)

        except NexusProcessingException as e:
            self.async_onerror_callback(e.reason, e.code)

        except Exception as e:
            self.async_onerror_callback(str(e), 500)



    def async_callback(self, result):
        super(ModularNexusHandlerWrapper, self).async_callback(result)
        if hasattr(result, 'cleanup'):
            result.cleanup()

def inject_args_in_config(args, config):
    """
        Takes command argparse arguments and push them in the config
         with syntax args.<section>-<option>
    """
    log = logging.getLogger(__name__)

    for t_opt in args._options.values():
        n = t_opt.name
        first_ = n.find('_')
        if first_ > 0:
            s, o = n[:first_], n[first_+1:]
            v = t_opt.value()
            log.info('inject argument {} = {} in configuration section {}, option {}'.format(n, v , s, o))
            if not config.has_section(s):
                config.add_section(s)
            config.set(s, o, v)
    return config


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
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
    define('solr_time_out', default=60,
           help='time out for solr requests in seconds, default (60) is ok for most deployments'
                ' when solr performances are not good this might need to be increased')

    parse_command_line()
    algorithm_config = inject_args_in_config(options, algorithm_config)

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

    max_request_threads = webconfig.getint("global", "server.max_simultaneous_requests")
    log.info("Initializing request ThreadPool to %s" % max_request_threads)
    request_thread_pool = tornado.concurrent.futures.ThreadPoolExecutor(max_request_threads)

    spark_context = None
    for clazzWrapper in NexusHandler.AVAILABLE_HANDLERS:
        if issubclass(clazzWrapper.clazz(), NexusHandler.SparkHandler):
            if spark_context is None:
                from pyspark import SparkConf
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.appName("nexus-analysis").getOrCreate()
                spark_context = spark.sparkContext

            handlers.append(
                (clazzWrapper.path(), ModularNexusHandlerWrapper,
                 dict(clazz=clazzWrapper, algorithm_config=algorithm_config, sc=spark_context,
                      thread_pool=request_thread_pool)))
        else:
            handlers.append(
                (clazzWrapper.path(), ModularNexusHandlerWrapper,
                 dict(clazz=clazzWrapper, algorithm_config=algorithm_config, thread_pool=request_thread_pool)))


    class VersionHandler(tornado.web.RequestHandler):
        def get(self):
            self.write(pkg_resources.get_distribution("nexusanalysis").version)


    handlers.append((r"/version", VersionHandler))

    if staticEnabled:
        handlers.append(
            (r'/(.*)', tornado.web.StaticFileHandler, {'path': staticDir, "default_filename": "index.html"}))

    app = tornado.web.Application(
        handlers,
        default_host=options.address,
        debug=options.debug
    )
    app.listen(options.port)

    log.info("Starting HTTP listener...")
    tornado.ioloop.IOLoop.current().start()
