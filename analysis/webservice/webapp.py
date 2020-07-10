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

import os
import ConfigParser
import importlib
import logging
import sys
import pkg_resources
import tornado.web
import webservice.algorithms_spark.NexusCalcSparkHandler
from tornado.options import define, options, parse_command_line

from webservice import NexusHandler
from webservice.nexus_tornado.request.handlers import NexusRequestHandler

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
        if issubclass(clazzWrapper, webservice.algorithms_spark.NexusCalcSparkHandler.NexusCalcSparkHandler):
            if spark_context is None:
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.appName("nexus-analysis").getOrCreate()
                spark_context = spark.sparkContext

            handlers.append(
                (clazzWrapper.path, NexusRequestHandler,
                 dict(clazz=clazzWrapper, algorithm_config=algorithm_config, sc=spark_context,
                      thread_pool=request_thread_pool)))
        else:
            handlers.append(
                (clazzWrapper.path, NexusRequestHandler,
                 dict(clazz=clazzWrapper, thread_pool=request_thread_pool)))


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
