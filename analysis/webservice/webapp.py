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

import configparser

import logging
import sys
import os
from datetime import datetime

import tornado.web
from tornado.routing import Rule, RuleRouter, AnyMatches
from tornado.options import define, options, parse_command_line

from webservice.redirect import RemoteCollectionMatcher
from webservice.nexus_tornado.app_builders import NexusAppBuilder
from webservice.nexus_tornado.app_builders import RedirectAppBuilder

from nexustiles.nexustiles import NexusTileService

try:
    from importlib.metadata import version as _version
    from importlib.metadata import files as _files
except ImportError:
    from importlib_metadata import version as _version

try:
    __version__ = _version('sdap-nexus')
except Exception:
    __version__ = 'Cannot be determined'

banner = [
     '',
     ' ____  ____    _    ____    | ',
     '/ ___||  _ \\  / \\  |  _ \\   | Apache SDAP (TM)',
     '\\___ \\| | | |/ _ \\ | |_) |  | Science Data Analytics Platform',
     f' ___) | |_| / ___ \\|  __/   | Version: {__version__}',
     '|____/|____/_/   \\_\\_|      | ',
     ''
]


def inject_args_in_config(args, config):
    """
        Takes command argparse arguments and push them in the config
         with syntax args.<section>-<option>
    """
    log = logging.getLogger(__name__)

    for t_opt in list(args._options.values()):
        n = t_opt.name
        first_ = n.find('_')
        if first_ > 0:
            s, o = n[:first_], n[first_ + 1:]
            v = t_opt.value()
            log.info('inject argument {} = {} in configuration section {}, option {}'.format(n, v, s, o))
            if not config.has_section(s):
                config.add_section(s)
            config.set(s, o, v)
    return config


def main():
    start = datetime.now()

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout
    )

    log = logging.getLogger(__name__)

    for line in banner:
        log.info(line)

    web_config = configparser.RawConfigParser()
    web_config.read_file(open(os.path.join(os.path.dirname(__file__), "config", "web.ini")))

    algorithm_config = configparser.RawConfigParser()
    algorithm_config.read_file(open(os.path.join(os.path.dirname(__file__), "config", "algorithms.ini")))

    define("debug", default=False, help="run in debug mode")
    define("port", default=web_config.get("global", "server.socket_port"), help="run on the given port", type=int)
    define("address", default=web_config.get("global", "server.socket_host"), help="Bind to the given address")
    define('solr_time_out', default=60,
           help='time out for solr requests in seconds, default (60) is ok for most deployments'
                ' when solr performances are not good this might need to be increased')
    define('solr_host', help='solr host and port')
    define('cassandra_host', help='cassandra host')
    define('cassandra_username', help='cassandra username')
    define('cassandra_password', help='cassandra password')
    define('collections_path', default=None, help='collection config path')

    parse_command_line()
    algorithm_config = inject_args_in_config(options, algorithm_config)

    remote_collections = None
    router_rules = []
    if options.collections_path:
        # build retirect app
        remote_collection_matcher = RemoteCollectionMatcher(options.collections_path)
        remote_collections = remote_collection_matcher.get_remote_collections()
        remote_sdap_app = RedirectAppBuilder(remote_collection_matcher).build(
            host=options.address,
            debug=options.debug)
        router_rules.append(Rule(remote_collection_matcher, remote_sdap_app))

    # build nexus app
    nexus_app_builder = NexusAppBuilder().set_modules(
        web_config.get("modules", "module_dirs").split(","),
        algorithm_config,
        remote_collections=remote_collections
    )

    if web_config.get("static", "static_enabled") == "true":
        nexus_app_builder.enable_static(
            web_config.get("static", "static_dir")
        )
    else:
        log.info("Static resources disabled")

    local_sdap_app = nexus_app_builder.build(host=options.address, debug=options.debug)
    router_rules.append(Rule(AnyMatches(), local_sdap_app))

    router = RuleRouter(router_rules)

    log.info("Initializing on host address '%s'" % options.address)
    log.info("Initializing on port '%s'" % options.port)
    log.info("Starting web server in debug mode: %s" % options.debug)
    server = tornado.web.HTTPServer(router)
    server.listen(options.port)
    log.info('Waiting for dataset backends to come up...')

    with NexusTileService.DS_LOCK:
        if not NexusTileService.is_update_thread_alive():
            log.critical('A fatal error occurred when loading the datasets')
            exit(-1)

    log.info(f"SDAP started in {datetime.now() - start}. Starting HTTP listener...")
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
