import logging
import importlib
from functools import partial
import pkg_resources
import tornado
from nexustiles.nexustiles import NexusTileService
from webservice import NexusHandler
from webservice.nexus_tornado.request.handlers import NexusRequestHandler
from .HandlerArgsBuilder import HandlerArgsBuilder


class NexusAppBuilder:
    def __init__(self):
        self.handlers = []
        self.log = logging.getLogger(__name__)

        class VersionHandler(tornado.web.RequestHandler):
            def get(self):
                self.write(pkg_resources.get_distribution("nexusanalysis").version)

        self.handlers.append((r"/version", VersionHandler))

        self.handlers.append(
            (r'/apidocs', tornado.web.RedirectHandler, {"url": "/apidocs/"}))

        apidocs_path = pkg_resources.resource_filename('webservice.apidocs', '')
        self.handlers.append(
            (
                r'/apidocs/(.*)', tornado.web.StaticFileHandler,
                {'path': str(apidocs_path), "default_filename": "index.html"}))

    def set_modules(self, module_dir, algorithm_config, remote_collections=None, max_request_threads=4):
        for moduleDir in module_dir:
            self.log.info("Loading modules from %s" % moduleDir)
            importlib.import_module(moduleDir)

        self.log.info("Running Nexus Initializers")
        NexusHandler.executeInitializers(algorithm_config)

        self.log.info("Initializing request ThreadPool to %s" % max_request_threads)
        tile_service_factory = partial(NexusTileService, False, False, algorithm_config)
        handler_args_builder = HandlerArgsBuilder(
            max_request_threads,
            tile_service_factory,
            algorithm_config,
            remote_collections=remote_collections
        )

        for clazzWrapper in NexusHandler.AVAILABLE_HANDLERS:
            self.handlers.append(
                (
                    clazzWrapper.path,
                    NexusRequestHandler,
                    handler_args_builder.get_args(clazzWrapper)
                )
            )

        return self

    def enable_static(self, static_dir):
        self.log.info("Using static root path '%s'" % static_dir)
        self.handlers.append(
                (r'/(.*)', tornado.web.StaticFileHandler, {'path': static_dir, "default_filename": "index.html"}))

        return self

    def build(self, host=None, debug=False):

        return tornado.web.Application(
            self.handlers,
            default_host=host,
            debug=debug
        )

