from webservice.redirect import RedirectHandler
from webservice.redirect import RemoteCollectionMatcher
import tornado


class RedirectAppBuilder:
    def __init__(self, remote_collection_matcher: RemoteCollectionMatcher):
        redirected_collections = remote_collection_matcher.get_remote_collections()
        self.redirect_handler = (r'/(.*)', RedirectHandler, {'redirected_collections': redirected_collections})

    def build(self, host=None, debug=False):
        return tornado.web.Application(
            [self.redirect_handler],
            default_host=host,
            debug=debug
        )
