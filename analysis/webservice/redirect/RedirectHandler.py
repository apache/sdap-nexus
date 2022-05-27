import tornado
from webservice.webmodel.RequestParameters import RequestParameters


class RedirectHandler(tornado.web.RequestHandler):

    def initialize(self, redirected_collections):
        self._redirected_collections = redirected_collections

    def get(self, *args, **kwargs):
        ds = self.get_arguments(ds=request.get_argument(RequestParameters.DATASET, None))
        self.redirect(
            self._redirected_collections[ds]['url'],
            permanent=True
        )


