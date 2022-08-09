import tornado.web
import tornado.gen
import logging
from webservice.webmodel.RequestParameters import RequestParameters

logger = logging.getLogger(__name__)


class RedirectHandler(tornado.web.RequestHandler):

    def initialize(self, redirected_collections=None):
        self._redirected_collections = redirected_collections

    @tornado.gen.coroutine
    def get(self, algo):
        collection_id = self.request.query_arguments[RequestParameters.DATASET][0].decode('utf-8')
        collection = self._redirected_collections[collection_id]
        full_url = self.request.full_url()

        #redirect to new URL
        base_url = full_url[:full_url.find(algo)].rstrip('/')
        new_base_url = collection['path'].rstrip('/')
        new_full_url = full_url.replace(base_url, new_base_url)

        # use remote collection id
        if 'remote_id' in collection:
            dataset_param = f"ds={collection_id}"
            new_dataset_param = f"ds={collection['remote_id']}"
            new_full_url = new_full_url.replace(dataset_param, new_dataset_param)
        logger.info("redirect request to ", new_full_url)
        self.redirect(
            new_full_url,
            permanent=True
        )


