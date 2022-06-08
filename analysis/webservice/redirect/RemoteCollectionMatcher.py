import yaml
from tornado.routing import Matcher
from webservice.webmodel.RequestParameters import RequestParameters
from tornado.httputil import HTTPServerRequest


class RemoteCollectionMatcher(Matcher):
    def __init__(self, collections_config: str):
        self._collections_config = collections_config
        self._remote_collections = None

    def get_remote_collections(self):
        if self._remote_collections is None:
            self._remote_collections = self._get_remote_collections(self._collections_config)
        return self._remote_collections

    @staticmethod
    def _get_remote_collections(collections_config: str):
        _remote_collections = {}
        with open(collections_config, 'r') as f:
            collections_yaml = yaml.load(f, Loader=yaml.FullLoader)
            for collection in collections_yaml['collections']:
                if "path" in collection and collection['path'].startswith('http'):
                    _remote_collections[collection["id"]] = {k.replace('-', '_'): v for k, v in collection.items()}

        return _remote_collections

    def match(self, request: HTTPServerRequest):
        if RequestParameters.DATASET in request.query_arguments:
            # the returmed values are not used because I did not find how to use them
            # just return empty dict() works to signify the request matches
            # TODO do not hardcode utf-8, no time to do better today
            collection = request.query_arguments[RequestParameters.DATASET][0].decode('utf-8')
            if collection in self._remote_collections:
                return dict()

        # when request does not match
        return None