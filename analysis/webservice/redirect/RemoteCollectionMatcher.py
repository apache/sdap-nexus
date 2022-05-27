import yaml
from webservice.webmodel.RequestParameters import RequestParameters


class RemoteCollectionMatcher(Matcher):
    def __init__(self, collections_config: str):
        self._collections_config = collections_config
        self._redirected_collections = None

    def get_redirected_collections(self):
        if self._redirected_collections is None:
            self._redirected_collections = self.get_redirected_collections(self._collections_config)
        return self._redicted_collections

    @staticmethod
    def _get_redirected_collections(collections_config: str):
        _redirected_collections = {}
        with open(collections_config, 'r') as f:
            collections_yaml = yaml.load(f, Loader=yaml.FullLoader)
            for collection in collections_yaml['collections']:
                if "url" in collection:
                    _redirected_collections[collection["id"]] = collection

        return _redirected_collections

    def match(self, request):
        ds = request.get_argument(RequestParameters.DATASET, None)
        if ds in self._redicted_collections:
            return self._redicted_collection[ds]