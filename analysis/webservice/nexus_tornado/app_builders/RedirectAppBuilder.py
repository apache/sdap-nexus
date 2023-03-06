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
