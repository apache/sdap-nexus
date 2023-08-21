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

import tornado
import webservice.algorithms_spark.NexusCalcSparkHandler
from .SparkContextBuilder import SparkContextBuilder


class HandlerArgsBuilder:
    def __init__(
            self,
            max_request_threads,
            tile_service_factory,
            algorithm_config,
            remote_collections=None
    ):
        self.request_thread_pool = tornado.concurrent.futures.ThreadPoolExecutor(max_request_threads)
        self.tile_service_factory = tile_service_factory
        self.algorithm_config = algorithm_config
        self.remote_collections = remote_collections

    @staticmethod
    def handler_needs_algorithm_config(class_wrapper):
        return (
                class_wrapper == webservice.algorithms_spark.Matchup.Matchup
                or class_wrapper == webservice.algorithms_spark.MatchupDoms.MatchupDoms
                or issubclass(class_wrapper, webservice.algorithms.doms.BaseDomsHandler.BaseDomsQueryCalcHandler)
                or issubclass(class_wrapper, webservice.algorithms_spark.NexusCalcSparkTornadoHandler.NexusCalcSparkTornadoHandler)
                or class_wrapper == webservice.algorithms.doms.ResultsRetrieval.DomsResultsRetrievalHandler
                or class_wrapper == webservice.algorithms.doms.ResultsPlotQuery.DomsResultsPlotHandler
        )

    @staticmethod
    def handler_needs_remote_collections(class_wrapper):
        return class_wrapper == webservice.algorithms.DataSeriesList.DataSeriesListCalcHandlerImpl

    def get_args(self, clazz_wrapper):
        args = dict(
            clazz=clazz_wrapper,
            tile_service_factory=self.tile_service_factory,
            thread_pool=self. request_thread_pool
        )

        if issubclass(clazz_wrapper, webservice.algorithms_spark.NexusCalcSparkHandler.NexusCalcSparkHandler):
            args['sc'] = SparkContextBuilder.get_spark_context()

        if self.handler_needs_algorithm_config(clazz_wrapper):
            args['config'] = self.algorithm_config

        if self.handler_needs_remote_collections(clazz_wrapper):
            args['remote_collections'] = self.remote_collections

        return args
