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

import unittest
import pkg_resources
import configparser
import sys
import os
import logging
import mock

from webservice.webapp import inject_args_in_config

logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)

log = logging.getLogger(__name__)


class MyTestCase(unittest.TestCase):

    @mock.patch('tornado.options')
    @mock.patch('tornado.options._Option')
    def test_inject_args_in_config_nominal_newoption(self, mock_options, mock_option):

        mock_option.name = 'solr_time_out'
        mock_option.value.return_value = '60'
        mock_options._options = {'solr-time-out' : mock_option}

        algorithm_config = configparser.RawConfigParser()
        algorithm_config.readfp(pkg_resources.resource_stream(__name__, "config/algorithms.ini"),
                                filename='algorithms.ini')

        inject_args_in_config(mock_options, algorithm_config)

        self.assertEqual('60', algorithm_config.get('solr', 'time_out'))

    @mock.patch('tornado.options')
    @mock.patch('tornado.options._Option')
    def test_inject_args_in_config_nominal_existingoption(self, mock_options, mock_option):
        mock_option.name = 'multiprocessing_maxprocesses'
        mock_option.value.return_value = '60'
        mock_options._options = {'multiprocessing-maxprocesses': mock_option}

        algorithm_config = configparser.RawConfigParser()
        algorithm_config.readfp(pkg_resources.resource_stream(__name__, "config/algorithms.ini"),
                                filename='algorithms.ini')

        inject_args_in_config(mock_options, algorithm_config)

        self.assertEqual('60', algorithm_config.get('multiprocessing', 'maxprocesses'))


    @mock.patch('tornado.options')
    @mock.patch('tornado.options._Option')
    def test_inject_args_in_config_nosection(self, mock_options, mock_option):
        mock_option.name = 'port'
        mock_option.value.return_value = '8080'
        mock_options._options = {'port': mock_option}

        algorithm_config = configparser.RawConfigParser()
        algorithm_config.readfp(pkg_resources.resource_stream(__name__, "config/algorithms.ini"),
                                filename='algorithms.ini')

        inject_args_in_config(mock_options, algorithm_config)

        # nothing should happend we just check that there is no section named after the option
        self.assertEqual(False, algorithm_config.has_section('port'))

    @mock.patch('tornado.options')
    @mock.patch('tornado.options._Option')
    def test_sdap_redirection(self):
        mock_option.name = 'collections_path'
        mock_option.value.return_value = os.path.join(
            os.path.dirname(__file__),
            'collections_config.yaml'
        )
        mock_options._options = {'collections_path': mock_option}




if __name__ == '__main__':
    unittest.main()
