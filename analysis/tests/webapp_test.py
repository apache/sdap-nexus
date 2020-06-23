import unittest
import pkg_resources
import ConfigParser
import sys
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

        algorithm_config = ConfigParser.RawConfigParser()
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

        algorithm_config = ConfigParser.RawConfigParser()
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

        algorithm_config = ConfigParser.RawConfigParser()
        algorithm_config.readfp(pkg_resources.resource_stream(__name__, "config/algorithms.ini"),
                                filename='algorithms.ini')

        inject_args_in_config(mock_options, algorithm_config)

        # nothing should happend we just check that there is no section named after the option
        self.assertEqual(False, algorithm_config.has_section('port'))


if __name__ == '__main__':
    unittest.main()
