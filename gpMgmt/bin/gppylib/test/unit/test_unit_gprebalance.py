
import sys
import io
import os
import imp

from .gp_unittest import *
from mock import *
from gppylib.gparray import Segment, GpArray
from gppylib.db.dbconn import DbURL
from gppylib.db import catalog
from gppylib.gplog import *
from gppylib.system.configurationInterface import GpConfigurationProvider
from gppylib.system.environment import GpCoordinatorEnvironment
from gppylib.db import dbconn


class GpTestRebalance(GpTestCase):
    def setUp(self):
        gprebalance_file = os.path.abspath(
            os.path.dirname(__file__) + "/../../../gprebalance")
        self.subject = imp.load_source('gprebalance', gprebalance_file)
        self.old_sys_argv = sys.argv
        sys.argv = []
        self.options, self.args, self.parser = self.subject.cli.parseargs()
        self.options.mirroring = 'grouped'

        self.subject.logger = Mock(
            spec=['log', 'warn', 'info', 'debug', 'error', 'warning', 'fatal', 'exception'])

        self.subject.check_running_gputils = Mock(return_value=False)

        self.apply_patches([
            patch('builtins.open', mock_open(), create=True),
            patch('builtins.input'),
            patch('gprebalance.dbconn.DbURL', return_value=Mock(), spec=DbURL),
            patch('gprebalance.dbconn.connect', return_value=Mock()),
            patch('gprebalance.GpCoordinatorEnvironment',
                  return_value=Mock(), spec=GpCoordinatorEnvironment),
            patch('gprebalance.configurationInterface.getConfigurationProvider'),
            patch('os.path.exists', return_value=Mock()),
            patch('gprebalance.get_default_logger',
                  return_value=self.subject.logger),
        ])
        self.input_mock = self.get_mock_from_apply_patch("input")
        self.getConfigProviderFunctionMock = self.get_mock_from_apply_patch(
            "getConfigurationProvider")
        self.gpCoordinatorEnvironmentMock = self.get_mock_from_apply_patch(
            "GpCoordinatorEnvironment")
        self.previous_coordinator_data_directory = os.getenv(
            'COORDINATOR_DATA_DIRECTORY', '')
        os.environ["COORDINATOR_DATA_DIRECTORY"] = '/tmp/dirdoesnotexist'
        configProviderMock = Mock(spec=GpConfigurationProvider)
        self.getConfigProviderFunctionMock.return_value = configProviderMock
        configProviderMock.initializeProvider.return_value = configProviderMock
        self.gpCoordinatorEnvironmentMock.return_value.getCoordinatorPort.return_value = 123456

    def tearDown(self):
        os.environ['COORDINATOR_DATA_DIRECTORY'] = self.previous_coordinator_data_directory
        sys.argv = self.old_sys_argv
        super(GpTestRebalance, self).tearDown()


if __name__ == '__main__':
    run_tests()
