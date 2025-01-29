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
import io
import sys


def initGparrayFromFile(basename):
    filename = os.path.dirname(__file__) + \
        "/data/gprebalance/" + basename + ".array"
    segdbs = []
    with open(filename, 'r') as fp:
        for line in fp:
            if not line.lstrip().startswith('#'):
                segdbs.append(Segment.initFromString(line))
    return GpArray(segdbs, segdbs)


class GpTestRebalance(GpTestCase):
    def setUp(self):
        gprebalance_file = os.path.abspath(
            os.path.dirname(__file__) + "/../../../gprebalance")
        self.subject = imp.load_source('gprebalance', gprebalance_file)
        self.old_sys_argv = sys.argv
        sys.argv = []
        self.options, self.args, self.parser = self.subject.parseargs()
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

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_grouped"))
    def test_already_balanced_grouped(self, mock1):
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Cluster is already balanced")

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_spread"))
    def test_already_balanced_spread(self, mock1):
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Cluster is already balanced")

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_no_mirrors"))
    def test_no_mirrors(self, mock1):
        self.input_mock.return_value = "N"
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            "User Aborted. Exiting...")
        self.input_mock.return_value = "Y"
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Cluster is already balanced")

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_no_mirrors"))
    def test_no_mirrors_allow_mirrorless(self, mock1):
        saved = self.options.allow_mirrorless
        self.options.allow_mirrorless = True
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Cluster is already balanced")
        self.options.allow_mirrorless = saved

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("segments_down"))
    def test_segments_down(self, mock1):
        self.input_mock.return_value = "N"
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            "User Aborted. Exiting...")
        self.input_mock.return_value = "Y"
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Cluster is already balanced")

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_pos"))
    def test_unbalanced_pos(self, mock1):
        self.input_mock.return_value = "Y"
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Validation passed. Preparing rebalance...")

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_neg"))
    def test_unbalanced_neg(self, mock1):
        self.input_mock.return_value = "Y"
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_called_with("gprebalance failed: Cannot evenly distribute 5 segments across 2 hosts. "
                                                     "\n\nExiting...")

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_to_grouped_pos"))
    def test_unbalanced_to_grouped_pos(self, mock1):
        saved = self.options.mirroring
        self.options.mirroring = 'grouped'
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Validation passed. Preparing rebalance...")
        self.options.mirroring = saved

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_to_spread_pos"))
    def test_unbalanced_to_spread_pos(self, mock1):
        saved = self.options.mirroring
        self.options.mirroring = 'spread'
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Validation passed. Preparing rebalance...")
        self.options.mirroring = saved

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_to_spread_neg"))
    def test_unbalanced_to_spread_neg(self, mock1):
        saved = self.options.mirroring
        self.options.mirroring = 'spread'
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_called_with(
            "gprebalance failed: Cannot support spread mirroring strategy on given configuration."
            " Use cluster utilities like gpresize or gpexpand to get desired cluster configuration \n\nExiting...")
        self.options.mirroring = saved

    @patch('gprebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_unpreferred"))
    def test_unbalanced_unpreferred(self, mock1):
        self.input_mock.return_value = "Y"
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Validation passed. Preparing rebalance...")


if __name__ == '__main__':
    run_tests()
