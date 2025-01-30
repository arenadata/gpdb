import sys
import yaml
from .gp_unittest import *
from mock import *
from gppylib.gparray import Segment, GpArray
from gppylib.db.dbconn import DbURL
from gppylib.db import catalog
from gppylib.gplog import *
from gppylib.db import dbconn

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), '/../../../gp_rebalance_modules')))

from gprebalance_modules.rebalance import GPRebalance, Host, dbconn, GpArray, ValidationError   # nopep8
from gprebalance_modules.rebalance_validator import ClusterValidator  # nopep8


def initGparrayFromFile(basename):
    filename = os.path.dirname(__file__) + \
        "/data/gprebalance/" + basename + ".array"
    segdbs = []
    with open(filename, 'r') as fp:
        for line in fp:
            if not line.lstrip().startswith('#'):
                segdbs.append(Segment.initFromString(line))
    return GpArray(segdbs, segdbs)


def getTargetHostsFromFile(hostsfile):
    filename = os.path.dirname(__file__) + \
        "/data/gprebalance/" + hostsfile
    with open(filename, 'r') as fp:
        hosts = {}
        config = yaml.safe_load(fp)
        for host_config in config['hosts']:
            key = (host_config['hostname'], host_config['address'])
            hosts[key] = Host(hostname=host_config['hostname'],
                              address=host_config['address'],
                              primary_datadirs=set(
                                  host_config['primary_datadirs']),
                              mirror_datadirs=set(
                host_config['mirror_datadirs']),
                primary_segments=set(), mirror_segments=set())
        return list(hosts.values())


class GpTestValidation(GpTestCase):
    def setUp(self):
        self.logger = Mock(
            spec=['log', 'warn', 'info', 'debug', 'error', 'warning', 'fatal', 'exception'])
        self.apply_patches([
            patch('gprebalance_modules.rebalance.dbconn.DbURL',
                  return_value=Mock(), spec=DbURL),
            patch('gprebalance_modules.rebalance.dbconn.connect',
                  return_value=Mock()),
        ])

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_grouped"))
    def test_already_balanced_grouped(self, mock1):
        strategy = "grouped"
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             strategy)
        self.assertFalse(gp_reb.needs_balance(False, False))

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_spread"))
    def test_already_balanced_spread(self, mock1):
        strategy = "spread"
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             strategy)
        self.assertFalse(gp_reb.needs_balance(False, False))

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_no_mirrors"))
    @patch('gprebalance_modules.rebalance.ask_yesno', return_value=False)
    def test_no_mirrors(self, mock1, mock2):
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             None)
        with self.assertRaises(ValidationError):
            gp_reb.needs_balance(False, False)

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_no_mirrors"))
    def test_no_mirrors_allow_mirrorless(self, mock1):
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             None)
        self.assertFalse(gp_reb.needs_balance(False, True))

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("segments_down"))
    @patch('gprebalance_modules.rebalance.ask_yesno', return_value=False)
    def test_segments_down(self, mock1, mock2):
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             None)
        with self.assertRaises(ValidationError):
            gp_reb.needs_balance(False, False)

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_pos"))
    @patch('gprebalance_modules.rebalance.ask_yesno', return_value=True)
    def test_unbalanced_pos(self, mock1, mock2):
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             None)
        self.assertTrue(gp_reb.needs_balance(False, False))

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_neg"))
    def test_unbalanced_neg(self, mock1):
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             None)
        with self.assertRaises(ValidationError):
            gp_reb.needs_balance(False, True)

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_to_grouped_pos"))
    def test_unbalanced_to_grouped_pos(self, mock1):
        strategy = "grouped"
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             strategy)
        self.assertTrue(gp_reb.needs_balance(False, False))

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_to_spread_pos"))
    def test_unbalanced_to_spread_pos(self, mock1):
        strategy = "spread"
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             strategy)
        self.assertTrue(gp_reb.needs_balance(False, False))

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_to_spread_neg"))
    def test_unbalanced_to_spread_neg(self, mock1):
        strategy = "spread"
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             strategy)
        with self.assertRaises(ValidationError):
            gp_reb.needs_balance(False, False)

    @patch('gprebalance_modules.rebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("unbalanced_unpreferred"))
    @patch('gprebalance_modules.rebalance.ask_yesno', return_value=True)
    def test_unbalanced_unpreferred(self, mock1, mock2):
        strategy = "grouped"
        gp_reb = GPRebalance(self.logger,
                             Mock(),
                             None,
                             strategy)
        self.assertTrue(gp_reb.needs_balance(False, False))
