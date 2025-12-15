import socket

from gppylib.test.unit.gp_unittest import *
from mock import *
from ..rebalance_commons import HostResolver
from gppylib.commands.unix import Hostname

class TestHostResolver(unittest.TestCase):
    """
    Test suite for HostResolver class
    """
    
    def setUp(self):
        self.resolver = HostResolver()
    
    def tearDown(self):
        self.resolver = None
    
    def test_is_ip_address_valid_ipv4(self):
        self.assertTrue(self.resolver.is_ip_address('192.168.1.1'))
        self.assertTrue(self.resolver.is_ip_address('10.0.0.1'))
        self.assertTrue(self.resolver.is_ip_address('255.255.255.255'))
        self.assertTrue(self.resolver.is_ip_address('0.0.0.0'))
    
    def test_is_ip_address_valid_ipv6(self):
        self.assertTrue(self.resolver.is_ip_address('2001:0db8:85a3::8a2e:0370:7334'))
        self.assertTrue(self.resolver.is_ip_address('::1'))
        self.assertTrue(self.resolver.is_ip_address('fe80::1'))
        self.assertTrue(self.resolver.is_ip_address('::'))
    
    def test_is_ip_address_invalid(self):
        self.assertFalse(self.resolver.is_ip_address('hostname'))
        self.assertFalse(self.resolver.is_ip_address('not-an-ip'))
        self.assertFalse(self.resolver.is_ip_address('999.999.999.999'))
        self.assertFalse(self.resolver.is_ip_address('192.168.1'))
        self.assertFalse(self.resolver.is_ip_address(''))
        self.assertFalse(self.resolver.is_ip_address('192.168.1.1.1'))
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_success_ipv4(self, mock_getaddrinfo):
        # Mock socket.getaddrinfo to return IPv4 address
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        result = self.resolver.resolve_hostname('testhost')
        
        self.assertEqual(result, '192.168.1.10')
        mock_getaddrinfo.assert_called_once_with(
            'testhost', None, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        # Check caching
        self.assertEqual(self.resolver._hostname_to_ips['testhost'], '192.168.1.10')
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_success_ipv6(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('2001:db8::1', 0, 0, 0))
        ]
        
        result = self.resolver.resolve_hostname('testhost6')
        
        self.assertEqual(result, '2001:db8::1')
        self.assertEqual(self.resolver._hostname_to_ips['testhost6'], '2001:db8::1')
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_multiple_ips(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.11', 0))
        ]
        
        result = self.resolver.resolve_hostname('multihost')
        
        # Should return the first IP
        self.assertEqual(result, '192.168.1.10')
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_cached(self, mock_getaddrinfo):
        self.resolver._hostname_to_ips['cached'] = '192.168.1.100'
        
        result = self.resolver.resolve_hostname('cached')
        
        self.assertEqual(result, '192.168.1.100')
        mock_getaddrinfo.assert_not_called()
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_failure(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = socket.gaierror('Name or service not known')
        
        result = self.resolver.resolve_hostname('nonexistent')
        
        self.assertIsNone(result)
        # Should not crash, just return None
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_socket_error(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = socket.error('Network unreachable')
        
        result = self.resolver.resolve_hostname('unreachable')
        
        self.assertIsNone(result)
        
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_success(self, mock_hostname_class):
        # Mock the Hostname command
        mock_cmd = MagicMock()
        mock_cmd.get_hostname.return_value = 'testhost'
        mock_hostname_class.return_value = mock_cmd
        
        result = self.resolver.resolve_ip('192.168.1.10')
        
        self.assertEqual(result, 'testhost')
        mock_hostname_class.assert_called_once()
        mock_cmd.run.assert_called_once()
        # Check caching
        self.assertEqual(self.resolver._ip_to_hostnames['192.168.1.10'], 'testhost')
    
    @patch('gppylib.commands.unix.Hostname')
    def test_resolve_ip_cached(self, mock_hostname_class):
        self.resolver._ip_to_hostnames['192.168.1.100'] = 'cached'
        
        result = self.resolver.resolve_ip('192.168.1.100')
        
        self.assertEqual(result, 'cached')
        mock_hostname_class.assert_not_called()
    
    @patch('gppylib.commands.unix.Hostname')
    def test_resolve_ip_invalid_ip(self, mock_hostname_class):
        result = self.resolver.resolve_ip('not-an-ip')
        
        self.assertIsNone(result)
        mock_hostname_class.assert_not_called()
    
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_failure(self, mock_hostname_class):
        mock_cmd = MagicMock()
        mock_cmd.run.side_effect = Exception('Resolution failed')
        mock_hostname_class.return_value = mock_cmd
        
        result = self.resolver.resolve_ip('192.168.1.99')
        
        self.assertIsNone(result)
    
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_ipv6(self, mock_hostname_class):
        mock_cmd = MagicMock()
        mock_cmd.get_hostname.return_value = 'testhost6'
        mock_hostname_class.return_value = mock_cmd
        
        result = self.resolver.resolve_ip('2001:db8::1')
        
        self.assertEqual(result, 'testhost6')
        
    def test_hosts_match_identical_hostnames(self):
        self.assertTrue(self.resolver.hosts_match('testhost', 'testhost'))
    
    def test_hosts_match_identical_ips(self):
        self.assertTrue(self.resolver.hosts_match('192.168.1.10', '192.168.1.10'))
    
    def test_hosts_match_different_ips(self):
        self.assertFalse(self.resolver.hosts_match('192.168.1.10', '192.168.1.11'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_ip_to_hostname(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        # IP matches hostname's resolved IP
        self.assertTrue(self.resolver.hosts_match('192.168.1.10', 'testhost'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_ip_to_hostname_no_match(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        # IP doesn't match hostname's resolved IP
        self.assertFalse(self.resolver.hosts_match('192.168.1.99', 'testhost'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_hostname_to_ip(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        # Hostname's resolved IP matches IP
        self.assertTrue(self.resolver.hosts_match('testhost', '192.168.1.10'))
    
    def test_hosts_match_different_hostnames(self):
        self.assertFalse(self.resolver.hosts_match('host1', 'host2'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_resolution_failure(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = socket.gaierror('Resolution failed')
        
        # Should return False when resolution fails
        self.assertFalse(self.resolver.hosts_match('192.168.1.10', 'unknown'))
    
    def test_hosts_match_ipv6(self):
        self.assertTrue(self.resolver.hosts_match('2001:db8::1', '2001:db8::1'))
        self.assertFalse(self.resolver.hosts_match('2001:db8::1', '2001:db8::2'))
        
    def test_find_matching_hostname_exact_match(self):
        existing_hosts = ['host1', 'host2', 'host3']
        
        result = self.resolver.find_matching_hostname('host2', existing_hosts)
        
        self.assertEqual(result, 'host2')
    
    @patch('socket.getaddrinfo')
    def test_find_matching_hostname_ip_match(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        existing_hosts = ['host1', 'host2']
        
        result = self.resolver.find_matching_hostname('192.168.1.10', existing_hosts)
        
        # Should match host1 if it resolves to 192.168.1.10
        self.assertIsNotNone(result)
    
    def test_find_matching_hostname_no_match(self):
        existing_hosts = ['host1', 'host2']
        
        result = self.resolver.find_matching_hostname('nonexistent', existing_hosts)
        
        self.assertIsNone(result)
    
    def test_find_matching_hostname_empty_list(self):
        result = self.resolver.find_matching_hostname('testhost', [])
        
        self.assertIsNone(result)
    
        
    @patch('socket.getaddrinfo')
    def test_caching_integration(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        # First call should hit the network
        result1 = self.resolver.resolve_hostname('testhost')
        self.assertEqual(result1, '192.168.1.10')
        self.assertEqual(mock_getaddrinfo.call_count, 1)
        
        # Second call should use cache
        result2 = self.resolver.resolve_hostname('testhost')
        self.assertEqual(result2, '192.168.1.10')
        self.assertEqual(mock_getaddrinfo.call_count, 1)  # Still 1, not called again
        
        # Third call to get from cache directly
        result3 = self.resolver.get_address('testhost')
        self.assertEqual(result3, '192.168.1.10')
    
    @patch('socket.getaddrinfo')
    def test_multiple_hosts(self, mock_getaddrinfo):
        def getaddrinfo_side_effect(hostname, *args):
            mapping = {
                'host1': [( socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))],
                'host2': [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.11', 0))],
                'host3': [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.12', 0))]
            }
            return mapping.get(hostname, [])
        
        mock_getaddrinfo.side_effect = getaddrinfo_side_effect
        
        existing_hosts = ['host1', 'host2', 'host3']
        
        # Test various scenarios
        self.assertEqual(self.resolver.find_matching_hostname('host1', existing_hosts), 'host1')
        self.assertEqual(self.resolver.find_matching_hostname('192.168.1.11', existing_hosts), 'host2')
        self.assertIsNone(self.resolver.find_matching_hostname('192.168.1.99', existing_hosts))
        
        # Test hosts_match
        self.assertTrue(self.resolver.hosts_match('host1', '192.168.1.10'))
        self.assertFalse(self.resolver.hosts_match('host1', '192.168.1.11'))

if __name__ == '__main__':
    run_tests()
