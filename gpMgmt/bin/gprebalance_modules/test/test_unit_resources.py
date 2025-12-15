from gppylib.test.unit.gp_unittest import *
from mock import *

from gprebalance_modules.planner import ResourceEstimator, ResourceError, LogicalMove, Planner
from gppylib.db.dbconn import DbURL
from gprebalance_modules.test.config import initGparrayFromFile
from gprebalance_modules.rebalance_commons import (
    SegmentSize, 
    DiskSpaceChecker, 
    DiskSpaceInfo,
    Host,
    HostStatus
)

def check_query(conn, query):
    if  "SELECT COUNT(1) FROM pg_namespace WHERE nspname =" in query:
        return [0]
    return None

class TestDiskSpaceChecker(GpTestCase):
    """Test cases for DiskSpaceChecker"""
    
    def setUp(self):
        self.logger = Mock()
        self.checker = DiskSpaceChecker(self.logger, batch_size=4)
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    @patch('gprebalance_modules.rebalance_commons.DiskUsage')
    def test_get_disk_usage_success(self, mock_disk_usage_class, mock_pool_class):
        """Test successful disk usage retrieval"""
        # Setup mock pool
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # Create mock commands
        cmd1 = Mock()
        cmd1.was_successful.return_value = True
        cmd1.directory = '/data1/primary/gpseg0'
        cmd1.kbytes_used.return_value = 1048576
        
        cmd2 = Mock()
        cmd2.was_successful.return_value = True
        cmd2.directory = '/data1/primary/gpseg1'
        cmd2.kbytes_used.return_value = 2097152
        
        mock_pool.getCompletedItems.return_value = [cmd1, cmd2]
        
        # Execute
        directories = ['/data1/primary/gpseg0', '/data1/primary/gpseg1']
        result = self.checker.get_disk_usage('sdw1', directories)
        
        # Verify results
        self.assertEqual(len(result), 2)
        self.assertEqual(result['/data1/primary/gpseg0'], 1048576)
        self.assertEqual(result['/data1/primary/gpseg1'], 2097152)
        
        # Verify pool usage
        self.assertEqual(mock_pool.addCommand.call_count, 2)
        mock_pool.join.assert_called_once()
        mock_pool.haltWork.assert_called_once()
        mock_pool.joinWorkers.assert_called_once()
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    def test_get_disk_usage_empty_list(self, mock_pool_class):
        """Test disk usage with empty directory list"""
        result = self.checker.get_disk_usage('sdw1', [])
        
        self.assertEqual(result, {})
        mock_pool_class.assert_not_called()
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    @patch('gprebalance_modules.rebalance_commons.DiskUsage')
    def test_get_disk_usage_command_failure(self, mock_disk_usage_class, mock_pool_class):
        """Test disk usage command failure"""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        cmd = Mock()
        cmd.was_successful.return_value = False
        cmd.get_results.return_value.stderr = "Permission denied"
        
        mock_pool.getCompletedItems.return_value = [cmd]
        
        with self.assertRaises(Exception) as context:
            self.checker.get_disk_usage('sdw1', ['/data1/primary/gpseg0'])
        
        self.assertIn("Unable to check disk usage", str(context.exception))
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    @patch('gprebalance_modules.rebalance_commons.DiskFree')
    @patch('gprebalance_modules.rebalance_commons.pickle')
    @patch('gprebalance_modules.rebalance_commons.base64')
    def test_get_available_space_success(self, mock_base64, mock_pickle, 
                                         mock_disk_free_class, mock_pool_class):
        """Test successful available space retrieval"""
        from gppylib.operations.validate_disk_space import FileSystem
        
        # Setup mock pool
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # Mock FileSystem objects
        fs1 = Mock(spec=FileSystem)
        fs1.name = '/dev/sdb1'
        fs1.disk_free = 10485760
        fs1.directories = ['/data1/primary/gpseg0']
        
        fs2 = Mock(spec=FileSystem)
        fs2.name = '/dev/sdb1'
        fs2.disk_free = 10485760
        fs2.directories = ['/data1/primary/gpseg1']
        
        mock_pickle.loads.return_value = [fs1, fs2]
        mock_base64.urlsafe_b64decode.return_value = b'pickled_data'
        
        cmd = Mock()
        cmd.was_successful.return_value = True
        cmd.get_results.return_value.stdout = 'encoded_data'
        
        mock_pool.getCompletedItems.return_value = [cmd]
        
        # Execute
        directories = ['/data1/primary/gpseg0', '/data1/primary/gpseg1']
        result = self.checker.get_available_space('sdw1', directories)
        
        # Verify
        self.assertEqual(len(result), 2)
        self.assertIn('/data1/primary/gpseg0', result)
        self.assertIn('/data1/primary/gpseg1', result)
        
        self.assertEqual(result['/data1/primary/gpseg0'].filesystem, '/dev/sdb1')
        self.assertEqual(result['/data1/primary/gpseg0'].available_kb, 10485760)
        self.assertEqual(result['/data1/primary/gpseg0'].available_gb, 10.0)
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    @patch('gprebalance_modules.rebalance_commons.DiskFree')
    def test_get_available_space_command_failure(self, mock_disk_free_class, mock_pool_class):
        """Test available space command failure"""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        cmd = Mock()
        cmd.was_successful.return_value = False
        cmd.get_results.return_value.stderr = "No such file or directory"
        
        mock_pool.getCompletedItems.return_value = [cmd]
        
        with self.assertRaises(Exception) as context:
            self.checker.get_available_space('sdw1', ['/data1/primary/gpseg0'])
        
        self.assertIn("Failed to check disk free", str(context.exception))
    
    def test_check_batch_usage_success(self):
        """Test batch disk usage check across multiple hosts"""
        self.checker.get_disk_usage = Mock()
        self.checker.get_disk_usage.side_effect = [
            {'/data1/seg0': 1000000, '/data1/seg1': 2000000},
            {'/data2/seg0': 1500000}
        ]
        
        dirs_by_host = {
            'sdw1': ['/data1/seg0', '/data1/seg1'],
            'sdw2': ['/data2/seg0']
        }
        
        result = self.checker.check_batch_usage(dirs_by_host)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result['sdw1']['/data1/seg0'], 1000000)
        self.assertEqual(result['sdw2']['/data2/seg0'], 1500000)
        
        self.assertEqual(self.checker.get_disk_usage.call_count, 2)
    
class TestResourceEstimator(GpTestCase):
    """Test cases for ResourceEstimator using real GpArray configuration"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.logger = Mock()
        self.logger.info = Mock()
        self.logger.debug = Mock()
        self.logger.warning = Mock()
        self.logger.error = Mock()
        
        self.conn = Mock()
        self.dburl = Mock(spec=DbURL)
        
        # Load real GpArray configuration
        self.gparray = initGparrayFromFile('unbalanced_9_ip')
        
        # Options for planner
        self.options = Mock()
        self.options.target_segment_count = 9
        self.options.target_hosts = None
        self.options.add_hosts = None
        self.options.remove_hosts = None
        self.options.target_datadirs = None
        self.options.target_hosts_file = None
        self.options.add_hosts_file = None
        self.options.remove_hosts_file = None
        self.options.target_datadirs_file = None
        self.options.mirror_mode = 'grouped'
        self.options.skip_rebalance = False
        self.options.skip_resource_estimation = False
        self.options.batch_size = 16
    
    @patch('gprebalance_modules.planner.dbconn')
    def test_estimate_segment_sizes_from_unbalanced_cluster(self, mock_dbconn):
        """Test segment size estimation from unbalanced cluster"""
        # Create a realistic move: move primary seg0 from sdw1 to sdw2
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        self.assertIsNotNone(seg0)
        self.assertEqual(seg0.hostname, 'sdw1')
        self.assertEqual(seg0.address, '172.20.0.6')
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=None
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker to return segment size
        estimator.disk_checker.get_disk_usage = Mock(return_value={
            '/data/primary0': 2097152  # 2GB
        })
        
        # Mock tablespace query (no tablespaces)
        mock_dbconn.query.return_value = []
        
        estimator._estimate_segment_sizes(moves)
        
        # Verify segment size was set
        self.assertIsNotNone(moves[0].segment_size)
        self.assertEqual(moves[0].segment_size.datadir_size_kb, 2097152)
        self.assertEqual(moves[0].segment_size.total_size_kb, 2097152)
        
        # Verify disk usage was called with correct parameters
        estimator.disk_checker.get_disk_usage.assert_called_once_with(
            '172.20.0.6',
            ['/data/primary0']
        )
    
    @patch('gprebalance_modules.planner.dbconn')
    def test_estimate_multiple_segments_same_host(self, mock_dbconn):
        """
        Test estimating multiple segments from same source host
        """
        # Get primaries from sdw1: seg0, seg1, seg2
        segs_from_sdw1 = []
        for seg in self.gparray.getDbList():
            if seg.hostname == 'sdw1' and seg.isSegmentPrimary() and seg.content >= 0:
                segs_from_sdw1.append(seg)
        
        self.assertEqual(len(segs_from_sdw1), 3)
        
        # Create moves for all three segments
        moves = [
            LogicalMove(
                seg=segs_from_sdw1[0],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=None
            ),
            LogicalMove(
                seg=segs_from_sdw1[1],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary1',
                target_port=7001,
                segment_size=None
            ),
            LogicalMove(
                seg=segs_from_sdw1[2],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary2',
                target_port=7002,
                segment_size=None
            )
        ]
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker to return sizes for all segments
        estimator.disk_checker.get_disk_usage = Mock(return_value={
            '/data/primary0': 1048576,  # 1GB
            '/data/primary1': 2097152,  # 2GB
            '/data/primary2': 1572864   # 1.5GB
        })
        
        # Mock tablespace query
        mock_dbconn.query.return_value = []
        
        estimator._estimate_segment_sizes(moves)
        
        # Verify all segment sizes were set
        self.assertEqual(moves[0].segment_size.datadir_size_kb, 1048576)
        self.assertEqual(moves[1].segment_size.datadir_size_kb, 2097152)
        self.assertEqual(moves[2].segment_size.datadir_size_kb, 1572864)
        
        # Verify single call to get_disk_usage with all directories
        estimator.disk_checker.get_disk_usage.assert_called_once()
        call_args = estimator.disk_checker.get_disk_usage.call_args[0]
        self.assertEqual(call_args[0], '172.20.0.6')
        self.assertEqual(set(call_args[1]), {'/data/primary0', '/data/primary1', '/data/primary2'})
    
    @patch('gprebalance_modules.planner.dbconn')
    def test_estimate_with_tablespaces(self, mock_dbconn):
        # Get primary seg0 from sdw1
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=None
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker for datadir and tablespaces
        call_count = [0]
        def mock_disk_usage_side_effect(host, dirs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: datadir
                return {'/data/primary0': 2097152}  # 2GB
            else:
                # Second call: tablespaces
                return {
                    '/tablespace1/2': 524288,   # 512MB
                    '/tablespace2/2': 1048576   # 1GB
                }
        
        estimator.disk_checker.get_disk_usage = Mock(side_effect=mock_disk_usage_side_effect)
        
        # Mock tablespace query to return tablespace locations
        mock_dbconn.query.return_value = [
            (2, '/tablespace1/2'),
            (2, '/tablespace2/2')
        ]
        
        estimator._estimate_segment_sizes(moves)
        
        # Verify segment size includes tablespaces
        self.assertIsNotNone(moves[0].segment_size)
        self.assertEqual(moves[0].segment_size.datadir_size_kb, 2097152)
        self.assertIsNotNone(moves[0].segment_size.tablespace_usage)
        self.assertEqual(moves[0].segment_size.tablespace_usage['/tablespace1/2'], 524288)
        self.assertEqual(moves[0].segment_size.tablespace_usage['/tablespace2/2'], 1048576)
        
        # Total should be datadir + tablespaces
        expected_total = 2097152 + 524288 + 1048576
        self.assertEqual(moves[0].segment_size.total_size_kb, expected_total)
    
    def test_validate_target_space_sufficient(self):
        # Get primary seg0
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=2097152)  # 2GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker - 20GB available (plenty of space)
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=20971520,  # 20GB
                    directory='/data/primary0'
                )
            }
        })
        
        # Should not raise exception
        try:
            estimator._validate_target_space(moves)
        except ResourceError:
            self.fail("ResourceError raised when space is sufficient")
        
        # Verify disk space check was called
        estimator.disk_checker.check_batch_available_space.assert_called_once()
        call_args = estimator.disk_checker.check_batch_available_space.call_args[0][0]
        self.assertIn('172.20.0.7', call_args)
        self.assertIn('/data/primary0', call_args['172.20.0.7'])
    
    def test_validate_target_space_insufficient(self):
        # Get primary seg0
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=10485760)  # 10GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker - only 2GB available (insufficient)
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=2097152,  # 2GB available
                    directory='/data/primary0'
                )
            }
        })
        
        # Should raise ResourceError
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        error_msg = str(context.exception)
        self.assertIn("Insufficient disk space", error_msg)
        self.assertIn("sdw2", error_msg)
    
    def test_validate_multiple_moves_same_filesystem_insufficient(self):
        # Get primaries from sdw1
        segs_from_sdw1 = []
        for seg in self.gparray.getDbList():
            if seg.hostname == 'sdw1' and seg.isSegmentPrimary() and seg.content < 2:
                segs_from_sdw1.append(seg)
        
        moves = [
            LogicalMove(
                seg=segs_from_sdw1[0],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            ),
            LogicalMove(
                seg=segs_from_sdw1[1],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary1',
                target_port=7001,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Total needed: (5GB + 5GB) * 1.1 = 11GB
        # Available: only 8GB (insufficient)
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=8388608,
                    directory='/data/primary0'
                ),
                '/data/primary1': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=8388608,
                    directory='/data/primary1'
                )
            }
        })
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        self.assertIn("Insufficient disk space", str(context.exception))
    
    def test_validate_target_space_no_space_info(self):
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=2097152)
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Return empty space info
        estimator.disk_checker.check_batch_available_space = Mock(return_value={})
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        self.assertIn("No disk space information for host sdw2", str(context.exception))
    
    @patch('gprebalance_modules.planner.DiskSpaceChecker')
    @patch('gprebalance_modules.planner.HostResolver.resolve_hostname')
    @patch('gprebalance_modules.planner.HostResolver.get_address')
    @patch('gprebalance_modules.planner.GreedySolver')
    @patch('gprebalance_modules.rebalance_schema.dbconn.queryRow', side_effect=check_query)
    @patch('gprebalance_modules.planner.dbconn')
    def test_planner_with_resource_estimation(self, mock_dbconn, mock_schema, mock_solver, 
                                               mock_get_address, mock_resolve, mock_disk_check):
        """Test Planner integration with resource estimation"""
        # Setup resolver mocks
        mock_resolve.return_value = None
        def address_side_effect(hostname):
            addr_map = {
                'sdw1': '172.20.0.6',
                'sdw2': '172.20.0.7',
                'sdw3': '172.20.0.8'
            }
            return addr_map.get(hostname, hostname)
        mock_get_address.side_effect = address_side_effect
        
        # Setup solver mock
        mock_solver_instance = Mock()
        mock_solver.return_value = mock_solver_instance
        
        # Mock solution: move segments to balance cluster
        solution = {0: (0, 1),
                    1: (0, 1),
                    2: (0, 1),
                    3: (2, 0),
                    4: (2, 0),
                    5: (1, 2),
                    6: (1, 2),
                    7: (1, 2),
                    8: (2, 0)}

        mock_solver_instance.solve.return_value = (solution, {})
        
        self.options.target_datadirs="/data/primary{content}, /data/mirror{content}"
        # Create planner
        planner = Planner(
            logger=self.logger,
            dburl=self.dburl,
            gpArray=self.gparray,
            options=self.options
        )
        
        # Mock disk usage - all segments are 2GB
        mock_disk_check.return_value.get_disk_usage.return_value = {
            '/data/primary0': 2097152,
            '/data/primary1': 2097152,
            '/data/primary2': 2097152,
            '/data/mirror3': 2097152,
            '/data/mirror4': 2097152,
            '/data/mirror5': 2097152,
            '/data/mirror6': 2097152,
            '/data/mirror7': 2097152,
            '/data/mirror8': 2097152,
        }
            
        # Mock available space - on all targets where we move segments to
        mock_disk_check.return_value.check_batch_available_space.return_value = {
            '172.20.0.7': {
                '/data/primary8': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/primary8'),
                '/data/mirror5': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/mirror5'),
                '/data/mirror6': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/mirror6'),
                '/data/mirror7': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/mirror7'),
            },
            '172.20.0.8': {
                '/data/mirror0': DiskSpaceInfo('/dev/sdc1', 52428800, '/data/mirror0'),
                '/data/mirror1': DiskSpaceInfo('/dev/sdc1', 52428800, '/data/mirror1'),
                '/data/mirror2': DiskSpaceInfo('/dev/sdc1', 52428800, '/data/mirror2'),
            },
        }
        
        mock_dbconn.connect.return_value = self.conn
        mock_dbconn.query.return_value = []
        
        # Execute planning
        plan = planner.plan()
        
        # Verify moves were created
        self.assertIsNotNone(plan.getMoves())
        
        # Verify resource estimation was performed
        for move in plan.getMoves():
            self.assertIsNotNone(move.segment_size, 
                                f"Segment size not set for move: {move}")
    
    @patch('gprebalance_modules.planner.dbconn')
    @patch('gprebalance_modules.rebalance_schema.dbconn.queryRow', side_effect=check_query)
    @patch('gprebalance_modules.planner.HostResolver.resolve_hostname')
    @patch('gprebalance_modules.planner.HostResolver.get_address')
    def test_planner_skips_resource_estimation_when_requested(self, mock_get_address, mock_resolve, mock_schema, mock_conn):
        """Test Planner skips resource estimation when skip_resource_estimation=True"""
        mock_resolve.return_value = None
        def address_side_effect(hostname):
            addr_map = {
                'sdw1': '172.20.0.6',
                'sdw2': '172.20.0.7',
                'sdw3': '172.20.0.8'
            }
            return addr_map.get(hostname, hostname)
        mock_get_address.side_effect = address_side_effect
        
        # Enable skip flag
        self.options.skip_resource_estimation = True
        
        planner = Planner(
            logger=self.logger,
            dburl=self.dburl,
            gpArray=self.gparray,
            options=self.options
        ).plan()
        
        # Verify warning was logged
        self.logger.warning.assert_any_call("Skipping resource estimation")

if __name__ == '__main__':
    run_tests()
