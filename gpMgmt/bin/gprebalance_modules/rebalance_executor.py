import base64
import fcntl
import multiprocessing
import time
from collections import defaultdict
import pickle
import shutil
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum
from gprebalance_modules.rebalance_plan import Move, Plan  # nopep8
from gprebalance_modules.rebalance_status import StatusManager, RebalanceStatus, MoveStatus, SqlError
from gprebalance_modules.rebalance import ClusterState, SegmentId, SegmentSize, Host
from gppylib.gparray import GpArray, Segment
from gppylib.db import dbconn
from gppylib.commands.base import *
from gppylib.commands.gp import *
from gppylib.commands.unix import DiskFree, DiskUsage
from gppylib.operations.validate_disk_space import FileSystem
from gppylib.parseutils import *
from gppylib.programs.clsRecoverSegment import GpRecoverSegmentProgram
from gppylib.system import configurationInterface, configurationImplGpdb, fileSystemInterface, \
    fileSystemImplOs, osInterface, osImplNative, faultProberInterface, faultProberImplGpdb

MAX_BATCH_SIZE = 128
FILENAME = "/move_"
CONF_DIR = "/rebalance"

begining_timestamp = None


class InsufficientDiskSpaceError(Exception):
    pass


class NoValidDataDirectories(Exception):
    pass


class RecoveryProcess:
    @staticmethod
    def run_recovery(cmd_args: list, result_queue: multiprocessing.Queue, log_file: str):
        try:
            log_fd = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
            flags = fcntl.fcntl(log_fd, fcntl.F_GETFL)
            fcntl.fcntl(log_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            os.dup2(log_fd, 1)  # stdout
            os.dup2(log_fd, 2)  # stderr
            os.close(log_fd)

            # Register all necessary interfaces to run a gprecoverseg
            # in a separate process
            configurationInterface.registerConfigurationProvider(
                configurationImplGpdb.GpConfigurationProviderUsingGpdbCatalog())
            fileSystemInterface.registerFileSystemProvider(
                fileSystemImplOs.GpFileSystemProviderUsingOs())
            osInterface.registerOsProvider(
                osImplNative.GpOsProviderUsingNative())
            faultProberInterface.registerFaultProber(
                faultProberImplGpdb.GpFaultProberImplGpdb())

            local_parser = GpRecoverSegmentProgram.createParser()
            local_options, args = local_parser.parse_args(cmd_args)

            # Create and run the program
            cmd = GpRecoverSegmentProgram.createProgram(local_options, args)
            cmd.run()

        except SystemExit as e:
            error_msg = None
            if e.code != 0:
                error_msg = f"Gprecoverseg failed with exit code: {e.code}. See the {log_file}"
            result_queue.put({
                "status": "FAILED" if e.code != 0 else "SUCCESS",
                "error": error_msg
            })
        except Exception as e:
            error_msg = f"Error in gprecoverseg process: {str(e)}"
            result_queue.put({
                "status": "FAILED",
                "error": error_msg
            })
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            cmd.cleanup()


class SingleMoveCommand(SQLCommand):
    def __init__(self, name: str, status_url: dbconn.DbURL, step_details, logger):
        self.status_url = status_url
        self.logger = logger
        (self.segment, self.move, self.size,
         self.conf_dir, self.needs_switch) = step_details

        self.move_error = False
        self.filename = None

        SQLCommand.__init__(self, name)

    def __del__(self):
        if self.filename is not None:
            if os.path.exists(self.filename):
                os.unlink(self.filename)

    def write_gprecoverseg_config(self):
        filename = self.conf_dir + FILENAME + "dbid" + str(self.segment.dbid)
        with open(filename, 'w') as fp:
            line = (f"{canonicalize_address(self.segment.address)}|"
                    f"{self.segment.port}|{self.segment.datadir} "
                    f"{canonicalize_address(self.move.dstHost.address)}|"
                    f"{self.move.target_port}|"
                    f"{self.move.target_datadir}/gpseg{self.move.segid.contentid}")
            self.logger.info(
                "About to run gprecoverseg for mirror move "
                f"(dbid = {self.segment.dbid}, content = {self.segment.content}) {line}")
            fp.write(line)
        return filename

    def run(self, validateAfter=False):
        status_conn = None
        try:
            status_conn = dbconn.connect(self.status_url, encoding='UTF8')
            StatusManager.record_move(status_conn, self.move, self.segment,
                                      self.size, datetime.datetime.now())

            self.filename = self.write_gprecoverseg_config()
            log_file = os.path.join(self.conf_dir,
                                    f"gprecoverseg_dbid{self.segment.dbid}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                                    )

            # Prepare command arguments
            cmd_args = [
                '-i', self.filename,
                '-B', '1',
                '-v', '-a'
            ]
            try:
                StatusManager.update_record_status(
                    status_conn, [self.segment.dbid], MoveStatus.IN_PROGRESS)
            except SqlError:
                raise Exception(
                    f"Could not update status for move with dbid={self.segment.dbid}: ")

            result_queue = multiprocessing.Queue()
            recovery_process = multiprocessing.Process(
                target=RecoveryProcess.run_recovery,
                args=(cmd_args, result_queue, log_file)
            )
            recovery_process.start()
            result = result_queue.get()
            recovery_process.join()
            if result["status"] == "FAILED":
                self.logger.error(
                    f"Could not perform mirror dbid={self.segment.dbid} "
                    f"move with content {self.segment.content} due to "
                    f"recoverseg error: {result['error']}\n"
                    "Check the gprecoverseg l og file, fix any problems, and re-run"
                )
                try:
                    StatusManager.update_record_status(
                        status_conn, [self.segment.dbid], MoveStatus.FAILED)
                except SqlError:
                    raise Exception(
                        f"Could not update status for move with dbid={self.segment.dbid}: ")

                self.move_error = True
                status_conn.close()
                return
            if self.needs_switch:
                try:
                    StatusManager.update_record_status(
                        status_conn, [self.segment.dbid], MoveStatus.AWAITS_SWITCH)
                except SqlError:
                    raise Exception(
                        f"Could not update status for move with dbid={self.segment.dbid}: ")

            try:
                StatusManager.update_record_status(
                    status_conn, [self.segment.dbid], MoveStatus.COMPLETED)
            except SqlError:
                raise Exception(
                    f"Could not update status for move with dbid={self.segment.dbid}: ")
            status_conn.close()

        except Exception as ex:
            self.logger.error(ex.__str__().strip())
            self.move_error = True
            status_conn.close()
            return


class FilesystemSpace:
    def __init__(self, filesystem: FileSystem, directories: Set[str]):
        self.filesystem = filesystem
        self.directories = directories  # dirs on this filesystem
        self.available_space = filesystem.disk_free
        self.planned_usage = 0

    def reserve_space(self, size: int):
        self.planned_usage += size

    def can_accommodate(self, size: int) -> bool:
        return (self.available_space - self.planned_usage) >= size


def _target_filesystems(addr: str, directories: List[str], batch_size) -> List[FileSystem]:
    filesystems = []  # list of FileSystem()
    pool = WorkerPool(numWorkers=min(len(directories), batch_size))
    try:
        cmd = DiskFree(addr, directories)
        pool.addCommand(cmd)
        pool.join()
    finally:
        pool.haltWork()
        pool.joinWorkers()
    for cmd in pool.getCompletedItems():
        if not cmd.was_successful():
            raise Exception("Failed to check disk free on target segment: {}" .format(
                cmd.get_results().stderr))
        filesystems = pickle.loads(
            base64.urlsafe_b64decode(cmd.get_results().stdout))
    return filesystems


class HostResources:
    def __init__(self, host: Host, ports: tuple[Set[int], Set[int]]):
        self.host_address = host.address
        self.primary_datadirs = host.primary_datadirs
        self.mirror_datadirs = host.mirror_datadirs
        self.used_primary_ports, self.used_mirror_ports = ports
        self.filesystem_spaces: List[FilesystemSpace] = []
        self.hostname = host.hostname

        # Initialize filesystem tracking for all directories
        all_dirs = host.primary_datadirs.union(host.mirror_datadirs)
        self._init_filesystem_spaces(all_dirs)
        self.base_port = self._determine_base_port()

    def _init_filesystem_spaces(self, directories: Set[str]):
        """Initialize filesystem space tracking for all directories"""
        filesystems = _target_filesystems(
            self.host_address, list(directories), MAX_BATCH_SIZE)

        # Group directories by filesystem
        for fs in filesystems:
            dirs_on_fs = {d for d in directories if d in fs.directories}
            self.filesystem_spaces.append(FilesystemSpace(fs, dirs_on_fs))

    def accommodate_segment(self, segment_size: SegmentSize, target_datadir: str):
        """Check if segment can be accommodated considering all its space requirements"""

        # Find filesystem for main datadir and add space requirement
        datadir_fs = self._get_filesystem_for_dir(target_datadir)
        if not datadir_fs:
            raise Exception(f"Host {self.hostname} does not have any valid primary "
                            f"datadirs for segment")
        if datadir_fs.can_accommodate(segment_size.source_data_dir_usage):
            datadir_fs.reserve_space(segment_size.source_data_dir_usage)

        # Add tablespace requirements to respective filesystems
        if segment_size.source_tablespace_usage:
            for tblspc_dir, usage in segment_size.source_tablespace_usage.items():
                tblspc_fs = self._get_filesystem_for_dir(tblspc_dir)
                if not tblspc_fs:
                    raise Exception(f"Host {self.hostname} does not have any valid primary "
                                    f"datadirs for segment")
                if tblspc_fs.can_accommodate(usage):
                    tblspc_fs.reserve_space(usage)

    def _get_filesystem_for_dir(self, directory: str) -> Optional[FilesystemSpace]:
        """Find FilesystemSpace object containing given directory"""
        # First check existing filesystem mappings
        for fs_space in self.filesystem_spaces:
            if directory in fs_space.directories:
                return fs_space

        # If not found, fetch filesystem info for this directory
        filesystems = _target_filesystems(
            self.host_address, [directory], MAX_BATCH_SIZE)

        if not filesystems:
            return None

        # Check if the filesystem already exists in our list
        fs = filesystems[0]
        for existing_fs in self.filesystem_spaces:
            if existing_fs.filesystem.name == fs.name:
                existing_fs.directories.add(directory)
                return existing_fs

        # If not, create new FilesystemSpace
        new_fs_space = FilesystemSpace(fs, {directory})
        self.filesystem_spaces.append(new_fs_space)
        return new_fs_space

    def _determine_base_port(self) -> int:
        """Determine base port from existing port assignments"""
        all_ports = self.used_primary_ports | self.used_mirror_ports

        # Find the most common base port
        port_bases = defaultdict(int)
        for port in all_ports:
            # For each port, calculate what base port it might correspond to
            # assuming port = base + (content * 2) [+ 1 for mirrors]
            for content in range(0, 128):  # reasonable content_id range
                if port % 2 == 0:  # primary
                    possible_base = port - (content * 2)
                else:  # mirror
                    possible_base = port - (content * 2) - 1

                if possible_base > 0:
                    port_bases[possible_base] += 1

        if not port_bases:
            return 7000

        # Return the most frequently occurring base port
        return max(port_bases.items(), key=lambda x: x[1])[0]

    def can_accommodate_port(self, is_mirror: bool, content_id: int) -> Optional[int]:
        """
        Find available port for segment using existing base port pattern
        Returns suitable port number or None if no port available
        """
        used_ports = self.used_mirror_ports if is_mirror else self.used_primary_ports

        # Calculate port based on content_id and base port
        port = self.base_port + (content_id * 2)
        if is_mirror:
            port += 1

        if port not in used_ports:
            return port

        # If standard port not available, try finding next available port
        # maintaining the same even/odd pattern
        start_port = max(used_ports) + 2 if used_ports else self.base_port
        if start_port % 2 != (0 if not is_mirror else 1):
            start_port += 1

        current_port = start_port
        while current_port < 65536:  # Max TCP port
            if current_port not in used_ports:
                return current_port
            current_port += 2  # Keep even/odd pattern

        return None

    def reserve_port(self, port: int, is_mirror: bool):
        """Reserve port for segment"""
        if is_mirror:
            self.used_mirror_ports.add(port)
        else:
            self.used_primary_ports.add(port)


class RebalanceExecutor:
    def __init__(self,
                 plan: Plan,
                 original_array: GpArray,
                 segmentMap: Dict[SegmentId, Segment],
                 cluster_state: ClusterState,
                 logger,
                 statusManager: StatusManager,
                 conn: dbconn.Connection,
                 dburl: dbconn.DbURL,
                 options,
                 ):
        self.moves = plan.moves
        self.plan = plan
        self.logger = logger
        self.gparr = original_array
        self.cluster_state = cluster_state
        self.segmentMap = segmentMap
        self.conn = conn
        self.statusManager = statusManager
        self.options = options
        self.dburl = dburl
        self.to_delete = []
        segids = []
        for m in plan.moves:
            segids.append(m.segid)
        self.segmentSizes = self.estimateSegmentSizes(segids)
        self.resources = self.initializeHostResources(plan.moves)
        self.queue = None

    def __del__(self):
        for to_delete in self.to_delete:
            self.logger.info(f"removing {to_delete}")
            shutil.rmtree(to_delete)

    def initializeHostResources(self, moves: List[Move]):
        resources = {}
        for m in moves:
            prim_ports = set()
            mir_ports = set()
            for psid in m.dstHost.primary_segments:
                prim_ports.add(self.segmentMap[psid].port)
            for msid in m.dstHost.mirror_segments:
                mir_ports.add(self.segmentMap[msid].port)
            resources[m.dstHost] = HostResources(
                m.dstHost, (prim_ports, mir_ports))
        return resources

    def _disk_usage(self, hostaddr: str, dirs: List[str]) -> Dict[str, int]:
        """
        Get the Disk usage for the given set of directories to the targeted host
        input: hostaddr , host from which the disk usage is fetched
        input: dirs, list of directories to fetch the details
        output: dictionary containing directories with it's disk usage stats in kb(kilo byte)
        """
        dirs_disk_usage = {}  # map of directories to disk usage

        if len(dirs) <= 0:
            return dirs_disk_usage

        pool = WorkerPool(numWorkers=min(len(dirs), self.options.batch_size))
        try:
            for directory in dirs:
                cmd = DiskUsage('check source segments disk space used',
                                directory, ctxt=REMOTE, remoteHostAddr=hostaddr)
                pool.addCommand(cmd)
            pool.join()
        finally:
            pool.haltWork()
            pool.joinWorkers()

        for cmd in pool.getCompletedItems():
            if not cmd.was_successful():
                raise Exception("Unable to check disk usage on source segment: {}" .format(
                    cmd.get_results().stderr))

            dirs_disk_usage[cmd.directory] = cmd.kbytes_used()

        return dirs_disk_usage

    def estimateSegmentSizes(self, seglist: List[SegmentId]) -> Dict[SegmentId, SegmentSize]:
        if not seglist:
            return {}
        oid_subq = """ (SELECT *
                    FROM (
                        SELECT oid FROM pg_tablespace
                        WHERE spcname NOT IN ('pg_default', 'pg_global')
                        ) AS _q1,
                        LATERAL gp_tablespace_location(_q1.oid)
                    ) AS t """
        segment_dbids = ','.join(f'({seg.dbid})' for seg in seglist)
        tablespace_location_sql = """
                SELECT c.dbid, c.content, t.tblspc_loc||'/'||c.dbid tblspc_loc
                FROM {oid_subq}
                    JOIN gp_segment_configuration AS c
                    ON t.gp_segment_id = c.content WHERE c.dbid in (VALUES {segment_ids_str})
                """ .format(oid_subq=oid_subq, segment_ids_str=segment_dbids)
        cursor = dbconn.query(self.conn, tablespace_location_sql)
        tablespaces = defaultdict(list)
        for dbid, content, loc in cursor:
            tablespaces[SegmentId(dbid, content)].append(loc)

        segmentSizes = {}
        for segid in seglist:
            sourceSeg = self.segmentMap[segid]
            source_data_dir_usage = self._disk_usage(
                sourceSeg.address, [sourceSeg.datadir])
            segmentSizes[segid] = SegmentSize(
                source_data_dir_usage[sourceSeg.datadir], None)
            self.to_delete.append(sourceSeg.datadir)
        for segid, tblspace_dirs in tablespaces.items():
            sourceSeg = self.segmentMap[segid]
            source_tblsps_usage = self._disk_usage(
                sourceSeg.address, tblspace_dirs)
            segmentSizes[segid].source_tablespace_usage = source_tblsps_usage
            self.to_delete.extend(tblspace_dirs)

        return segmentSizes

    def _prepare_swaps(self, swaps: List[Tuple[Move, Move]]):
        """
        Choose the target directory for swap case:
        1. primary is moved to mirror dir in its own host
        2. mirror is moved to primary dir in its own host
        3. role switching takes place
        """
        for primary_move, mirror_move in swaps:
            primary_host = primary_move.srcHost
            mirror_host = mirror_move.srcHost

            primary_id = primary_move.segid

            # define datadir
            for datadir in primary_host.mirror_datadirs:
                try:
                    self.resources[primary_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    primary_move.target_datadir = datadir
                    break
                except:
                    continue
            if primary_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {primary_host.hostname} does not have any valid primary "
                                             f"datadirs for segment {mirror_move.segid}")
            primary_move.dstHost = primary_host
            for datadir in mirror_host.primary_datadirs:
                try:
                    self.resources[mirror_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    mirror_move.target_datadir = datadir
                    break
                except:
                    continue
            if mirror_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {mirror_host.hostname} does not have any valid primary "
                                             f"datadirs or ports for segment {mirror_move.segid}")
            mirror_move.dstHost = mirror_host

            primary_move.target_port = self.resources[primary_host].can_accommodate_port(
                True, primary_id.contentid)
            if not primary_move.target_port:
                raise Exception("Cannot accomodate port")
            mirror_move.target_port = self.resources[mirror_host].can_accommodate_port(
                False, primary_id.contentid)
            if not mirror_move.target_port:
                raise Exception("Cannot accomodate port")

    def _prepare_pms(self, primary_mirrors:  List[Tuple[Move, Move]]):
        """
        Choose the target directory for primary-mirror move case:
        1. mirror is moved to primary dir in primary's target host
        2. role switching takes place
        2. primary is moved to mirror dir in mirror's target host
        """
        for primary_move, mirror_move in primary_mirrors:
            primary_host = primary_move.dstHost
            mirror_host = mirror_move.dstHost

            primary_id = primary_move.segid
            mirror_id = mirror_move.segid

            # define datadir
            for datadir in mirror_host.mirror_datadirs:
                try:
                    self.resources[primary_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    primary_move.target_datadir = datadir
                    break
                except Exception as e:
                    self.logger.error(str(e))
                    continue
            primary_move.dstHost = mirror_host
            if primary_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {primary_host.hostname} does not have any valid primary "
                                             f"datadirs for segment {mirror_move.segid}")
            for datadir in primary_host.primary_datadirs:
                try:
                    self.resources[mirror_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    mirror_move.target_datadir = datadir
                    break
                except:
                    continue
            if mirror_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {mirror_host.hostname} does not have any valid primary "
                                             f"datadirs or ports for segment {mirror_move.segid}")
            mirror_move.dstHost = primary_host

            primary_move.target_port = self.resources[mirror_host].can_accommodate_port(
                True, primary_id.contentid)
            if not primary_move.target_port:
                raise Exception("Cannot accomodate port")
            mirror_move.target_port = self.resources[primary_host].can_accommodate_port(
                False, primary_id.contentid)
            if not mirror_move.target_port:
                raise Exception("Cannot accomodate port")

    def _prepare_ps(self, primaries: List[Move]):
        """
        Choose the target directory for primary-only move case:
        1. role switch takes place
        2. primary is moved to target dir
        3. primary is moved to mirror dir in mirror's target host
        """
        for primary_move in primaries:
            primary_host = primary_move.dstHost

            primary_id = primary_move.segid
            # define datadir
            for datadir in primary_host.primary_datadirs:
                try:
                    self.resources[primary_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    primary_move.target_datadir = datadir
                    break
                except:
                    continue
            if primary_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {primary_host.hostname} does not have any valid primary "
                                             f"datadirs for segment {primary_move.segid}")

            primary_move.target_port = self.resources[primary_host].can_accommodate_port(
                True, primary_id.contentid)
            if not primary_move.target_port:
                raise Exception("Cannot accomodate port")

    def _prepare_ms(self, mirrors: List[Move]):
        """
        Choose the target directory for mirror-only move case:
        1. mirror is moved to mirror dir in mirror's target host
        """
        for mirror_move in mirrors:
            mirror_host = mirror_move.dstHost

            mirror_id = mirror_move.segid
            # define datadir
            for datadir in mirror_host.mirror_datadirs:
                try:
                    self.resources[mirror_host].accommodate_segment(
                        self.segmentSizes[mirror_id], datadir)
                    mirror_move.target_datadir = datadir
                    break
                except:
                    continue
            if mirror_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {mirror_host.hostname} does not have any valid primary "
                                             f"datadirs for segment {mirror_move.segid}")

            mirror_move.target_port = self.resources[mirror_host].can_accommodate_port(
                True, mirror_id.contentid)
            if not mirror_move.target_port:
                raise Exception("Cannot accomodate port")

    def _classify_moves(self) -> Tuple[List[Tuple[Move, Move]], List[Tuple[Move, Move]], List[Move], List[Move]]:
        """
        Classify moves into:
        - pure_swaps: pairs of moves where primary and mirror just switch places
        - primary_moves_with_mirrors: pairs of moves where we move both primary and mirror
        - primary_moves: independent primary moves
        - mirror_moves: independent mirror moves
        """
        # Group moves by contentid
        moves_by_content = defaultdict(list)
        for move in self.moves:
            moves_by_content[move.segid.contentid].append(move)

        primary_moves_with_mirrors = []
        primary_moves = []
        mirror_moves = []
        pure_swaps = []

        for contentid, moves in moves_by_content.items():
            if len(moves) == 2:
                primary_move = next(
                    (m for m in moves if not m.is_mirror), None)
                mirror_move = next((m for m in moves if m.is_mirror), None)

                if (primary_move and mirror_move and
                    primary_move.srcHost == mirror_move.dstHost and
                        primary_move.dstHost == mirror_move.srcHost):
                    pure_swaps.append((primary_move, mirror_move))
                elif primary_move and mirror_move:
                    # This is a primary move with corresponding mirror move
                    primary_moves_with_mirrors.append(
                        (primary_move, mirror_move))

            elif len(moves) == 1:
                move = moves[0]
                if move.is_mirror:
                    mirror_moves.append(move)
                else:
                    primary_moves.append(move)

        return pure_swaps, primary_moves_with_mirrors, primary_moves, mirror_moves

    def _create_move_sequence(self) -> Tuple[List[List[Move]], Set[Move], Set[Move]]:
        """Create full sequence of moves"""
        sequences = []
        current_batch = []

        pure_swaps, primary_mirrors, primaries, mirrors = self._classify_moves()
        self._prepare_swaps(pure_swaps)
        self._prepare_pms(primary_mirrors)
        self._prepare_ps(primaries)
        self._prepare_ms(mirrors)

        former_switches = set()
        latter_switches = set()

        current_batch = []
        for mirror_move in mirrors:
            if len(current_batch) >= self.options.batch_size:
                sequences.append(current_batch)
                current_batch = []
            current_batch.append(mirror_move)

        # Phase 2: First part moves (before first switch)
        current_batch = []

        # Pure swaps: move mirrors to primary dirs
        for primary_move, mirror_move in pure_swaps:
            if len(current_batch) >= self.options.batch_size:
                sequences.append(current_batch)
                current_batch = []
            current_batch.append(mirror_move)

        # Primary-mirror pairs: move mirrors to primary's target
        for primary_move, mirror_move in primary_mirrors:
            if len(current_batch) >= self.options.batch_size:
                sequences.append(current_batch)
                current_batch = []
            current_batch.append(mirror_move)

        if current_batch:
            sequences.append(current_batch)

        # First switch point - affects:
        # - Pure swaps
        # - Primary-mirror pairs
        # - First switch for primary-only moves
        segments_for_switch1 = []
        segments_for_switch1.extend([pm[0].segid for pm in pure_swaps])
        segments_for_switch1.extend([pm[0].segid for pm in primary_mirrors])
        # First switch for primaries
        segments_for_switch1.extend([pm.segid for pm in primaries])

        if segments_for_switch1:
            sequences.append(['SWITCH', segments_for_switch1])
            for seg in segments_for_switch1:
                former_switches.add(seg.contentid)
                # Phase 3: Post-first-switch moves
        current_batch = []

        # Pure swaps: move ex-primaries to mirror dirs
        for primary_move, _ in pure_swaps:
            if len(current_batch) >= self.options.batch_size:
                sequences.append(current_batch)
                current_batch = []
            current_batch.append(primary_move)

        # Primary-mirror pairs: move ex-primaries to target mirror dirs
        for primary_move, _ in primary_mirrors:
            if len(current_batch) >= self.options.batch_size:
                sequences.append(current_batch)
                current_batch = []
            current_batch.append(primary_move)

        # Primary-only: move ex-primaries to target primary dirs
        for primary_move in primaries:
            if len(current_batch) >= self.options.batch_size:
                sequences.append(current_batch)
                current_batch = []
            current_batch.append(primary_move)

        if current_batch:
            sequences.append(current_batch)

        # Second switch point - affects:
        # - Second switch for primary-only moves
        if primaries:
            sequences.append(['SWITCH', [pm.segid for pm in primaries]])
            for seg in primaries:
                latter_switches.add(seg.segid.contentid)

        return sequences, former_switches, latter_switches

    def execute_moves(self, firstRun=True):
        """Main execution method"""
        try:
            if not firstRun:
                raise NotImplementedError(
                    "rebalance rerun is not implemented properly yet")

            move_sequences, former_switches, latter_switches = self._create_move_sequence()

            global begining_timestamp
            begining_timestamp = datetime.datetime.now()

            conf_dir = self.options.coordinator_data_directory + CONF_DIR

            if firstRun:
                self.statusManager.set_status('EXECUTION_PREPARED')
                self.statusManager.set_db_status(
                    RebalanceStatus.PREPARED, begining_timestamp)
                self.plan.save_to_file(conf_dir, "plan")

            self.statusManager.set_db_status(
                RebalanceStatus.IN_PROGRESS)

            self.queue = WorkerPool(self.options.parallel)

            stopTime = None
            stoppedEarly = False
            had_error = False
            if self.options.end:
                stopTime = self.options.end
            for sequence in move_sequences:
                if isinstance(sequence[0], str) and sequence[0] == 'SWITCH':

                    while not self.queue.isDone():
                        logger.debug(
                            "woke up.  queue: %d finished %d  " % (self.queue.assigned, self.queue.completed_queue.qsize()))
                        if stopTime and datetime.datetime.now() >= stopTime:
                            stoppedEarly = True
                            break
                        time.sleep(5)

                    for moveCommand in self.queue.getCompletedItems():
                        if moveCommand.move_error:
                            had_error = True
                        break

                    if stoppedEarly or had_error:
                        break

                    self.statusManager.set_db_status(
                        RebalanceStatus.AWAITS_SWITCH)
                    self.logger.info(
                        f"Executing role swaps for {len(sequence[1])} segments")
                    self._execute_role_swaps(sequence[1])

                    self.statusManager.set_db_status(
                        RebalanceStatus.IN_PROGRESS)
                else:
                    for move in sequence:
                        segid = move.segid
                        needs_switch = False
                        if segid.contentid in former_switches or segid.contentid in latter_switches:
                            needs_switch = True
                        segsize = self.segmentSizes[segid].source_data_dir_usage
                        if self.segmentSizes[segid].source_tablespace_usage:
                            segsize += sum(
                                self.segmentSizes[segid].source_tablespace_usage.values())
                        step_details = (self.segmentMap[segid],
                                        move,
                                        segsize,
                                        conf_dir,
                                        needs_switch
                                        )

                        cmd = SingleMoveCommand(
                            "name", self.dburl, step_details, self.logger)
                        self.queue.addCommand(cmd)

                while not self.queue.isDone():
                    logger.debug(
                        "woke up.  queue: %d finished %d  " % (self.queue.assigned, self.queue.completed_queue.qsize()))
                    if stopTime and datetime.datetime.now() >= stopTime:
                        stoppedEarly = True
                        break
                    time.sleep(5)
                if stoppedEarly:
                    self.logger.info("Execution timeout is reached. Waiting the existing jobs to finish "
                                     "and stopping rebalance.")
                    break

            self.queue.haltWork()
            self.queue.joinWorkers()

            for moveCommand in self.queue.getCompletedItems():
                if moveCommand.move_error:
                    had_error = True
                    break
            if had_error or stoppedEarly:
                self.statusManager.set_db_status(
                    RebalanceStatus.STOPPED)
            else:
                self.statusManager.set_db_status(
                    RebalanceStatus.COMPLETED)

            if stoppedEarly:
                self.logger.info("Rebalance stopped due to timeout")

        except Exception as e:
            raise

    def _execute_role_swaps(self, segids: List[SegmentId]):
        """Execute multiple role swaps in single gprecoverseg -r call"""
        if not segids:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("SET allow_system_table_mods=1;")
                data = tuple([segid.contentid for segid in segids])
                cur.execute("UPDATE gp_segment_configuration SET preferred_role = 't' WHERE "
                            "content IN %s AND preferred_role = 'm'", (data,))
                cur.execute("UPDATE gp_segment_configuration SET preferred_role = 'm' WHERE "
                            "content IN %s AND preferred_role = 'p'", (data,))
                cur.execute("UPDATE gp_segment_configuration SET preferred_role = 'p' WHERE "
                            "content IN %s AND preferred_role = 't'", (data,))
                cur.execute("UPDATE gprebalance.status_detail SET status = 'PENDING' WHERE "
                            "status = 'awaits_switch'")
                cur.execute("COMMIT")
        except Exception as e:
            raise Exception('could not execute SQL : %s' % str(e))

        recoversegOptions = "-r -a"
        cmd = GpRecoverSeg("Running gprecverseg", options=recoversegOptions)
        cmd.run(validateAfter=True)

    def shutdown(self):
        if self.queue:
            self.queue.haltWork()
            self.queue.joinWorkers()
            self.queue = None
