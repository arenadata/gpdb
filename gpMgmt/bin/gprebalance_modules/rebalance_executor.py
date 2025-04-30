import base64
import fcntl
import multiprocessing
import time
from collections import defaultdict
import pickle
from typing import List, Dict, Optional, Set, Tuple
from gprebalance_modules.rebalance_plan import Move, Plan  # nopep8
from gprebalance_modules.rebalance_status import StatusManager, RebalanceStatus, MoveStatus
from gprebalance_modules.rebalance import ClusterState, SegmentId, SegmentSize, Host
from gppylib.gparray import GpArray, Segment
from gppylib.db import dbconn
from gppylib.commands.base import *
from gppylib.commands.gp import *
from gppylib.commands.unix import DiskFree, DiskUsage, RemoveDirectory, getLocalHostname, getUserName
from gppylib.operations.validate_disk_space import FileSystem
from gppylib.parseutils import *
from gppylib.programs.clsRecoverSegment import GpRecoverSegmentProgram
from gppylib.system import configurationInterface, configurationImplGpdb, fileSystemInterface, \
    fileSystemImplOs, osInterface, osImplNative, faultProberInterface, faultProberImplGpdb
from gppylib.userinput import *

MAX_BATCH_SIZE = 128
FILENAME = "/move_"
CONF_DIR = "/rebalance"
DEFAULT_PRIMARY_PREF = "/data/primary"
DEFAULT_MIRROR_PREF = "/data/mirror"
GPRECOVERSEG_DIR = 'gpAdminLogs/rebalance'

begining_timestamp = None
segment_prefix = "gpseg"

class InsufficientDiskSpaceError(Exception):
    pass


class NoValidDataDirectories(Exception):
    pass


class RecoveryProcess:
    @staticmethod
    def run_recovery(cmd_args: list, result_queue: multiprocessing.Queue, log_file: str):
        try:
            #prevent signal propagation from parent
            os.setpgrp()
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            
            log_fd = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
            flags = fcntl.fcntl(log_fd, fcntl.F_GETFL)
            fcntl.fcntl(log_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            os.dup2(log_fd, 1)  # stdout
            os.dup2(log_fd, 2)  # stderr
            os.close(log_fd)

            import gppylib.gplog as gplog
            if gplog._LOGGER:
                for handler in gplog._LOGGER.handlers[:]:
                    handler.close()
                    gplog._LOGGER.removeHandler(handler)
            gplog._LOGGER = None

            gplog._FILENAME = None
            gplog._DEFAULT_FORMATTER = None
            gplog._LITERAL_FORMATTER = None
            gplog._SOUT_HANDLER = None
            gplog._FILE_HANDLER = None

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
            
            gplog.setup_tool_logging("gprecoverseg", getLocalHostname(),
                                          getUserName(),
                                          logdir=local_options.logfileDirectory)

            # Create and run the program
            cmd = GpRecoverSegmentProgram.createProgram(local_options, args)
            cmd.run()

        except SystemExit as e:
            error_msg = None
            if e.code != 0:
                error_msg = f"Gprecoverseg failed with exit code: {e.code}. See the log in {log_file}"
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
            cmd.cleanup()


class SingleMoveCommand(SQLCommand):
    def __init__(self, name: str, step_details, logger, statusManager: StatusManager):
        self.status_manager = statusManager
        self.logger = logger
        (self.segment, self.move, self.segmentSize,
         self.conf_dir, self.needs_switch, self.options) = step_details

        self.move_error = False

        SQLCommand.__init__(self, name)

    def write_gprecoverseg_config(self):
        filename = self.conf_dir + FILENAME + "dbid" + str(self.segment.dbid)
        with open(filename, 'w') as fp:
            line = (f"{canonicalize_address(self.segment.address)}|"
                    f"{self.segment.port}|{self.segment.datadir} "
                    f"{canonicalize_address(self.move.dstHost.address)}|"
                    f"{self.move.target_port}|"
                    f"{self.move.target_datadir}")
            self.logger.info(
                "About to run gprecoverseg for mirror move "
                f"(dbid = {self.segment.dbid}, content = {self.segment.content}) {line}")
            fp.write(line)
        return filename

    def run(self, validateAfter=False):
        status_conn = None
        try:
            
            segsize = self.segmentSize.source_data_dir_usage
            if self.segmentSize.source_tablespace_usage:
                segsize += sum(self.segmentSize.source_tablespace_usage.values())
            self.status_manager.update_move_status([self.segment.dbid], MoveStatus.IN_PROGRESS)

            filename = self.write_gprecoverseg_config()

            strtime=datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

            # in order to run gprecoverseg processes separately and avoid any races
            # for resources gprecoverseg generates, we create separate log directories.
            log_dir = f"{os.path.join(os.environ.get('HOME', '.'),GPRECOVERSEG_DIR)}/gprecoverseg_dbid{self.segment.dbid}_{strtime}"
            mkdirCmd = MakeDirectory("rebalance log dir", log_dir)
            mkdirCmd.run(validateAfter=True)
            
            mkdirCmd = MakeDirectory("rebalance log dir", log_dir, ctxt=REMOTE, remoteHost=self.move.srcHost.hostname)
            mkdirCmd.run(validateAfter=True)
            mkdirCmd = MakeDirectory("rebalance log dir", log_dir, ctxt=REMOTE, remoteHost=self.move.dstHost.hostname)
            mkdirCmd.run(validateAfter=True)

            log_file = f"{self.conf_dir}/gprecoverseg_dbid{self.segment.dbid}_{strtime}"
            # Prepare command arguments
            cmd_args = [
                '-i', filename,
                '-B', '1',
                '-v', '-a',
                '-l', log_dir
            ]
            if self.options.hba_hostnames:
                cmd_args.append('--hba-hostnames')

            result_queue = multiprocessing.Queue()
            recovery_process = multiprocessing.Process(
                target=RecoveryProcess.run_recovery,
                args=(cmd_args, result_queue, log_file)
            )
            recovery_process.start()
            result = None
            while recovery_process.is_alive():
            # Check if result is available without blocking
                try:
                    if not result_queue.empty():
                        result = result_queue.get(block=False)
                        recovery_process.join()
                except Exception:
                    pass
                time.sleep(10)
            
            if not result_queue.empty():
                result = result_queue.get(block=False)
           
            if result is None:
                exit_code = recovery_process.exitcode
                if exit_code < 0:
                    self.logger.error(
                    f"Could not perform mirror dbid={self.segment.dbid} "
                    f"move with content {self.segment.content} due to "
                    f"recoverseg error: Process terminated by signal {-exit_code}\n"
                    f"Check the gprecoverseg log file {log_file}, fix any problems, and re-run"
                )
                else:
                    self.logger.error(
                    f"Could not perform mirror dbid={self.segment.dbid} "
                    f"move with content {self.segment.content} due to "
                    f"recoverseg error: Process exited with code {exit_code}\n"
                    f"Check the gprecoverseg log file {log_file}, fix any problems, and re-run")
                self.status_manager.update_move_status([self.segment.dbid], MoveStatus.FAILED)
                self.move_error = True
                return
            
            if result["status"] == "FAILED":
                self.logger.error(
                    f"Could not perform mirror dbid={self.segment.dbid} "
                    f"move with content {self.segment.content} due to "
                    f"recoverseg error: {result['error']}\n"
                    f"Check the gprecoverseg log file {log_file}, fix any problems, and re-run"
                )
                self.status_manager.update_move_status([self.segment.dbid], MoveStatus.FAILED)

                self.move_error = True
                return
            if self.needs_switch:
                self.status_manager.update_move_status([self.segment.dbid], MoveStatus.AWAITS_SWITCH)
            else:
                self.status_manager.update_move_status([self.segment.dbid], MoveStatus.COMPLETED)
           

            self.logger.info("Removing old segment's datadir (dbidi = %d): %s",
                             self.segment.dbid, self.segment.datadir)
            cmd = RemoveDirectory("remove old mirror segment directories", self.segment.datadir,
                                  ctxt=REMOTE, remoteHost=self.segment.address)
            cmd.run(validateAfter=True)
            if self.segmentSize.source_tablespace_usage:
                for tblspdir in self.segmentSize.source_tablespace_usage:
                    cmd = RemoveDirectory("remove old mirror segment directories", tblspdir,
                                          ctxt=REMOTE, remoteHost=self.segment.address)
                    self.logger.info("Removing old segment's tablespace datadir (dbidi = %d): %s",
                                     self.segment.dbid, tblspdir)
                    cmd.run(validateAfter=True)

        except Exception as ex:
            self.logger.error(ex.__str__().strip())
            self.move_error = True
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
        used_ports = self.used_primary_ports | self.used_mirror_ports

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
            current_port += 2

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
        segids = []
        for m in plan.moves:
            segids.append(m.segid)
        self.segmentSizes = self.estimateSegmentSizes(segids)
        self.resources = self.initializeHostResources(plan.moves) if not options.rollback else None
        self.queue = None
        self.shutdown_requested = False

        self.define_datadir_prefix()
    
    def define_datadir_prefix(self):
        first_source_dir = None
        for _, segment in self.segmentMap.items():
            if segment.content >= 0:
                first_source_dir = segment.datadir
                break
        
        basename = os.path.basename(first_source_dir)
        global segment_prefix
        segment_prefix = ''.join(c for c in basename if not c.isdigit())

    def initializeHostResources(self, moves: List[Move]):
        def datadir_validator(input_value, default,  *args):
            if not input_value and not default:
                return None
            elif not input_value or input_value == '':
                input_value = default
            if not input_value or input_value.find(' ') != -1 or input_value == '':
                return None
            else:
                return input_value
        resources = {}
        for m in moves:
            prim_ports = set()
            mir_ports = set()
            if m.dstHost not in resources:
                for psid in self.cluster_state[(m.dstHost.hostname, m.dstHost.address)].primary_segments:
                    prim_ports.add(self.segmentMap[psid].port)
                for msid in self.cluster_state[(m.dstHost.hostname, m.dstHost.address)].mirror_segments:
                    mir_ports.add(self.segmentMap[msid].port)
                if len(m.dstHost.primary_datadirs) == 0:
                    prirmary_prefix = DEFAULT_PRIMARY_PREF
                    if not self.options.silent:
                        prirmary_prefix = ask_input(f"\nThe segment (dbid={m.segid.dbid}, content={m.segid.contentid}) "
                                                     f"is about to be moved to host {m.dstHost.hostname}, but no primary datadirs "
                                                     "are specified for the host.", "Enter the primary datadir prefix",f" (default={DEFAULT_PRIMARY_PREF})",
                                                     DEFAULT_PRIMARY_PREF, datadir_validator, None)
                    m.dstHost.primary_datadirs.add(prirmary_prefix.strip())
                if  self.gparr.hasMirrors and len(m.dstHost.mirror_datadirs) == 0:
                    mirror_prefix = DEFAULT_MIRROR_PREF
                    if not self.options.silent:
                        mirror_prefix = ask_input(f"\nThe segment (dbid={m.segid.dbid}, content={m.segid.contentid}) "
                                                     f"is about to be moved to host {m.dstHost.hostname}, but no mirror datadirs "
                                                     "are specified for the host.", "Enter the mirror datadir prefix",f" (default={DEFAULT_MIRROR_PREF})",
                                                     DEFAULT_MIRROR_PREF, datadir_validator, None)
                    m.dstHost.mirror_datadirs.add(mirror_prefix.strip())

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
        for segid, tblspace_dirs in tablespaces.items():
            sourceSeg = self.segmentMap[segid]
            source_tblsps_usage = self._disk_usage(
                sourceSeg.address, tblspace_dirs)
            segmentSizes[segid].source_tablespace_usage = source_tblsps_usage

        return segmentSizes

    def _prepare_swaps(self, swaps: List[Tuple[Move, Move]]):
        """
        Choose the target directory for swap case:
        1. primary is moved to mirror dir in its own host
        2. mirror is moved to primary dir in its own host
        3. role switching takes place
        """
        for primary_move, mirror_move in swaps:
            #important notice. srcHost and dstHost are different
            # objects even if they are describing the same host. 
            # This happens due to code in get_moves_between_states(state1, state2)
            # srcHost and dstHost are taken from state1 and state2 correspondingly
            primary_host = mirror_move.dstHost
            mirror_host = primary_move.dstHost

            primary_id = primary_move.segid

            if self.options.rollback:
                mirror_move.dstHost = mirror_host
                primary_move.dstHost = primary_host
                primary_move.target_datadir, mirror_move.target_datadir =  mirror_move.target_datadir, primary_move.target_datadir
                primary_move.target_port, mirror_move.target_port =  mirror_move.target_port, primary_move.target_port
                continue
            # define datadir
            for datadir in primary_host.mirror_datadirs:
                try:
                    self.resources[primary_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    primary_move.target_datadir = datadir + f"/{segment_prefix}{primary_move.segid.contentid}"
                    break
                except:
                    continue
            if primary_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {primary_host.hostname} does not have any valid mirror "
                                             f"datadirs for segment {primary_move.segid}. None of the "
                                             f"{primary_host.mirror_datadirs} either exists or has "
                                             "enough free space for segment movement")
            primary_move.dstHost = primary_host
            for datadir in mirror_host.primary_datadirs:
                try:
                    self.resources[mirror_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    mirror_move.target_datadir = datadir + f"/{segment_prefix}{mirror_move.segid.contentid}"
                    break
                except:
                    continue
            if mirror_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {mirror_host.hostname} does not have any valid primary "
                                             f"datadirs for segment {mirror_move.segid}. None of the "
                                             f"{mirror_host.primary_datadirs} either exists or has "
                                             "enough free space for segment movement")
            mirror_move.dstHost = mirror_host

            primary_move.target_port = self.resources[primary_host].can_accommodate_port(
                True, primary_id.contentid)
            if not primary_move.target_port:
                raise Exception("Cannot accomodate port")
            self.resources[primary_host].reserve_port(primary_move.target_port, True)
            mirror_move.target_port = self.resources[mirror_host].can_accommodate_port(
                False, primary_id.contentid)
            if not mirror_move.target_port:
                raise Exception("Cannot accomodate port")
            self.resources[mirror_host].reserve_port(mirror_move.target_port,False)

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

            if self.options.rollback:
                primary_move.dstHost = mirror_host
                mirror_move.dstHost = primary_host
                primary_move.target_datadir, mirror_move.target_datadir =  mirror_move.target_datadir, primary_move.target_datadir
                primary_move.target_port, mirror_move.target_port =  mirror_move.target_port, primary_move.target_port
                continue
            # define datadir
            for datadir in mirror_host.mirror_datadirs:
                try:
                    self.resources[primary_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    primary_move.target_datadir = datadir + f"/{segment_prefix}{primary_move.segid.contentid}"
                    break
                except Exception as e:
                    self.logger.error(str(e))
                    continue
            primary_move.dstHost = mirror_host
            if primary_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {mirror_host.hostname} does not have any valid mirror "
                                             f"datadirs for segment {primary_move.segid}. None of the "
                                             f"{mirror_host.mirror_datadirs} either exists or has "
                                             "enough free space for segment movement")
            for datadir in primary_host.primary_datadirs:
                try:
                    self.resources[mirror_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    mirror_move.target_datadir = datadir + f"/{segment_prefix}{mirror_move.segid.contentid}"
                    break
                except:
                    continue
            if mirror_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {primary_host.hostname} does not have any valid primary "
                                             f"datadirs for segment {mirror_move.segid}. None of the "
                                             f"{primary_host.primary_datadirs} either exists or has "
                                             "enough free space for segment movement")
            mirror_move.dstHost = primary_host

            primary_move.target_port = self.resources[mirror_host].can_accommodate_port(
                True, primary_id.contentid)
            if not primary_move.target_port:
                raise Exception("Cannot accomodate port")
            self.resources[mirror_host].reserve_port(primary_move.target_port,True)
            mirror_move.target_port = self.resources[primary_host].can_accommodate_port(
                False, primary_id.contentid)
            if not mirror_move.target_port:
                raise Exception("Cannot accomodate port")
            self.resources[primary_host].reserve_port(mirror_move.target_port,False)

    def _prepare_ps(self, primaries: List[Move]):
        """
        Choose the target directory for primary-only move case:
        1. role switch takes place
        2. primary is moved to target dir
        3. role switch
        """
        if self.options.rollback:
            return

        for primary_move in primaries:
            primary_host = primary_move.dstHost

            primary_id = primary_move.segid
            # define datadir
            for datadir in primary_host.primary_datadirs:
                try:
                    self.resources[primary_host].accommodate_segment(
                        self.segmentSizes[primary_id], datadir)
                    primary_move.target_datadir = datadir + f"/{segment_prefix}{primary_move.segid.contentid}"
                    break
                except:
                    continue
            if primary_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {primary_host.hostname} does not have any valid primary "
                                             f"datadirs for segment {primary_move.segid}. None of the "
                                             f"{primary_host.primary_datadirs} either exists or has "
                                             "enough free space for segment movement")

            primary_move.target_port = self.resources[primary_host].can_accommodate_port(
                True, primary_id.contentid)
            if not primary_move.target_port:
                raise Exception("Cannot accomodate port")
            self.resources[primary_host].reserve_port(primary_move.target_port ,True)

    def _prepare_ms(self, mirrors: List[Move]):
        """
        Choose the target directory for mirror-only move case:
        1. mirror is moved to mirror dir in mirror's target host
        """
        if self.options.rollback:
            return 
        for mirror_move in mirrors:
            mirror_host = mirror_move.dstHost

            mirror_id = mirror_move.segid
            # define datadir
            for datadir in mirror_host.mirror_datadirs:
                try:
                    self.resources[mirror_host].accommodate_segment(
                        self.segmentSizes[mirror_id], datadir)
                    mirror_move.target_datadir = datadir + f"/{segment_prefix}{mirror_move.segid.contentid}"
                    break
                except:
                    continue
            if mirror_move.target_datadir == None:
                raise NoValidDataDirectories(f"Host {mirror_host.hostname} does not have any valid mirror "
                                             f"datadirs for segment {mirror_move.segid}. None of the "
                                             f"{mirror_host.mirror_datadirs} either exists or has "
                                             "enough free space for segment movement")

            mirror_move.target_port = self.resources[mirror_host].can_accommodate_port(
                True, mirror_id.contentid)
            if not mirror_move.target_port:
                raise Exception("Cannot accomodate port")
            self.resources[mirror_host].reserve_port(mirror_move.target_port ,True)


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

            #record moves in status file
            detail_list = []
            for seq_no, seq in enumerate(move_sequences):
                if not isinstance(seq[0], str):
                    for move in seq:
                        needs_switch = False
                        if move.segid.contentid in former_switches or move.segid.contentid in latter_switches:
                            needs_switch = True
                        if move.segid.contentid in former_switches and move.segid.contentid in latter_switches:
                            needs_switch = False
                        detail_list.append((seq_no, self.segmentMap[move.segid],
                                move,
                                self.segmentSizes[move.segid].source_data_dir_usage,
                                needs_switch))
            self.statusManager.record_moves_batch(detail_list)

            hosts = set()
            for move in self.moves:
                hosts.add(move.srcHost.hostname)
                hosts.add(move.dstHost.hostname)
            
            if firstRun:
                # in order to run gprecoverseg processes separately and avoid any races
                # for resources gprecoverseg generates (progress file), we create
                # separate log directories.
                mkdirCmd = MakeDirectory("rebalance log dir", GPRECOVERSEG_DIR)
                mkdirCmd.run(validateAfter=True)
                for host in hosts:
                    mkdirCmd = MakeDirectory("rebalance log dir", GPRECOVERSEG_DIR, ctxt=REMOTE, remoteHost=host)
                    mkdirCmd.run(validateAfter=True)
                
                if self.options.rollback:
                    self.statusManager.set_status(RebalanceStatus.ROLLBACK_PREPARED, conf_dir)
                    self.plan.save_to_file(conf_dir, "rollback_plan")
                else:
                    self.statusManager.set_status(RebalanceStatus.PREPARED, conf_dir)
                    self.plan.save_to_file(conf_dir, "plan")

            self.queue = WorkerPool(self.options.parallel)

            stopTime = None
            stoppedEarly = False
            had_error = False
            if self.options.end:
                stopTime = self.options.end
            if self.options.rollback:
                self.statusManager.set_status(RebalanceStatus.ROLLBACK_IN_PROGRESS)
            else:
                self.statusManager.set_status(RebalanceStatus.IN_PROGRESS)
            for sequence in move_sequences:
                if self.shutdown_requested:
                    break
                if isinstance(sequence[0], str) and sequence[0] == 'SWITCH':

                    while not self.queue.isDone():
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

                    if self.shutdown_requested:
                        break

                    self.statusManager.set_status(
                        RebalanceStatus.AWAITS_SWITCH)
                    self.logger.info(
                        f"Executing role swaps for {len(sequence[1])} segments")
                    try:
                        self._execute_role_swaps(sequence[1])
                        for segid in sequence[1]:
                            if segid in former_switches and segid not in latter_switches:
                                self.statusManager.update_move_status(sequence[1], MoveStatus.COMPLETED)
                                break
                    except Exception as e:
                        had_error = True
                        self.logger.error(f"Could not execute role swaps:{str(e)}")
                        break
                    self.statusManager.set_status(
                        RebalanceStatus.IN_PROGRESS)
                else:
                    for move in sequence:
                        if self.shutdown_requested:
                            break
                        segid = move.segid
                        needs_switch = False
                        if segid.contentid in former_switches or segid.contentid in latter_switches:
                            needs_switch = True
                        step_details = (self.segmentMap[segid],
                                        move,
                                        self.segmentSizes[segid],
                                        conf_dir,
                                        needs_switch,
                                        self.options
                                        )

                        cmd = SingleMoveCommand(
                            "name", step_details, self.logger, self.statusManager)
                        self.queue.addCommand(cmd)

                while not self.queue.isDone():
                    if stopTime and datetime.datetime.now() >= stopTime:
                        stoppedEarly = True
                        break
                    time.sleep(5)
                if stoppedEarly:
                    self.logger.info("Execution timeout is reached. Waiting the existing jobs to finish "
                                     "and stopping rebalance.")
                    break
                for moveCommand in self.queue.getCompletedItems():
                    if moveCommand.move_error:
                        had_error = True
                        break
            
            if self.queue:
                self.queue.haltWork()
                self.queue.joinWorkers()
                self.queue = None
            
            if stoppedEarly or self.shutdown_requested:
                self.statusManager.set_status(RebalanceStatus.STOPPED)
                if not self.shutdown_requested:
                    self.logger.info("Rebalance stopped due to timeout")
            elif had_error:
                if self.options.rollback:
                    self.statusManager.set_status(
                    RebalanceStatus.ROLLBACK_FAILED)
                else:
                    self.statusManager.set_status(
                    RebalanceStatus.FAILED)
                raise Exception("execution encountered movement erorrs")
            else:
                if self.options.rollback:
                    self.statusManager.set_status(RebalanceStatus.ROLLBACK_DONE)
                else:
                    self.statusManager.set_status(RebalanceStatus.DONE)

                rmdirCmd = RemoveDirectory("remove recoverseg log dir",  f"{os.path.join(os.environ.get('HOME', '.'),GPRECOVERSEG_DIR)}")
                rmdirCmd.run()
                for host in hosts:
                    rmdirCmd = RemoveDirectory("remove recoverseg log dir",  f"{os.path.join(os.environ.get('HOME', '.'),GPRECOVERSEG_DIR)}", ctxt=REMOTE, remoteHost=host)
                    rmdirCmd.run()
                

        except Exception as e:
            if self.options.rollback:
                self.statusManager.set_status(RebalanceStatus.ROLLBACK_FAILED)
            else:
                self.statusManager.set_status(RebalanceStatus.FAILED)
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
                cur.execute("COMMIT")
        except Exception as e:
            raise Exception('could not execute SQL : %s' % str(e))

        recoversegOptions = f"-r -a -l {os.path.join(os.environ.get('HOME', '.'),GPRECOVERSEG_DIR)}"
        if self.options.hba_hostnames:
            recoversegOptions += " --hba-hostnames"
        cmd = GpRecoverSeg("Running gprecverseg", options=recoversegOptions)
        cmd.run(validateAfter=True)


    def shutdown(self):
        # the execution shutdown assumes finishing
        # current jobs in queue
        if self.queue:
            self.logger.info("Shutdown requested, will complete current jobs...")
            self.shutdown_requested = True
            self.queue.haltWork()
            self.queue.joinWorkers()
            self.queue = None
