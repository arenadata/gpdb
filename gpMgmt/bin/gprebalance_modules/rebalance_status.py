import atexit
import copy
from datetime import datetime
import json
import os
from enum import Enum
import pickle
import threading
import time
from typing import Dict, List, Tuple, Any, Optional

from gprebalance_modules.rebalance_plan import Move
from gprebalance_modules.rebalance import SegmentId, Segment
import gppylib.operations.update_pg_hba_on_segments as hba_upd
from gppylib.operations.detect_unreachable_hosts import get_unreachable_segment_hosts, update_unreachable_flag_for_segments

from gppylib.commands import pg
from gppylib.commands.base import Command, REMOTE
from gppylib.userinput import *
from gppylib.db import dbconn
from gppylib.system import configurationInterface as configInterface
from gppylib.parseutils import *

class MoveStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    AWAITS_SWITCH = "AWAITS_SWITCH"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"
    REVERTED = "REVERTED"
    ASK_ROLLBACK_OR_SKIP = "ASK_USER"
    SKIPPED = "NEEDS_MANUAL_RECOVERY"

class RebalanceStatus(Enum):
    UNKNOWN = "UNKNOWN"
    UNINITIALIZED = "UNINITIALIZED"
    VALIDATED = "VALIDATED"
    PRE_EXECUTE_FAILED= "PRE_EXECUTE_NOT_PASSED"
    PREPARED = "EXECUTION_PREPARED"
    IN_PROGRESS = "EXECUTION_IN_PROGRESS"
    AWAITS_SWITCH_APPROVE = "EXECUTION_AWAITING_SWITCHOVER_APPROVE"
    AWAITS_SWITCH = "EXECUTION_AWAITING_SWITCHOVER"
    STEPS_FAILED = "EXECUTION_STEPS_FAILED"
    FAILED = "EXECUTION_FAILED"
    DONE = "EXECUTION_DONE"
    AWAITS_ROLLBACK_APPROVE = "EXECUTION_AWAITING_ROLLBACK_APPROVE"
    STOPPED = "EXECUTION_STOPPED"
    ROLLBACK_PREPARED = "EXECUTION_ROLLBACK_PREPARED"
    ROLLBACK_IN_PROGRESS = "EXECUTION_ROLLBACK_PREPARED"
    ROLLBACK_DONE = "EXECUTION_ROLLBACK_DONE"
    ROLLBACK_FAILED = "EXECUTION_ROLLBACK_FAILED"

class InvalidStatusError(Exception):
    pass

def load_plan(datadir, rollback):
    filename = datadir 
    if rollback:
        filename += "/rollback_plan.pkl"
    else:
        filename += "/plan.pkl"
    if not os.path.exists(filename):
        raise FileNotFoundError(f"No pickle file found at {filename}")
    with open(filename, 'rb') as f:
        plan = pickle.load(f)
    return plan

wait_timeout_s = 5

class StatusManager:
    # Class variable for the singleton instance
    _instance = None
    _instance_lock = threading.Lock()
    
    @classmethod
    def get_instance(cls, options=None, logger=None):
        """
        Get or create the singleton instance of StatusManager.
        
        Args:
            options: Configuration options (required first time)
            logger: Logger object (required first time)
            
        Returns:
            The singleton StatusManager instance
        """
        with cls._instance_lock:
            if cls._instance is None:
                if options is None or logger is None:
                    raise ValueError("options and logger required for initial instance creation")
                cls._instance = cls(options, logger)
            return cls._instance
    
    def __init__(self, options, logger, gparray, conn, gpEnv):
        self.options = options
        self.logger = logger
        self._status_filename = self.options.coordinator_data_directory + '/gprebalance.status.json'
        self.initial_gparray= gparray
        self._flush_interval = 2.0
        self._last_flush_time = 0
        self._dirty = False
        self._lock = threading.RLock()
        self.conn = conn
        self.gpenv = gpEnv

        # Initialize the status data structure
        self.status_data = {
            "current_status": None,
            "rebalance_dir" :None,
            "hosts_file" : None,
            "status_history": [],
            "moves": [],
            "last_modified": datetime.now().isoformat()
        }
        
        self.conf_dir = None
        self.target_hosts_filename = None
        
        # Load existing status file if it exists
        if os.path.exists(self._status_filename):
            self._read_status_file()
            self.analyze_gprecoverseg_states()

        
        atexit.register(self._ensure_flush)
        self._lock_timeout = 10  # seconds to wait for lock acquisition
        self._max_retries = 3    # maximum number of retries for operations
        self._retry_delay = 0.5  # seconds between retries

        self._stop_flush_thread = False
        self._flush_thread = threading.Thread(target=self._auto_flush_worker, daemon=True)
        self._flush_thread.start()
    
    def _auto_flush_worker(self):
        """Background thread that periodically flushes dirty status to disk"""
        while not self._stop_flush_thread:
            if self._dirty and time.time() - self._last_flush_time >= self._flush_interval:
                try:
                    self._flush_to_disk()
                except Exception as e:
                    self.logger.error(f"Auto-flush failed: {str(e)}")
            time.sleep(0.5)  # Check every half second
    
    def _ensure_flush(self):
        """Ensure any pending changes are flushed to disk"""
        if self._dirty:
            try:
                self._flush_to_disk()
            except Exception as e:
                self.logger.error(f"Final flush failed: {str(e)}")
    
    def _read_status_file(self):
        """Load status data from disk once"""
        self.logger.debug(f"Loading status file: {self._status_filename}")
        try:
            with open(self._status_filename, 'r') as fp:
                data = json.load(fp)
                
                # Basic validation
                required_keys = ["current_status", "status_history", "moves"]
                for key in required_keys:
                    if key not in data:
                        raise InvalidStatusError(f"Status file missing required key: {key}")
                
                # Store the data
                self.status_data = data
                self.conf_dir = self.status_data["rebalance_dir"]
                
                self._dirty = False
                self._last_flush_time = time.time()
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in status file: {str(e)}")
            raise InvalidStatusError(f"Status file contains invalid JSON: {str(e)}")
        except Exception as e:
            self.logger.error(f"Failed to load status file: {str(e)}")
            raise
        
    def _flush_to_disk(self):
        """Write current status data to disk with file locking"""
        with self._lock:
            if not self._dirty:
                return
                
            self.logger.debug("Flushing status data to disk")
            # Update last modified timestamp
            self.status_data["last_modified"] = datetime.now().isoformat()            
            # Create temporary file
            temp_file = f"{self._status_filename}.tmp"
            try:
                # Write to temp file first
                with open(temp_file, 'w') as fp:
                    json.dump(self.status_data, fp, indent=2)
                    fp.flush()
                    os.fsync(fp.fileno())
                
                # Atomic rename for safer file updates
                os.rename(temp_file, self._status_filename)
                
                self._dirty = False
                self._last_flush_time = time.time()
                self.logger.debug("Status data flushed successfully")
            except Exception as e:
                self.logger.error(f"Failed to flush status data: {str(e)}")
                # Clean up temp file if it exists
                if os.path.exists(temp_file):
                    try:
                        os.unlink(temp_file)
                    except:
                        pass
                raise
    
    def create_status_file(self):
        """Creates a new status file with initial state"""
        with self._lock:
            self.status_data = {
                "current_status": "UNINITIALIZED",
                "rebalance_dir" : None,
                "hosts_file" : None,
                "status_history": [
                    {
                        "status": "UNINITIALIZED",
                        "timestamp": datetime.now().isoformat(),
                        "info": None
                    }
                ],
                "moves": [],
                "last_modified": datetime.now().isoformat()
            }
            self._dirty = True
            self._flush_to_disk()  # Immediate flush for initial creation
    
    def get_current_status(self) -> Tuple[str, str]:
        """Gets the current status"""
        with self._lock:
            if not self.status_data["status_history"]:
                return (None, None)
            
            last_status = self.status_data["status_history"][-1]
            return (last_status["status"], last_status["info"])
    
    def set_status(self, statusEnum: RebalanceStatus, status_info=None):
        """Set overall rebalance status"""
        status = statusEnum.value
        with self._lock:
            status_entry = {
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "info": status_info
            }
            
            self.status_data["current_status"] = status
            self.status_data["status_history"].append(status_entry)
            
            # Update conf_dir or target_hosts_filename if applicable
            if statusEnum == RebalanceStatus.PREPARED:
                self.status_data["rebalance_dir"] = status_info
            elif statusEnum == RebalanceStatus.VALIDATED:
                self.status_data["hosts_file"] = status_info
                
            self._dirty = True
            
            # Consider immediate flush for important status changes
            if statusEnum in [RebalanceStatus.FAILED, RebalanceStatus.PRE_EXECUTE_FAILED, RebalanceStatus.STEPS_FAILED, RebalanceStatus.ROLLBACK_FAILED]:
                self._flush_to_disk()
    
    def record_moves_batch(self, moves_data):

        with self._lock:
            for seq_no, segment, move, size, needs_switch in moves_data:
                move_entry = {
                    "dbid": segment.dbid,
                    "content": segment.content,
                    "role": segment.role,
                    "role_after_switch": ('p' if move.is_mirror else 'm') if needs_switch else segment.role,
                    "status": MoveStatus.PENDING.value,
                    "source_kbytes": size,
                    "seq_id": seq_no,
                }

                self.status_data["moves"].append(move_entry)

            if moves_data:
                self._dirty = True
                self._flush_to_disk()
    
    def update_move_status(self, dbids: List[int], status: MoveStatus):
        """Update status for specified moves"""
        with self._lock:
            updated = False
            for move in self.status_data["moves"]:
                if move["dbid"] in dbids:
                    move["status"] = status.value
                    updated = True
            
            if updated:
                self._dirty = True
                
                # Consider immediate flush for completion status
                if status in [MoveStatus.COMPLETED, MoveStatus.FAILED]:
                    self._flush_to_disk()
    
    def get_moves_by_status(self, status: MoveStatus) -> List[Dict[str, Any]]:
        """Get all moves with the specified status"""
        with self._lock:
            return [move.copy() for move in self.status_data["moves"] 
                   if move["status"] == status.value]
    
    def get_move_by_dbid(self, dbid: int) -> Optional[Dict[str, Any]]:
        """Get a specific move by dbid"""
        with self._lock:
            for move in self.status_data["moves"]:
                if move["dbid"] == dbid:
                    return move.copy()
            return None
    
    def flush(self):
        """Manually flush status data to disk"""
        self._flush_to_disk()
    
    def remove_all(self):
        """Remove the status file and reset state"""
        with self._lock:
            self._stop_flush_thread = True
            if self._flush_thread.is_alive():
                self._flush_thread.join(timeout=2.0)
                
            if os.path.exists(self._status_filename):
                try:
                    os.unlink(self._status_filename)
                except Exception as e:
                    self.logger.error(f"Failed to remove status file: {str(e)}")
            
            # Reset internal state
            self.status_data = {
                "current_status": None,
                "status_history": [],
                "moves": [],
                "last_modified": datetime.now().isoformat()
            }
            self._dirty = False
            self.conf_dir = None
            self.target_hosts_filename = None
    
    def analyze_gprecoverseg_states(self):
        if not self.conf_dir:
            return
        is_rollback = self.options.rollback
        plan = load_plan(self.conf_dir, is_rollback)
        failed_moves = []
        in_progress_moves = []
        not_started_moves = []
        completed_moves = []
        for move in self.status_data.get("moves", []):
            move_status = move["status"]
            if move_status == "FAILED":
                failed_moves.append(move)
            elif move_status == "IN_PROGRESS":
                in_progress_moves.append(move)
            elif move_status == "PENDING":
                not_started_moves.append(move)
            elif move_status == "COMPLETED":
                completed_moves.append(move)
        if failed_moves or in_progress_moves:
            self.logger.info(f"Found {len(failed_moves)} failed moves and {len(in_progress_moves)} in-progress moves")
            moves_to_analyze = failed_moves + in_progress_moves
            self._analyze_failed_segments(moves_to_analyze, plan)
            moves_to_recover = []
            for move in moves_to_analyze:
                if move["status"] == "ASK_USER":
                    if ask_yesno('', f"Failed mirror move (dbid={move['dbid']}, content={move['content']}) cannot be rerun"
                                     f" properly. Do you want to try to rollback it to original state?", "N"):
                        moves_to_recover.append(move)
                else:
                    move["status"] = MoveStatus.SKIPPED.value
            self._try_restore_config(moves_to_recover, plan)
            self._dirty = True
            self._flush_to_disk()

            for move in moves_to_analyze:
                if move["status"] == MoveStatus.SKIPPED.value:
                    self.logger.info(f"Couldn't revert the move (dbid={move['dbid']}, content={move['content']})."
                                     " Please, restore the required state and re-run")


    def _analyze_failed_segments(self, moves, plan):
        
        self.logger.info(f"Analyzing {len(moves)} moves for state recovery")
        segmentMap = {SegmentId(
            seg.dbid, seg.content): seg for seg in self.initial_gparray.getSegmentsAsLoadedFromDb()}
        hosts = set(self.initial_gparray.get_hostlist(includeCoordinator=False))
        unreachable_hosts = get_unreachable_segment_hosts(hosts, self.options.parallel)
        update_unreachable_flag_for_segments(self.initial_gparray, unreachable_hosts)
        plan_moves = {move.segid.dbid: move for move in plan.moves}
        for move in moves:
            segid = SegmentId(move["dbid"], move["content"])
            original_segment = plan.segmentMap[segid]
            current_segment = segmentMap[segid]

            peer = None
            is_mirror = False
            for pair in self.initial_gparray.segmentPairs:
                if pair.primaryDB.dbid == segid.dbid:
                    peer = pair.mirrorDB
                    break
                elif pair.mirrorDB.dbid == segid.dbid:
                    peer = pair.primaryDB
                    is_mirror = True
                    break

            if is_mirror:
                self.logger.info(f"Analyzing failed mirror move (dbid={original_segment.dbid}, content={original_segment.content}) "
                         f"to host {plan_moves[original_segment.dbid].dstHost.hostname}, directory {plan_moves[original_segment.dbid].target_datadir}")
                if original_segment.hostname == current_segment.hostname and\
                    original_segment.address == current_segment.address and\
                    original_segment.dbid == current_segment.dbid and\
                    (original_segment.role == current_segment.role or move["role_after_switch"] == current_segment.role) and\
                    original_segment.datadir == current_segment.datadir and\
                    peer.valid and not current_segment.valid:
                    # case 1 gprecoverseg could only have updated pg_hba_conf
                    # of corresponding primary before failure
                    # check that current_segment is running
                    if current_segment.unreachable:
                        move["status"] = MoveStatus.FAILED.value
                        continue
                    commandStr = f"pg_ctl status -D {current_segment.datadir}"
                    cmd_status = Command("pid", commandStr, REMOTE, current_segment.hostname)
                    cmd_status.run()
                    if cmd_status.get_return_code() != 0:
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue

                    #restore primary's pg_hba_conf
                    entries = hba_upd.create_entries(peer.hostname, original_segment.hostname, self.options.hba_hostnames)
                    updatecmd = hba_upd.SegUpdateHba(entries, peer.datadir, REMOTE, peer.hostname)
                    updatecmd.run()
                    if updatecmd.get_return_code() != 0:
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue
                    host_port_tuple = (peer.hostname, peer.port)
                    pg.kill_existing_walsenders_on_primary([host_port_tuple])
                    time.sleep(wait_timeout_s)
                    cmd_status = Command("pid", commandStr, REMOTE, current_segment.hostname)
                    cmd_status.run()
                    if cmd_status.get_return_code() != 0:
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue
                    move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                    continue
                # case 2 : gp_segment_configuration has been updated by gprecoverseg
                # from buildMirrorSegments.py:295 and we need to define whether
                # basebackup was launched, completed or interrupred
                elif original_segment.dbid == current_segment.dbid and\
                    (original_segment.role == current_segment.role or\
                    # case when seg is moved after 1st switch
                    move["role_after_switch"] == current_segment.role or
                    #primary only move. 2 switches, case when seg is moved after 1st switch
                    move["role_after_switch"] == move["role"]) and\
                    (original_segment.hostname != current_segment.hostname or\
                     original_segment.address != current_segment.address or\
                     original_segment.datadir != current_segment.datadir):
                    
                    if current_segment.valid and current_segment.mode == 's':
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue

                    # check whether target directory exists
                    cmd = Command(
                         name="check_directory",
                         cmdStr=f"test -d {current_segment.datadir} && echo 'exists' || echo 'not_exists'",
                         ctxt=REMOTE,
                        remoteHost=current_segment.hostname)
                    cmd.run()
                    if not cmd.was_successful() or cmd.get_stdout().strip() != 'exists':
                        self.logger.info(f"Failed to check directory on {current_segment.hostname}")
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue

                    #check the port update
                    cmd = Command(
                    name="get_postgresql_conf_port",
                    cmdStr=f"grep -E '^port\\s*=' {current_segment.datadir}/postgresql.conf | sed -E 's/^port\\s*=\\s*([0-9]+).*/\\1/'",
                    ctxt=REMOTE,
                    remoteHost=current_segment.hostname
                     )
                    cmd.run()
                    if not cmd.was_successful():
                        self.logger.info(f"Failed to get port from postgresql.conf on {current_segment.hostname}: {cmd.get_stderr()}")
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue
                    output = cmd.get_stdout().strip()
                    if not output or not output.isdigit():
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue
                    datadir_port = int(output)
                    if datadir_port != current_segment.port:
                        self.logger.info(f"Port mismatch in postgresql.conf: expected {current_segment.port}, got {datadir_port}")
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue

                    #check if server is running
                    process_running = self._check_postgres_process(current_segment.hostname, current_segment.datadir)
                    if not process_running:

                        is_started = self._wait_for_pg_startup(current_segment.hostname, current_segment.datadir, timeout_seconds=20)
                        if not is_started:
                            attemp_start = self._attempt_start_mirror(current_segment)
                            if not attemp_start and not self._wait_for_pg_startup(current_segment.hostname, current_segment.datadir, timeout_seconds=20):
                                self.logger.info("Failed to start the mirror")
                                move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                                continue
                        if not self._wait_for_config_update(current_segment.dbid, 60):
                            move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                            continue
                    elif not self._wait_for_config_update(current_segment.dbid, 60):
                        move["status"] = MoveStatus.ASK_ROLLBACK_OR_SKIP.value
                        continue
                    
                    move["status"] = MoveStatus.COMPLETED.value


    def _check_postgres_process(self, host, data_dir):
        try:
            # First check with pg_ctl status
            pg_ctl_cmd = Command(
                name="check_pg_ctl_status",
                cmdStr=f"pg_ctl status -D {data_dir}",
                ctxt=REMOTE,
                remoteHost=host
            )
            pg_ctl_cmd.run()

            # pg_ctl status returns 0 if server is running
            if pg_ctl_cmd.get_return_code() == 0:
                return True

            # If pg_ctl status failed, check for actual process
            # Get postmaster.pid contents if it exists
            pid_cmd = Command(
                name="check_postmaster_pid",
                cmdStr=f"test -f {data_dir}/postmaster.pid && cat {data_dir}/postmaster.pid | head -1",
                ctxt=REMOTE,
                remoteHost=host
            )
            pid_cmd.run()

            if not pid_cmd.was_successful() or not pid_cmd.get_stdout().strip():
                return False

            pid = pid_cmd.get_stdout().strip()
            if not pid.isdigit():
                return False

            # Check if process with that PID exists and is postgres
            process_cmd = Command(
                name="check_postgres_process",
                cmdStr=f"ps -p {pid} -o cmd= | grep postgres",
                ctxt=REMOTE,
                remoteHost=host
            )
            process_cmd.run()

            return process_cmd.was_successful() and process_cmd.get_stdout().strip()

        except Exception as e:
            self.logger.error(f"Error checking postgres process on {host}: {str(e)}")
            return False
        
    def _wait_for_pg_startup(self, host, data_dir, timeout_seconds=120):
       
        self.logger.info(f"Waiting for PostgreSQL to start on {host}:{data_dir}")

        start_time = time.time()
        check_interval = 5  # Check every 5 seconds

        while time.time() - start_time < timeout_seconds:
            if self._check_postgres_process(host, data_dir):
                elapsed = int(time.time() - start_time)
                self.logger.info(f"PostgreSQL started on {host}:{data_dir} after {elapsed} seconds")
                return True

            # Wait before next check
            time.sleep(check_interval)
            elapsed = int(time.time() - start_time)

        self.logger.info(f"Timeout waiting for PostgreSQL to start on {host}:{data_dir}")
        return False
    
    def _wait_for_config_update(self, dbid, timeout_seconds=120):
       
        start_time = time.time()
        check_interval = 5

        conf_sql = """
                SELECT c.mode, c.status
                FROM gp_segment_configuration AS c WHERE c.dbid = {sdbid}
                """ .format(sdbid=dbid)
        while time.time() - start_time < timeout_seconds:
            cursor = dbconn.query(self.conn, conf_sql)
            for mode, status in cursor:
                if mode == 's' and status == 'u':
                    return True

            # Wait before next check
            time.sleep(check_interval)
            elapsed = int(time.time() - start_time)

        return False
    
    def _try_restore_config(self, moves_to_recover, plan):
        from gppylib.commands.gp import GpRecoverSeg
        gparray = copy.deepcopy(self.initial_gparray)
        def write_gprecoverseg_config(segment, conf_dir):
            filename = self.conf_dir + "_revert_" + "dbid" + str(segment.dbid)
            with open(filename, 'w') as fp:
                line = (f"{canonicalize_address(segment.address)}|"
                    f"{segment.port}|{segment.datadir}")
                self.logger.info(
                "About to run gprecoverseg for recovering mirror  "
                f"(dbid = {segment.dbid}, content = {segment.content}) {line}")
                fp.write(line)
            return filename
        for move in moves_to_recover:
            segid = SegmentId(move["dbid"], move["content"])
            original_segment = plan.segmentMap[segid]
            dbids = {}
            seg = None
            peer = None
            for pair in gparray.segmentPairs:
                if pair.mirrorDB.dbid == segid.dbid:
                    pair.mirrorDB = copy.copy(original_segment)
                    pair.mirrorDB.status = 'd'
                    pair.mirrorDB.mode = 'n'
                    dbids[pair.mirrorDB.dbid] = True
                    pair.mirrorDB.role = 'm'
                    seg=pair.mirrorDB
                    peer = pair.primaryDB
            configInterface.getConfigurationProvider().updateSystemConfig(
                gparray,
                "gprebalance: segment config for resync",
                dbIdToForceMirrorRemoveAdd=dbids,
                useUtilityMode=False,
                allowPrimary=False
            )
            #host_port_tuple = (peer.hostname, peer.port)
            #pg.kill_existing_walsenders_on_primary([host_port_tuple])
            filename = write_gprecoverseg_config(seg, self.conf_dir)

            recoversegOptions = f"-a -l {os.path.join(os.environ.get('HOME', '.'),'gpAdminLogs/rebalance')} "\
                            f"-i {filename}"
            if self.options.hba_hostnames:
                recoversegOptions += " --hba-hostnames"
            cmd = GpRecoverSeg("Running gprecoverseg", options=recoversegOptions)
            cmd.run()
            recordStatus = MoveStatus.REVERTED.value
            if not cmd.was_successful():
                self.logger.info(f"Failed to rollback mirror moves (dbid = {seg.dbid}, content = {seg.content}): {cmd.get_stderr()}, skipping...")
                recordStatus = MoveStatus.SKIPPED.value
            move["status"] = recordStatus
            os.unlink(filename)
    
    def _attempt_start_mirror(self, segment):
        from gppylib.commands.gp import SegmentStart
        from gppylib.gp_era import read_era
        seg = Segment(None, None, None, None, None, None, None, None,
                  segment.port, segment.datadir)
        cmd =  cmd = SegmentStart(
                name="Attemping to start segment with dbid %s:" % (str(segment.dbid))
                , gpdb=seg
                , numContentsInCluster=0
                , era=read_era(self.gpenv.getCoordinatorDataDir(), logger=self.logger)
                , mirrormode="mirror"
                , utilityMode=False
                , ctxt=REMOTE
                , remoteHost=segment.hostname)
        self.logger.info(str(cmd))
        cmd.run()
        if not cmd.was_successful():
            return False
        return True