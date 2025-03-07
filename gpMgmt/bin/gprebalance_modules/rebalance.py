from collections import defaultdict
from datetime import datetime
import yaml
import os
from dataclasses import dataclass
from typing import List, Set, Dict, NamedTuple
from enum import Enum
from gppylib.db import dbconn
from gppylib.gparray import GpArray, Segment, MODE_NOT_SYNC, STATUS_DOWN
from gppylib.commands.base import *
from gppylib.commands.gp import *


class SegmentId(NamedTuple):
    dbid: int
    contentid: int


@dataclass
class SegmentSize():
    source_data_dir_usage: int
    source_tablespace_usage: Dict[str, int]


class HostStatus(Enum):
    ACTIVE = "active"
    DECOMMISSIONING = "decommissioning"
    NEW = "new"
    REPLACEMENT = "replacement"


class Host:
    def __init__(self, hostname: str, address: str, primary_datadirs: Set[str] = set(),
                 mirror_datadirs: Set[str] = set(), primary_segments: Set[SegmentId] = set(),
                 mirror_segments: Set[SegmentId] = set(), status: HostStatus = HostStatus.ACTIVE):
        self.hostname = hostname
        self.address = address
        # set of strings
        self.primary_datadirs = primary_datadirs
        self.mirror_datadirs = mirror_datadirs
        # set of SegmentId(dbid, content id)
        self.primary_segments = primary_segments
        self.mirror_segments = mirror_segments
        self.replacement_for = None
        self.status = status

    def __eq__(self, other):
        return self.hostname == other.hostname and \
            self.address == other.address

    def __hash__(self):
        return hash((self.hostname, self.address))

    def __len__(self):
        return len(self.primary_segments)

    def is_overloaded(self, target_load: int) -> bool:
        return len(self.primary_segments) > target_load

    def is_underloaded(self, target_load: int) -> bool:
        return len(self.primary_segments) < target_load

    def add_primary(self, seg):
        self.primary_segments.add(seg)

    def remove_primary(self, seg):
        self.primary_segments.discard(seg)

    def add_mirror(self, seg):
        self.mirror_segments.add(seg)

    def remove_mirror(self, seg):
        self.mirror_segments.discard(seg)

    def __str__(self):
        mirrors = []
        prims = []
        for m in self.mirror_segments:
            mirrors.append(f"M{m.contentid}")
        for p in self.primary_segments:
            prims.append(f"S{p.contentid}")

        ps = " ".join(prims)
        ms = " ".join(mirrors)
        return f"H_{self.hostname}:[{ps} {ms}]"


ClusterState = Dict[tuple[str, str], Host]


class MirrorStrategy(Enum):
    MIRRORLESS = "none"
    GROUPED = "grouped"
    SPREAD = "spread"


from gprebalance_modules.rebalance_plan import ClusterBalancer, Move, Plan  # nopep8
from gprebalance_modules.rebalance_executor import RebalanceExecutor, CONF_DIR  # nopep8
from gprebalance_modules.rebalance_status import StatusManager, RebalanceStatus  # nopep8


class GPRebalance:
    def __init__(self, logger, gparray, dburl, options):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.original_gparray = gparray
        self.conn = dbconn.connect(
            self.dburl, encoding='UTF8', allowSystemTableMods=True)
        if options.mirroring == 'spread':
            self.target_strategy = MirrorStrategy.SPREAD
        elif options.mirroring == 'grouped':
            self.target_strategy = MirrorStrategy.GROUPED
        else:
            self.target_strategy = MirrorStrategy.MIRRORLESS

        self.current_conf = self.getHostsFromGpArray()
        self.current_hosts = set(list(self.current_conf.values()))
        self.target_hosts = self.current_hosts
        if options.filename:
            with open(options.filename, 'r') as fp:
                hosts = {}
                config = yaml.safe_load(fp)
                for host_config in config['hosts']:
                    key = (host_config['hostname'], host_config['address'])
                    file_host = Host(hostname=host_config['hostname'],
                                      address=host_config['address'],
                                      primary_datadirs=set(
                        host_config['primary_datadirs']),
                        primary_segments=set(),
                        mirror_datadirs=set(
                        host_config['mirror_datadirs']),
                        mirror_segments=set())
                    hosts[key] = file_host
                    if key in self.current_conf:
                        self.current_conf[key].primary_datadirs |= file_host.primary_datadirs
                        self.current_conf[key].mirror_datadirs |= file_host.mirror_datadirs
                    # User can explicitly mark the host to be replacement for existing one
                    if "replace" in host_config:
                        rep = tuple(host_config['replace'].split(', '))
                        if rep in self.current_conf:
                            hosts[key].replacement_for = rep
                            hosts[key].status = HostStatus.REPLACEMENT
                            self.current_conf[rep].status = HostStatus.DECOMMISSIONING
                        else:
                            raise ValueError(f"\'replace\' field of host {host_config['hostname']} contains"
                                             "does not belong to current configuration ")

                self.target_hosts = set(list(hosts.values()))
                if len(self.current_hosts & self.target_hosts) < len(self.current_hosts):
                    for k, h in self.current_conf.items():
                        if k not in hosts:
                            h.status = HostStatus.DECOMMISSIONING

        self.unpreferred_segments = self.getSegmentsUnpreferredRole()
        self.segmentMap = {SegmentId(
            seg.dbid, seg.content): seg for seg in self.original_gparray.getSegmentsAsLoadedFromDb()}
        self.statusManager = StatusManager(
            self.conn, self.options, self.logger)

        self.executor = None

    def getSegmentsUnpreferredRole(self) -> List[tuple[Segment, Segment]]:
        segs = []
        for pair in self.original_gparray.segmentPairs:
            prim = pair.primaryDB
            mir = pair.mirrorDB
            if prim.role != prim.preferred_role and mir.role != mir.preferred_role:
                segs.append((prim, mir))
        return segs

    def setMirroringStrategy(self, strategy: MirrorStrategy):
        self.target_strategy = strategy

    def getHostsFromGpArray(self) -> ClusterState:
        hosts = {}
        for seg in self.original_gparray.getSegmentsAsLoadedFromDb():
            if seg.content >= 0:
                hosts[(seg.hostname, seg.address)] = Host(
                    hostname=seg.hostname, address=seg.address, primary_datadirs=set(), primary_segments=set(), mirror_datadirs=set(), mirror_segments=set())
        for pair in self.original_gparray.segmentPairs:
            primary = pair.primaryDB
            mirror = pair.mirrorDB
            key_pr = (primary.hostname, primary.address)
            hosts[key_pr].primary_datadirs.add(
                os.path.dirname(primary.datadir))
            hosts[key_pr].primary_segments.add(
                SegmentId(primary.dbid, primary.content))
            if mirror:
                key_mr = (mirror.hostname, mirror.address)
                hosts[key_mr].mirror_datadirs.add(
                    os.path.dirname(mirror.datadir))
                hosts[key_mr].mirror_segments.add(
                    SegmentId(mirror.dbid, mirror.content))
        return hosts

    def getNewHosts(self) -> tuple[Set, Set]:
        new_hosts = self.target_hosts.difference(
            self.current_hosts & self.target_hosts)
        replacement_hosts = set()
        other = set()
        for h in new_hosts:
            if h.status == HostStatus.REPLACEMENT:
                replacement_hosts.add(h)
            else:
                h.status = HostStatus.NEW
                other.add(h)
        return other if len(other) > 0 else None, replacement_hosts if len(replacement_hosts) > 0 else None

    def createPlan(self) -> Plan:
        primaries_count = self.original_gparray.get_primary_count()
        total_hosts = len(self.target_hosts)
        assert (primaries_count % total_hosts == 0)
        self.logger.info('Planning rebalance...')

        new_hosts, replacement_hosts = self.getNewHosts()
        target_load = primaries_count // total_hosts
        original_segments_map = {SegmentId(
            seg.dbid, seg.content): seg for seg in self.original_gparray.getSegmentsAsLoadedFromDb()}

        balancer = ClusterBalancer(self.current_conf, (new_hosts, replacement_hosts),
                                   original_segments_map, target_load, self.target_strategy)

        return balancer.getPlan(balancer.balance())

    def save_plan(self, plan: Plan):
        # pickle the plan in conf directory
        datadir = self.options.coordinator_data_directory + CONF_DIR
        os.makedirs(datadir, exist_ok=True)
        plan.save_to_file(datadir, "plan")
        self.statusManager.set_status('PLANNED', datadir)

    def load_plan(self, datadir):
        filename = datadir + "/plan.pkl"
        if not os.path.exists(filename):
            raise FileNotFoundError(f"No pickle file found at {filename}")
        with open(filename, 'rb') as f:
            plan = pickle.load(f)
        return plan

    def execute_plan(self, plan: Plan):
        self.executor = RebalanceExecutor(plan,
                                     self.original_gparray,
                                     self.segmentMap,
                                     self.current_conf,
                                     self.logger,
                                     self.statusManager,
                                     self.conn,
                                     self.dburl,
                                     self.options)
        self.executor.execute_moves()

    @staticmethod
    def prepare_gpdb_state(logger, dburl, options) -> str:
        status_file_exists = os.path.exists(
            options.coordinator_data_directory + '/gprebalance.status')
        gprebalance_db_status = None

        if not status_file_exists:
            logger.info('Querying gprebalance schema for current state')
            try:
                gprebalance_db_status = GPRebalance.get_status_from_db(
                    dburl, options)
            except Exception as e:
                raise Exception(
                    'Error while trying to query the gprebalance schema: %s' % e)
            logger.debug('Expansion status returned is %s' %
                         gprebalance_db_status)
        return gprebalance_db_status

    @staticmethod
    def get_status_from_db(dburl, options) -> str:
        """Gets gprebalance status from the gprebalance schema"""
        status_conn = None
        gprebalance_db_status = None
        if get_local_db_mode(options.coordinator_data_directory) == 'NORMAL':
            try:
                status_conn = dbconn.connect(dburl, encoding='UTF8')
                # Get the last status entry
                cursor = dbconn.query(
                    status_conn, 'SELECT status FROM gprebalance.status ORDER BY updated DESC LIMIT 1')
                if cursor.rowcount == 1:
                    gprebalance_db_status = cursor.fetchone()[0]

            except Exception:
                # rebalance schema doesn't exists or there was a connection failure.
                pass
            finally:
                if status_conn:
                    status_conn.close()

        # make sure gprebalance schema doesn't exist since it wasn't in DB provided
        if not gprebalance_db_status:
            conn = dbconn.connect(dburl, encoding='UTF8', utility=True)
            try:
                count = dbconn.querySingleton(conn,
                                              "SELECT count(n.nspname) FROM pg_catalog.pg_namespace n WHERE n.nspname = 'gprebalance'")
                if count > 0:
                    raise Exception(
                        "Existing rebalance state could not be determined, but a gprebalance schema already exists. Cannot proceed.")
            finally:
                conn.close()

        return gprebalance_db_status

    def get_state_from_file(self):
        """Returns expansion state from status file"""
        return self.statusManager.get_current_status()[0]

    def setup_schema(self):
        self.statusManager.set_status('REBALANCE_PREPARE_SCHEMA_STARTED')
        self.logger.info('Creating expansion schema')
        self.statusManager.setup_schema()
        self.statusManager.set_status('REBALANCE_PREPARE_SCHEMA_DONE')
        self.statusManager.set_db_status(RebalanceStatus.INITIALIZED)

    def cleanup_schema(self):
        self.logger.info('Dropping rebalance schema')
        self.statusManager.cleanup_schema()

    def remove_status_file(self):
        if self.statusManager:
            self.statusManager.remove_all()

    def resume(self):
        """TODO: implement proper state handling and provide
        possibility to perform rebalance after fails"""
        pass

    def shutdown(self):
        self.logger.info('Shutting down gprebalance...')
        if self.executor:
            self.executor.shutdown()
            self.executor = None
        if self.conn:
            self.conn.close()
            self.conn = None
