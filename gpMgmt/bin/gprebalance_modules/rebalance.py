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
    def __init__(self, logger, gparray, dburl, options, gpEnv):
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
        self.statusManager = StatusManager(self.options, self.logger, self.original_gparray, self.conn, gpEnv)

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
    
    def load_plan(self, datadir, rollback):
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

    def execute_plan(self, plan: Plan):
        self.executor = RebalanceExecutor(plan,
                                     self.original_gparray,
                                     self.segmentMap,
                                     plan.in_conf,
                                     self.logger,
                                     self.statusManager,
                                     self.conn,
                                     self.dburl,
                                     self.options)
        self.executor.execute_moves()

    def get_state_from_file(self):
        """Returns expansion state from status file"""
        status = self.statusManager.get_current_status()[0]
        if status:
            return RebalanceStatus(status)
        return None

    def cleanup_directory(self):
        self.logger.info('Dropping rebalance directory')
        datadir = self.options.coordinator_data_directory + CONF_DIR
        cmd = RemoveDirectory("Dropping rebalance directory", datadir)
        cmd.run(validateAfter=True)

    def remove_status_file(self):
        self.logger.info('Dropping status file')
        if self.statusManager:
            self.statusManager.remove_all()
    

    def rollback(self, plan:Plan):
        new_plan = Plan()
        cb = ClusterBalancer(self.current_conf, (set(), set()),
                             self.segmentMap, 0, MirrorStrategy.MIRRORLESS)
        new_moves, rls = cb.get_moves_between_states(self.current_conf, plan.in_conf)
        for new_move in new_moves:
            seg = plan.segmentMap[new_move.segid]
            if self.segmentMap[new_move.segid].role == seg.role:
                new_move.target_datadir = seg.datadir
                new_move.target_port = seg.port
            else:
                for id, pair_seg in plan.segmentMap.items():
                    if new_move.segid.contentid == id.contentid and new_move.segid.dbid != id.dbid:
                        assert(pair_seg.role == self.segmentMap[new_move.segid].role)
                        new_move.target_datadir = pair_seg.datadir
                        new_move.target_port = pair_seg.port
                        break
        for segid, current_seg in self.segmentMap.items():
            target_seg = plan.segmentMap[segid]
            if (current_seg.hostname, current_seg.address) == (target_seg.hostname, target_seg.address) \
                and current_seg.role == target_seg.role and current_seg.datadir != target_seg.datadir:
                # Are there any cases besides swap when previously rebalance moved segment
                # to the same host but to different dir?
                assert(current_seg.role == 'm')
                move = Move(segid, self.current_conf[(current_seg.hostname, current_seg.address)],
                            self.current_conf[(current_seg.hostname, current_seg.address)], True,
                            target_seg.datadir, target_seg.port)
                new_moves.append(move)                         
        
        new_plan.moves = new_moves
        new_plan.segmentMap = self.segmentMap
        new_plan.in_conf = self.current_conf
        new_plan.out_conf = plan.in_conf

        self.executor = RebalanceExecutor(new_plan, self.original_gparray, self.segmentMap,
                                          self.current_conf, self.logger, self.statusManager,
                                          self.conn, self.dburl, self.options)
        self.executor.execute_moves()

    def resume(self, plan):
        """TODO: implement proper state handling and provide
        possibility to perform rebalance after fails"""
        raise NotImplementedError('Resuming operation is not implemented. Call gprebalance -c and rerun')

    def shutdown(self):
        self.logger.info('Shutting down gprebalance...')
        if self.executor:
            self.executor.shutdown()
            self.executor = None
        if self.conn:
            self.conn.close()
            self.conn = None
