import copy
import os
import pickle
import random
import math
from typing import List, Set, Dict
from dataclasses import dataclass
from collections import defaultdict
from gprebalance_modules.rebalance import Host, HostStatus, SegmentId, ClusterState, MirrorStrategy, Segment


@dataclass
class Move:
    segid: SegmentId
    srcHost: Host
    dstHost: Host
    is_mirror: bool
    target_datadir: str
    target_port: int


class Plan:
    def __init__(self):
        self.in_conf = None
        self.moves = []
        self.out_conf = None
        self.segmentMap = None
        self.roles = []

    def __str__(self):
        finalStr = ""
        for i in range(len(self.moves)):
            m = self.moves[i]
            if self.roles[m.segid] == 'p':
                finalStr += f"{i} :S{m.segid.contentid} :"
            else:
                finalStr += f"{i} :M{m.segid.contentid} :"
            finalStr += f"H{m.srcHost.hostname} -> H{m.dstHost.hostname}\n"
        return finalStr

    def save_to_file(self, directory: str, filename: str) -> str:
        file_path = os.path.join(directory, f"{filename}.pkl")

        # Pickle and save the plan
        with open(file_path, 'wb') as f:
            pickle.dump(self, f)


class NoValidMovesError(Exception):
    def __init__(self, msg="No valid targets for selected segment"):
        super().__init__(msg)


class ClusterBalancer():
    def __init__(self, initial_conf: ClusterState,
                 new_hosts: tuple[Set[Host], Set[Host]],
                 segmentMap: Dict[SegmentId, Segment],
                 target_load: int,
                 strat: MirrorStrategy
                 ):
        self.in_conf = initial_conf
        self.new_hosts, self.replacement_hosts = new_hosts
        self.initialSegmentMap = segmentMap
        self.target_load = target_load
        self.target_strategy = strat
        self.initial_mirror_mapping = {}
        for k, v in segmentMap.items():
            if v.isSegmentMirror():
                self.initial_mirror_mapping[k.contentid] = v

    def _get_working_conf(self) -> ClusterState:
        """Get host-centric representation of cluster configuration"""
        working_conf = copy.deepcopy(self.in_conf)
        if self.new_hosts:
            for h in self.new_hosts:
                assert (h.status == HostStatus.NEW)
                working_conf[(h.hostname, h.address)] = copy.deepcopy(h)
        if self.replacement_hosts:
            for h in self.replacement_hosts:
                assert (h.status == HostStatus.REPLACEMENT)
                working_conf[(h.hostname, h.address)] = copy.deepcopy(h)
                assert (
                    working_conf[h.replacement_for].status == HostStatus.DECOMMISSIONING)
        return working_conf

    def _getCurrentMirrorMapping(self, state: ClusterState) -> Dict[int, tuple[str, str]]:
        mirror_mapping = {}
        for k, v in state.items():
            for mirid in v.mirror_segments:
                mirror_mapping[mirid.contentid] = (v.hostname, v.address)
        return mirror_mapping

    def _getCurrentSegmentMapping(self, state: ClusterState) -> Dict[int, tuple[str, str]]:
        segment_mapping = {}
        for k, v in state.items():
            for segid in v.primary_segments:
                segment_mapping[segid.contentid] = (v.hostname, v.address)
        return segment_mapping

    def _satisfies_strategy(self, state: ClusterState) -> bool:
        mirror_mapping = self._getCurrentMirrorMapping(state)
        if self.target_strategy == MirrorStrategy.GROUPED:
            for _, host in state.items():
                if host.status == HostStatus.DECOMMISSIONING:
                    continue
                mirror_hosts = set()
                for s in host.primary_segments:
                    mirror_hosts.add(mirror_mapping[s.contentid])
                if len(mirror_hosts) > 1:
                    return False
                elif len(mirror_hosts & set([(host.hostname, host.address)])) > 0:
                    return False
                elif len(host.mirror_segments) == 0:
                    return False
        elif self.target_strategy == MirrorStrategy.SPREAD:
            for _, host in state.items():
                if host.status == HostStatus.DECOMMISSIONING:
                    continue
                mirror_hosts_counts: Dict[Host, int] = defaultdict(int)
                for s in host.primary_segments:
                    mirror_hosts_counts[mirror_mapping[s.contentid]] += 1
                for t_h, count in mirror_hosts_counts.items():
                    if t_h == (host.hostname, host.address):
                        return False
                    elif count > 1:
                        return False
        return True

    def _check_balance(self, state: ClusterState) -> bool:
        for _, host in state.items():
            load = len(host.primary_segments)
            deviation = abs(load - self.target_load)
            if host.status == HostStatus.DECOMMISSIONING and (host.primary_segments or host.mirror_segments):
                return False
            elif host.status != HostStatus.DECOMMISSIONING and deviation != 0:
                return False
        return True

    def _move_from_decomissioning(self, state: ClusterState):
        if self.replacement_hosts:
            for rep in self.replacement_hosts:
                # Host from which we must move all segments
                decom_host_id = rep.replacement_for
                replacement_host_id = (rep.hostname, rep.address)
                assert (state[decom_host_id].status ==
                        HostStatus.DECOMMISSIONING)
                primaries = state[decom_host_id].primary_segments
                mirrors = state[decom_host_id].mirror_segments
                for segid in primaries:
                    state[replacement_host_id].add_primary(segid)
                for segid in mirrors:
                    state[replacement_host_id].add_mirror(segid)
                state[decom_host_id].primary_segments = set()
                state[decom_host_id].mirror_segments = set()

    def _check_constraints(self, state: ClusterState):
        return self._check_balance(state) and self._satisfies_strategy(state)

    def balance(self) -> ClusterState:
        working_conf = self._get_working_conf()
        # First, check if there are hosts explicitly marked for replacement
        if self.replacement_hosts:
            self._move_from_decomissioning(working_conf)
            if self._check_constraints(working_conf):
                return working_conf

        conf = self.rough_balance(working_conf)
        assert (self._check_constraints(conf))
        return conf

    def _is_move_valid(self, segmentid: SegmentId, target_host: Host, state: ClusterState) -> bool:
        segment_locations = self._getCurrentSegmentMapping(state)

        current_host = segment_locations[segmentid.contentid]

        if current_host == (target_host.hostname, target_host.address):
            return False

        # Check target host status
        if target_host.status == HostStatus.DECOMMISSIONING:
            return False

        # For primary moves, only check capacity
        if state[current_host].status != HostStatus.DECOMMISSIONING:
            return len(target_host.primary_segments) + 1 <= self.target_load

        return True

    def rough_balance(self, working_conf: ClusterState) -> ClusterState:

        def calculate_surplus_deficit():
            loads = {(h.hostname, h.address): len(h.primary_segments)
                     for _, h in working_conf.items()
                     if h.status != HostStatus.DECOMMISSIONING}
            surplus = {}
            deficit = {}
            for host_id, load in loads.items():
                surplus[host_id] = max(0, load - self.target_load)
                deficit[host_id] = max(0, self.target_load - load)
            return surplus, deficit
        # Phase 1: Redistribute Primary Segments
        while True:
            # Priority 1: Clear decommissioning hosts
            decom_hosts = [h for _, h in working_conf.items()
                           if h.status == HostStatus.DECOMMISSIONING]

            moved = False
            # Move from decommissioning hosts first
            for source in decom_hosts:
                if not source.primary_segments:
                    continue

                potential_targets = sorted(
                    [h for _, h in working_conf.items()
                     if h.status not in (HostStatus.DECOMMISSIONING,)],
                    key=lambda h: len(h.primary_segments)
                )

                for segment in sorted(list(source.primary_segments), key=lambda x: x.contentid, reverse=True):
                    for target in potential_targets:
                        if self._is_move_valid(segment, target, working_conf):
                            source.remove_primary(segment)
                            target.add_primary(segment)
                            moved = True
                            break

            surplus, deficit = calculate_surplus_deficit()
            if sum(surplus.values()) == 0:
                break
            # If no decom moves, balance regular hosts
            if not moved:
                surplus_hosts = [(s, h) for h, s in surplus.items() if s > 0]
                deficit_hosts = [(d, h) for h, d in deficit.items() if d > 0]
                surplus_hosts.sort(reverse=True)
                deficit_hosts.sort(reverse=True)

                for _, source_id in surplus_hosts:
                    source = working_conf[source_id]
                    for _, target_id in deficit_hosts:
                        target = working_conf[target_id]

                        for segment in sorted(list(source.primary_segments), key=lambda x: x.contentid, reverse=True):
                            if self._is_move_valid(segment, target, working_conf):
                                source.remove_primary(segment)
                                target.add_primary(segment)
                                moved = True
                                break
                        if moved:
                            break
                    if moved:
                        break

            if not moved:
                break

            # Phase 2: Deterministic Mirror Placement
        for _, host in working_conf.items():
            host.mirror_segments = set()

        active_hosts = [(h.hostname, h.address) for _, h in working_conf.items()
                        if h.status != HostStatus.DECOMMISSIONING]
        active_hosts.sort()  # ensure consistent ordering

        if self.target_strategy == MirrorStrategy.GROUPED:
            # For each host, place all its primaries' mirrors on the next host
            for i, host_id in enumerate(active_hosts):
                next_host_id = active_hosts[(i + 1) % len(active_hosts)]
                primaries = working_conf[host_id].primary_segments
                for segment in primaries:
                    mirror = self.initial_mirror_mapping[segment.contentid]
                    working_conf[next_host_id].add_mirror(
                        SegmentId(mirror.dbid, mirror.content))

        elif self.target_strategy == MirrorStrategy.SPREAD:
            # For each host, distribute mirrors across other hosts
            for host_id in active_hosts:
                other_hosts = [h for h in active_hosts if h != host_id]
                primaries = list(working_conf[host_id].primary_segments)

                # Distribute mirrors evenly across other hosts
                for i, segment in enumerate(primaries):
                    mirror_host = other_hosts[i % len(other_hosts)]
                    mirror = self.initial_mirror_mapping[segment.contentid]
                    working_conf[mirror_host].add_mirror(
                        SegmentId(mirror.dbid, mirror.content))

        return working_conf

    def getPlan(self, finalState: ClusterState) -> Plan:
        in_conf = self._get_working_conf()
        moves, roles = self.get_moves_between_states(in_conf, finalState)
        plan = Plan()
        plan.moves = moves
        plan.in_conf = in_conf
        plan.out_conf = finalState
        plan.segmentMap = self.initialSegmentMap
        plan.roles = roles
        return plan

    def get_moves_between_states(self, state1: ClusterState, state2: ClusterState):
        moves = []
        roles = {}
        primary_map1 = self._getCurrentSegmentMapping(state1)
        mirror_map1 = self._getCurrentMirrorMapping(state1)
        primary_map2 = self._getCurrentSegmentMapping(state2)
        mirror_map2 = self._getCurrentMirrorMapping(state2)

        # Find primary segment moves
        for contentid, target_location in primary_map2.items():
            current_location = primary_map1.get(contentid)
            if current_location and current_location != target_location:
            #important notice. srcHost and dstHost are different
            # objects even if they are describing the same host. 
            # srcHost and dstHost are taken from state1 and state2 correspondingly
                source_host = state1[current_location]
                target_host = state2[target_location]
                segid = next(seg for seg in source_host.primary_segments
                             if seg.contentid == contentid)
                moves.append(
                    Move(segid, source_host, target_host, False, None, None))
                roles[segid] = 'p'

        # Find mirror segment moves
        for contentid, target_location in mirror_map2.items():
            current_location = mirror_map1.get(contentid)
            if current_location and current_location != target_location:
                source_host = state1[current_location]
                target_host = state2[target_location]
                segid = next(seg for seg in source_host.mirror_segments
                             if seg.contentid == contentid)
                moves.append(
                    Move(segid, source_host, target_host, True, None, None))
                roles[segid] = 'm'

        moves.sort(key=lambda m: 1 if m.is_mirror else 0)

        return moves, roles
