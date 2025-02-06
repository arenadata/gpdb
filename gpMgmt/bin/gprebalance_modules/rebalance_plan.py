import copy
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


class Plan:
    def __init__(self):
        self.in_conf = None
        self.moves = []
        self.out_conf = None
        self.roles = []
        self.number_of_moves = 0

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


class NoValidMovesError(Exception):
    def __init__(self, msg="No valid targets for selected segment"):
        super().__init__(msg)


class ClusterBalancer():
    def __init__(self, initial_conf: ClusterState,
                 new_hosts: tuple[Set[Host], Set[Host]],
                 segmentMap: Dict[SegmentId, Segment],
                 segmentSizes: Dict[int, int],
                 target_load: int,
                 strat: MirrorStrategy
                 ):
        self.in_conf = initial_conf
        self.new_hosts, self.replacement_hosts = new_hosts
        self.initialSegmentMap = segmentMap
        self.target_load = target_load
        self.segmentSizes = segmentSizes
        self.target_strategy = strat
        self.initial_mirror_mapping = {}
        for k, v in segmentMap.items():
            if v.isSegmentMirror():
                self.initial_mirror_mapping[k.contentid] = v

    def get_working_conf(self) -> ClusterState:
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
                    working_conf[(h.replacement_for.hostname, h.replacement_for.address)].status == HostStatus.DECOMMISSIONING)
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

    def _calculate_mirror_strategy_penalty(self, state: ClusterState) -> float:
        penalty = 0.0
        mirror_mapping = self._getCurrentMirrorMapping(state)
        if self.target_strategy == MirrorStrategy.GROUPED:
            for _, host in state.items():
                if host.status == HostStatus.DECOMMISSIONING:
                    continue
                mirror_hosts = set()
                for s in host.primary_segments:
                    mirror_hosts.add(mirror_mapping[s.contentid])
                if len(mirror_hosts) > 1:
                    penalty += 10 * (len(mirror_hosts) - 1)
                elif len(mirror_hosts & set([(host.hostname, host.address)])) > 0:
                    penalty += 20
                elif len(host.mirror_segments) == 0:
                    penalty += 20
        elif self.target_strategy == MirrorStrategy.SPREAD:
            for _, host in state.items():
                if host.status == HostStatus.DECOMMISSIONING:
                    continue
                mirror_hosts_counts: Dict[Host, int] = defaultdict(int)
                for s in host.primary_segments:
                    mirror_hosts_counts[mirror_mapping[s.contentid]] += 1
                for t_h, count in mirror_hosts_counts.items():
                    if t_h == (host.hostname, host.address):
                        penalty += 200 * count
                    elif count > 1:
                        penalty += 100 * (count - 1)
        return penalty

    def _move_from_decomissioning(self, state: ClusterState):
        if self.replacement_hosts:
            for rep in self.replacement_hosts:
                # Host from which we must move all segments
                decom_host_id = (rep.replacement_for.hostname,
                                 rep.replacement_for.address)
                replacement_host_id = (rep.hostname, rep.address)
                primaries = state[decom_host_id].primary_segments
                mirrors = state[decom_host_id].mirror_segments
                for segid in primaries:
                    state[replacement_host_id].add_primary(segid)
                for segid in mirrors:
                    state[replacement_host_id].add_mirror(segid)
                rep.replacement_for.primaries = []
                rep.replacement_for.mirrors = []

    def balance(self) -> ClusterState:
        working_conf = self.get_working_conf()
        # First, check if there are hosts explicitly marked for replacement

        conf = self.greedy_balance(working_conf)

        return conf

    def _is_move_valid_greedy(self, segmentid: SegmentId, is_mirror: bool, target_host: Host, state: ClusterState) -> bool:
        segment_locations = self._getCurrentSegmentMapping(state)
        mirror_locations = self._getCurrentMirrorMapping(state)

        if is_mirror:
            current_host = mirror_locations[segmentid.contentid]
        else:
            current_host = segment_locations[segmentid.contentid]

        if current_host == (target_host.hostname, target_host.address):
            return False

        # Check target host status
        if target_host.status == HostStatus.DECOMMISSIONING:
            return False

        # For primary moves, only check capacity
        if not is_mirror and state[current_host].status != HostStatus.DECOMMISSIONING:
            return len(target_host.primary_segments) + 1 <= self.target_load

        # For mirror moves, check mirror strategy constraints
        if is_mirror and self.target_strategy != MirrorStrategy.MIRRORLESS:
            primary_host = segment_locations[segmentid.contentid]

            # Never allow mirror on same host as primary
            if primary_host == (target_host.hostname, target_host.address):
                return False

            if self.target_strategy == MirrorStrategy.SPREAD:
                # Check spread strategy constraints only for mirror moves
                mirror_hosts = set()
                for ps in state[primary_host].primary_segments:
                    mirror_hosts.add(mirror_locations[ps.contentid])
                if (target_host.hostname, target_host.address) in mirror_hosts:
                    return False
            else:
                mirrors_by_host = defaultdict(int)
                for prim in state[primary_host].primary_segments:
                    if mirror_locations[prim.contentid] != current_host and \
                            state[mirror_locations[prim.contentid]].status != HostStatus.DECOMMISSIONING:
                        mirrors_by_host[mirror_locations[prim.contentid]] += 1
                mirror_hosts = sorted(mirrors_by_host.keys(),
                                      key=lambda h: mirrors_by_host[h],
                                      reverse=True)
                if mirror_hosts and (target_host.hostname, target_host.address) != mirror_hosts[0]:
                    return False
        return True

    def greedy_balance(self, working_conf: ClusterState) -> ClusterState:
        working_conf = self.get_working_conf()

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

                for segment in list(source.primary_segments):
                    for target in potential_targets:
                        if self._is_move_valid_greedy(segment, False, target, working_conf):
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

                        for segment in list(source.primary_segments):
                            if self._is_move_valid_greedy(segment, False, target, working_conf):
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

        # Phase 2: Fix Mirroring Violations
        def mirror_violation_fixed():
            return self._calculate_mirror_strategy_penalty(working_conf) < 1e-3
        while not mirror_violation_fixed():

            for _, host in working_conf.items():
                segment_locations = self._getCurrentSegmentMapping(
                    working_conf)
                mirror_locations = self._getCurrentMirrorMapping(working_conf)

                # Check and fix mirror violations
                for mirror in list(host.mirror_segments):

                    # Check if mirror is on same host or violates strategy
                    current_primary_host = segment_locations[mirror.contentid]
                    needs_move = False

                    if current_primary_host == (host.hostname, host.address):
                        needs_move = True
                    elif host.status == HostStatus.DECOMMISSIONING:
                        needs_move = True
                    elif self.target_strategy == MirrorStrategy.SPREAD:
                        mirror_count = sum(1 for p in working_conf[current_primary_host].primary_segments
                                           if mirror_locations[p.contentid] == (host.hostname, host.address))
                        if mirror_count > 1:
                            needs_move = True
                    elif self.target_strategy == MirrorStrategy.GROUPED:
                        mirrors_by_primary_host = defaultdict(int)
                        for mir in host.mirror_segments:
                            if segment_locations[mir.contentid] == (host.hostname, host.address):
                                continue
                            mirrors_by_primary_host[segment_locations[mir.contentid]] += 1
                        primary_hosts = sorted(mirrors_by_primary_host.keys(),
                                               key=lambda h: mirrors_by_primary_host[h],
                                               reverse=True)
                        # move if not in largest group
                        if mirrors_by_primary_host[current_primary_host] < mirrors_by_primary_host[primary_hosts[0]] or\
                                len(primary_hosts) > 1:
                            needs_move = True

                    if needs_move:
                        # Find valid target for mirror
                        potential_targets = sorted(
                            [h for _, h in working_conf.items()
                             if h.status != HostStatus.DECOMMISSIONING],
                            key=lambda h: len(h.mirror_segments)
                        )

                        for target in potential_targets:
                            if self._is_move_valid_greedy(mirror, True, target, working_conf):
                                host.remove_mirror(
                                    mirror)
                                target.add_mirror(mirror)
                                break

        return working_conf

    def getPlan(self, finalState: ClusterState) -> Plan:
        in_conf = self.get_working_conf()
        moves, roles = self.get_moves_between_states(in_conf, finalState)
        plan = Plan()
        plan.moves = moves
        plan.number_of_moves = len(moves)
        plan.in_conf = in_conf
        plan.out_conf = finalState
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
                source_host = state1[current_location]
                target_host = state1[target_location]
                segid = next(seg for seg in source_host.primary_segments
                             if seg.contentid == contentid)
                moves.append(Move(segid, source_host, target_host))
                roles[segid] = 'p'

        # Find mirror segment moves
        for contentid, target_location in mirror_map2.items():
            current_location = mirror_map1.get(contentid)
            if current_location and current_location != target_location:
                source_host = state1[current_location]
                target_host = state1[target_location]
                segid = next(seg for seg in source_host.mirror_segments
                             if seg.contentid == contentid)
                moves.append(Move(segid, source_host, target_host))
                roles[segid] = 'm'

        def is_primary_move(move):
            return move.segid in state1[move.srcHost.hostname, move.srcHost.address].primary_segments

        moves.sort(key=lambda m: 0 if is_primary_move(m) else 1)

        return moves, roles
