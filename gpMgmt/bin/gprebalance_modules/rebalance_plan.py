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

        self.mirroring_violation_prob = 0.0
        self.mirror_choosing_prob = 0.3
        self.max_iteratons = len(self.in_conf) * 100

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

    def _calculate_energy(self, state:  ClusterState) -> tuple[float, float, float]:
        imbalance_penalty = self._calculate_imbalance_penalty(state)
        mirror_penalty = self._calculate_mirror_strategy_penalty(state)
        moves_penalty = self._calculate_moves_penalty(state)
        return (imbalance_penalty + mirror_penalty + moves_penalty, imbalance_penalty, mirror_penalty)

    def _calculate_imbalance_penalty(self, state: ClusterState) -> float:
        penalty = 0.0
        for _, host in state.items():
            load = len(host.primary_segments)
            deviation = abs(load - self.target_load)
            if host.status == HostStatus.DECOMMISSIONING and (host.primary_segments or host.mirror_segments):
                penalty += (len(host.primary_segments) +
                            len(host.mirror_segments)) * 1000
            else:
                penalty += deviation ** 2 * 100
        return penalty

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
                    penalty += 100 * (len(mirror_hosts) - 1)
                elif len(mirror_hosts & set([(host.hostname, host.address)])) > 0:
                    penalty += 200
                elif len(host.mirror_segments) == 0:
                    penalty += 200
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

    def _calculate_moves_penalty(self, state: ClusterState) -> float:
        penalty = 0.0
        s = 0
        for k, v in state.items():
            for sid in v.primary_segments:
                seg = self.initialSegmentMap[sid]
                if seg.hostname != v.hostname \
                        and self.in_conf[(seg.hostname, seg.address)].status != HostStatus.DECOMMISSIONING:
                    s += 1
            for sid in v.mirror_segments:
                seg = self.initialSegmentMap[sid]
                if seg.hostname != v.hostname \
                        and self.in_conf[(seg.hostname, seg.address)].status != HostStatus.DECOMMISSIONING:
                    s += 1

        penalty += s * 10
        return penalty

    def _is_move_valid(self, segmentid: SegmentId, is_mirror: bool, target_host: Host, state: ClusterState) -> bool:
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

        # Check if move would exceed host capacity
        if not is_mirror and len(target_host.primary_segments) + 1 > self.target_load:
            return False

        # Mirror placement constraints
        if self.target_strategy != MirrorStrategy.MIRRORLESS:
            pair_host = None
            if is_mirror:
                pair_host = segment_locations[segmentid.contentid]
            else:
                pair_host = mirror_locations[segmentid.contentid]

            if self.target_strategy == MirrorStrategy.SPREAD:
                mirror_hosts = set()

                if not is_mirror:
                    for ps in target_host.primary_segments:
                        mirror_hosts.add(mirror_locations[ps.contentid])
                    if pair_host in mirror_hosts and random.random() >= self.mirroring_violation_prob:
                        return False
                else:
                    for ps in state[pair_host].primary_segments:
                        mirror_hosts.add(mirror_locations[ps.contentid])
                    if (target_host.hostname, target_host.address) in mirror_hosts and random.random() >= self.mirroring_violation_prob:
                        return False
            else:
                if is_mirror:
                    # Count mirrors by primary host on target
                    mirrors_by_primary_host = defaultdict(int)
                    for mir in target_host.mirror_segments:
                        mir_primary = segment_locations[mir.contentid]
                        if mir_primary != (target_host.hostname, target_host.address):
                            mirrors_by_primary_host[mir_primary] += 1

                    # If target has mirrors, prefer the host that has most mirrors from same primary
                    if mirrors_by_primary_host:
                        main_primary = max(
                            mirrors_by_primary_host.items(), key=lambda x: x[1])[0]
                        if current_host != main_primary and random.random() >= self.mirroring_violation_prob:
                            return False

        return True

    def _select_candidate_move(self, state: ClusterState) -> Move:
        """Select random valid move considering both primaries and mirrors"""
        decom_hosts = [h for _, h in state.items() if h.status ==
                       HostStatus.DECOMMISSIONING]
        is_mirror = False
        if len(decom_hosts) > 0:
            host = random.choice(decom_hosts)
            if host.primary_segments or host.mirror_segments:
                # Get both primary and mirror segments from decom host
                segmentid = None
                if host.primary_segments:
                    segmentid = random.choice(list(host.primary_segments))

                # Also consider the mirror if exists
                if host.mirror_segments and not host.primary_segments:
                    # 50% chance to move mirror instead
                    segmentid = random.choice(list(host.mirror_segments))
                    is_mirror = True

                valid_targets = [
                    h for _, h in state.items()
                    if h.status != HostStatus.DECOMMISSIONING and segmentid
                    and self._is_move_valid(segmentid, is_mirror, h, state)
                ]
                if valid_targets:
                    return Move(segmentid, host, random.choice(valid_targets))

        # Regular balancing move
        active_hosts = [h for _, h in state.items() if h.status !=
                        HostStatus.DECOMMISSIONING]
        if not active_hosts:
            raise NoValidMovesError("No active hosts available")

        host_weights = []
        for host in active_hosts:
            load_factor = len(host.primary_segments)
            # Weight based on how overloaded the host is
            weight = max(0.1, (load_factor - self.target_load) /
                         self.target_load)
            host_weights.append(weight)

        total_weight = sum(host_weights)
        if total_weight <= 0:
            raise NoValidMovesError("No valid source hosts")

        # Normalize weights to probabilities
        host_probs = [w/total_weight for w in host_weights]
        source_host = random.choices(
            list(active_hosts), weights=host_probs, k=1)[0]

        if len(source_host.primary_segments) == 0:
            raise NoValidMovesError("No segments on selected host")

        # Select segment and potentially its mirror
        primary_segment = random.choice(list(source_host.primary_segments))

        # Decide whether to move primary or mirror
        selected_segment = primary_segment
        if source_host.mirror_segments and random.random() < self.mirror_choosing_prob:
            selected_segment = random.choice(list(source_host.mirror_segments))
            is_mirror = True

        valid_targets = []
        for target_host in active_hosts:
            if self._is_move_valid(selected_segment, is_mirror, target_host, state):
                valid_targets.append(target_host)

        if not valid_targets:
            return None
        target_host = random.choice(valid_targets)
        return Move(selected_segment, source_host, target_host)

    def hybrid_balance(self) -> ClusterState:
        working_conf = self.get_working_conf()
        converged = False

        current_energy, _, _ = self._calculate_energy(
            working_conf)
        best_energy = current_energy
        best_conf = copy.deepcopy(working_conf)

        no_improvement_count = 0

        for iteration in range(self.max_iteratons):
            try:
                move = self._select_candidate_move(working_conf)
                if move is None:
                    continue
                segment = move.segid
                source = move.srcHost
                target = move.dstHost
                working_conf_backup = copy.deepcopy(working_conf)
                if segment in source.primary_segments:
                    source.remove_primary(segment)
                    target.add_primary(segment)
                else:
                    source.remove_mirror(segment)
                    target.add_mirror(segment)
                new_energy, imbalance_penalty, mirror_penalty = self._calculate_energy(
                    working_conf)
                energy_delta = new_energy - current_energy
                accept = False
                if energy_delta <= 0:
                    accept = True
                elif energy_delta < current_energy * 0.8:
                    accept = random.random() < 0.8
                elif energy_delta < current_energy * 0.6:
                    accept = random.random() < 0.6
                elif energy_delta < current_energy * 0.2:
                    accept = random.random() < 0.2
                else:
                    accept = random.random() < 0.1
                if accept:
                    current_energy = new_energy
                    if current_energy < best_energy:
                        best_energy = current_energy
                        best_conf = copy.deepcopy(working_conf)
                        no_improvement_count = 0
                        self.mirroring_violation_prob = 0.0
                    else:
                        no_improvement_count += 1
                else:
                    working_conf = working_conf_backup
                    no_improvement_count += 1

                if no_improvement_count % 50 == 0 and self.mirroring_violation_prob < 0.8:
                    self.mirroring_violation_prob += 0.2
                if imbalance_penalty < 1e-3:
                    self.mirror_choosing_prob = 1.0

                if imbalance_penalty + mirror_penalty < 1e-3:
                    converged = True

                if no_improvement_count == 200:
                    break
            except NoValidMovesError:
                continue

        conf = self.greedy_balance()
        if converged and self._calculate_moves_penalty(conf) > self._calculate_moves_penalty(best_conf):
            return best_conf
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

    def greedy_balance(self) -> ClusterState:
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
