import math
import random
from typing import NewType, Set, Dict, List, Tuple
from collections import defaultdict
import time

class TimeoutException(Exception):
    """Raised when search time limit is exceeded"""
    pass

# 0 <= hostid <= n_hosts_initial
HostId = NewType('HostId', int)
# 0 <= contentid <= n_segments
ContentId = NewType('ContentId', int)
# number of moves
Cost = NewType('Cost', int)
# load
Load = NewType('Load', int)
# {'contendid' - > (primary host, mirror host)}
Solution = Dict[ContentId, Tuple[HostId, HostId]]


class GreedySolver:
    """
    Enhanced greedy solver with better initial solution quality.
    """
    
    def __init__(self, 
                 n_segments: int,
                 n_hosts_target: int,
                 n_hosts_initial: int,
                 initial_primary: List[HostId],
                 initial_mirror: List[HostId],
                 strategy: str = 'grouped',
                 run_improve = True,
                 printing = False):
        
        self.n_segments = n_segments
        self.n_hosts_target = n_hosts_target
        self.n_hosts_initial = n_hosts_initial
        self.initial_primary = initial_primary
        self.initial_mirror = initial_mirror
        self.strategy = strategy
        self.run_improve = run_improve
        self.printing = printing
        self.target_primary_load = n_segments // n_hosts_target
        self.target_load = 2 * n_segments // n_hosts_target

        self.best_primary = None
        self.best_mirror = None
        self.best_cost = None

        if self.n_hosts_target < 2:
            raise ValueError("Cannot balance to single host")
        
        if self.n_segments % self.n_hosts_target != 0:
            raise ValueError(f"Cannot evenly distribute {self.n_segments}"
                             f"segments across {self.n_hosts_target} hosts")

        if strategy == 'spread':
            if self.target_primary_load > self.n_hosts_target - 1:
                raise ValueError("Cannot follow spread mirroring strategy")
    
    def solve(self) -> Tuple[Solution, Cost]:
        """
        Multi-phase greedy construction with cost awareness.
        """
        # Phase 1: Balance primaries (minimize primary movements)
        self.best_primary = self._balance_primaries()
    
        # Phase 2: Assign mirrors (minimize mirror movements)
        self.best_mirror = self._assign_mirrors(self.best_primary)

        if self.run_improve:
            try:
                alns = ALNS(self, max_iterations=1000, timeout=60.0)
                self.best_primary, self.best_mirror =\
                    alns.optimize(self.best_primary, self.best_mirror)
            except TimeoutException:
                if self.printing:
                    print(f"\n Time limit reached ({60}s)")
                   
        solution = {i: (self.best_primary[i], self.best_mirror[i]) for i in range(self.n_segments)}
        
        cost = self._calculate_cost(self.best_primary, self.best_mirror)

        assert(self._validate_solution(solution))

        return solution, cost
    
    def _balance_primaries(self) -> List[HostId]:
        """Balance primaries with cost-aware assignment"""
        primary = [-1] * self.n_segments
        
        # Calculate current loads
        initial_load = [0] * self.n_hosts_initial
        for p in self.initial_primary:
            if p < self.n_hosts_target:
                initial_load[p] += 1
        
        # Sort segments by: 1) must-move first, 2) original host preference
        segment_order = []
        for i in range(self.n_segments):
            orig_host = self.initial_primary[i]
            must_move = orig_host >= self.n_hosts_target or initial_load[orig_host] > self.target_primary_load
            segment_order.append((must_move, -initial_load[orig_host], i))
        
        segment_order.sort()
        
        # Assign segments
        current_load = [0] * self.n_hosts_target
        
        for _, _, seg_id in segment_order:
            orig_host = self.initial_primary[seg_id]
            
            # Try to keep on original host if possible
            if (orig_host < self.n_hosts_target and 
                current_load[orig_host] < self.target_primary_load):
                primary[seg_id] = orig_host
                current_load[orig_host] += 1
            else:
                # Find least loaded host
                best_host = min(range(self.n_hosts_target), key=lambda h: current_load[h])
                primary[seg_id] = best_host
                current_load[best_host] += 1
        
        return primary
    
    def _assign_mirrors(self, primary: List[HostId]) -> List[HostId]:
        """
        Assign mirrors.
        """
        mirror = [-1] * self.n_segments
        mirror_load = [0] * self.n_hosts_target

        # Group segments by primary host
        groups = defaultdict(list)
        for i in range(self.n_segments):
            groups[primary[i]].append(i)

        if self.strategy == 'grouped':
            # Track assignments for swapping
            primary_to_mirror = {}  # p_host -> m_host

            # Process larger groups first for better balance
            sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)

            for p_host, segments in sorted_groups:
                group_size = len(segments)

                # Count original mirror preferences
                mirror_votes = defaultdict(int)
                for seg in segments:
                    orig_mirror = self.initial_mirror[seg]
                    if orig_mirror < self.n_hosts_target and orig_mirror != p_host:
                        mirror_votes[orig_mirror] += 1

                best_mirror = None

                # Priority 1: Most voted original mirror (if has capacity)
                if mirror_votes:
                    candidates_with_capacity = [
                        (votes, host) for host, votes in mirror_votes.items()
                        if mirror_load[host] + group_size <= self.target_primary_load
                    ]
                    if candidates_with_capacity:
                        best_mirror = max(candidates_with_capacity, key=lambda x: x[0])[1]

                # Priority 2: Least loaded host (if no original preference fits)
                if best_mirror is None:
                    available_hosts = [
                        h for h in range(self.n_hosts_target)
                        if h != p_host and mirror_load[h] + group_size <= self.target_primary_load
                    ]
                    if available_hosts:
                        best_mirror = min(available_hosts, key=lambda h: mirror_load[h])

                # Priority 3: DEADLOCK - Try swapping with already assigned group
                if best_mirror is None:
                    best_mirror = self._swap_to_resolve_deadlock(
                        p_host, group_size, mirror_load, primary_to_mirror, groups, mirror)

                # Assign all segments to chosen mirror
                for seg in segments:
                    mirror[seg] = best_mirror
                    mirror_load[best_mirror] += 1

                primary_to_mirror[p_host] = best_mirror

        elif self.strategy == 'spread':
            used_in_group = defaultdict(set)

            # Phase 1: Try to assign segments to their original mirrors
            unassigned = []

            for seg in range(self.n_segments):
                p_host = primary[seg]
                orig_mirror = self.initial_mirror[seg]

                # Check if original mirror is valid and available
                can_use_original = (
                    orig_mirror < self.n_hosts_target and
                    orig_mirror != p_host and
                    orig_mirror not in used_in_group[p_host] and
                    mirror_load[orig_mirror] < self.target_primary_load
                )

                if can_use_original:
                    mirror[seg] = orig_mirror
                    mirror_load[orig_mirror] += 1
                    used_in_group[p_host].add(orig_mirror)
                else:
                    unassigned.append(seg)

            # Phase 2: Assign remaining segments with load balancing
            for seg in unassigned:
                p_host = primary[seg]
                orig_mirror = self.initial_mirror[seg]
                best_host = None

                # Priority 2: Find available hosts (not primary, not used in group, has capacity)
                if best_host is None:
                    available = [
                        h for h in range(self.n_hosts_target)
                        if (h != p_host and
                            h not in used_in_group[p_host] and
                            mirror_load[h] < self.target_primary_load)
                    ]
                    if available:
                        best_host = min(available, key=lambda h: mirror_load[h])

                # Priority 3: DEADLOCK Try swapping - move another segment to underloaded host
                if best_host is None:
                    hosts_with_capacity = [
                        h for h in range(self.n_hosts_target)
                        if mirror_load[h] < self.target_primary_load
                    ]
                    if hosts_with_capacity:
                        # Find hosts we could use (not in our group, at any load level)
                        candidate_hosts = [
                            h for h in range(self.n_hosts_target)
                            if h != p_host and h not in used_in_group[p_host]
                        ]
                        # Try to find a segment using one of these hosts that can move to p_host
                        for candidate_host in candidate_hosts:
                            # Find segments currently using candidate_host as mirror
                            for other_seg in range(self.n_segments):
                                if mirror[other_seg] != candidate_host:
                                    continue
                                
                                other_p_host = primary[other_seg]
                                # Check if other_seg can use p_host as mirror
                                for dest_host in hosts_with_capacity:
                                    # Check if other_seg can move to dest_host
                                    can_swap = (
                                        dest_host != other_p_host and  # Not other's primary
                                        dest_host not in used_in_group[other_p_host]  # Not in other's group
                                    )

                                    if can_swap:
                                        # Perform the swap
                                        # Remove other_seg from candidate_host
                                        used_in_group[other_p_host].remove(candidate_host)
                                        mirror_load[candidate_host] -= 1

                                        # Move other_seg to dest_host
                                        mirror[other_seg] = dest_host
                                        used_in_group[other_p_host].add(dest_host)
                                        mirror_load[dest_host] += 1

                                        # Current segment takes candidate_host
                                        best_host = candidate_host
                                        break
                                
                                if best_host is not None:
                                    break
                        
                            if best_host is not None:
                                    break

                mirror[seg] = best_host
                mirror_load[best_host] += 1
                used_in_group[p_host].add(best_host)
   
        else:  # 'any' strategy
            for seg in range(self.n_segments):
                p_host = primary[seg]
                orig_mirror = self.initial_mirror[seg]

                # Get available hosts (not primary, has capacity)
                available = [
                    h for h in range(self.n_hosts_target)
                    if h != p_host and mirror_load[h] < self.target_primary_load
                ]

                # Priority 1: Original mirror if available
                if orig_mirror in available:
                    best_host = orig_mirror
                else:
                    # Priority 2: Least loaded
                    best_host = min(available, key=lambda h: mirror_load[h])

                mirror[seg] = best_host
                mirror_load[best_host] += 1
        return mirror
    
    def _swap_to_resolve_deadlock(self, 
                                blocked_p_host: HostId,
                                blocked_size: int, # size of the primary group at p_host
                                mirror_load: List[Load],
                                primary_to_mirror: Dict[HostId, HostId],
                                groups: Dict[HostId, List[ContentId]],
                                mirror: List[HostId]):
        """
        Resolve deadlock by moving an existing group to a different mirror.
        The alternative mirror can be:
        1. Any mirror with capacity
        2. The blocked_p_host itself (circular swap)
        """

        for other_p_host, current_mirror in primary_to_mirror.items():
            if other_p_host == blocked_p_host:
                continue
            
            other_size = len(groups[other_p_host])

            # Find alternative mirror (INCLUDING the blocked host!)
            alternative_mirror = next(
                (h for h in range(self.n_hosts_target)
                 if h != other_p_host and  # Can't be other's primary
                    mirror_load[h] + other_size <= self.target_primary_load),
                None
            )

            if alternative_mirror is None:
                continue 
            
            space_after_move = mirror_load[current_mirror] - other_size
            if space_after_move + blocked_size > self.target_primary_load:
                continue
            
            # Swap: move other_p_host to alternative_mirror
            for seg in groups[other_p_host]:
                mirror[seg] = alternative_mirror

            mirror_load[current_mirror] -= other_size
            mirror_load[alternative_mirror] += other_size
            primary_to_mirror[other_p_host] = alternative_mirror

            return current_mirror

        return None

    def _calculate_cost(self, primary: List[HostId], mirror: List[HostId]) -> int:
        """Calculate movement cost"""
        return sum(
            (1 if primary[i] != self.initial_primary[i] else 0) +
            (1 if mirror[i] != self.initial_mirror[i] else 0)
            for i in range(self.n_segments)
        )
    
    def _validate_solution(self, solution: Solution) -> bool:
        """Validate that solution satisfies all constraints"""
        
        # Check 1: All segments assigned
        if len(solution) != self.n_segments:
            return False
        
        # Check 2: Primary host != Mirror host 
        for i, (p, m) in solution.items():
            if p == m:
                return False
        
        # Check 3: Load balance
        load = [0] * (self.n_hosts_target)
        for i, (p, m) in solution.items():
            load[p] += 1
            load[m] += 1
        
        for h in range(self.n_hosts_target):
            if load[h] != self.target_primary_load * 2 :
                return False
        
        # Check 4: Strategy constraints
        segments_by_host = defaultdict(list)
        for i, (p, m) in solution.items():
            segments_by_host[p].append((i, m))
        
        for _, segs in segments_by_host.items():
            if len(segs) < 2:
                continue
            
            mirror_hsts = [r for (i, r) in segs]
            
            if self.strategy == 'grouped':
                if len(set(mirror_hsts)) > 1:
                    return False
            elif self.strategy == 'spread':
                if len(set(mirror_hsts)) != len(mirror_hsts):
                    return False
        
        return True

class ALNS:
    """
    Adaptive Large Neighborhood Search for Greengage segments rebalancing.
    """
    def __init__(self,
                 solver: GreedySolver,
                 max_iterations: int = 1000,
                 timeout: float = 60.0):
        self.solver = solver
        self.n_segments = solver.n_segments
        self.n_hosts = solver.n_hosts_target
        self.strategy = solver.strategy
        self.initial_primary = solver.initial_primary
        self.initial_mirror = solver.initial_mirror
        self.target_primary_load = solver.target_primary_load
        self.printing = solver.printing
    
        self.start_time = None
        self.timeout = timeout
        self.max_iterations = max_iterations
        self._host_set = set(range(self.n_hosts))

    
    def optimize(self, 
                 primary: List[HostId],
                 mirror: List[HostId]) -> Tuple[List[HostId], List[HostId]]:
        """
        ALNS.
        Uses all destroy/repair strategies with SA acceptance.
        """
        self.start_time = time.time()
        current_primary = primary[:]
        current_mirror = mirror[:]
        current_cost = self.solver._calculate_cost(primary, mirror)
        
        best_primary = primary[:]
        best_mirror = mirror[:]
        best_cost = current_cost
        
        if self.solver.printing:
            print(f"Initial cost: {best_cost}")
        
        stagnation_count = 0
        last_improvement_iter = 0
        
        for iteration in range(self.max_iterations):
            if self._timeout_reached():
                raise TimeoutException
            
            # Temperature schedule
            temperature = 1.0 * (0.95 ** iteration)
            temperature = max(0.01, temperature)
            
            # Adaptive destroy size based on progress
            if iteration - last_improvement_iter > 20:
                # Stuck: try larger neighborhoods
                destroy_size = random.uniform(0.20, 0.40)
            elif iteration < self.max_iterations * 0.3:
                # Early phase: explore
                destroy_size = random.uniform(0.15, 0.30)
            else:
                # Late phase: intensify
                destroy_size = random.uniform(0.10, 0.20)
            
            # Select destroy method
            destroy_method = self._select_destroy_method(iteration, stagnation_count)
            
            # Apply destroy
            if destroy_method == 'group_destroy' and self.strategy == 'grouped':
                destroyed = self._destroy_primary_groups(current_primary, current_mirror, destroy_size)
            elif destroy_method == 'bad_segments':
                destroyed = self._destroy_bad_segments(current_primary, current_mirror, destroy_size)
            elif destroy_method == 'shaw_removal':
                destroyed = self._shaw_removal(current_primary, current_mirror, destroy_size)
            else:  # random
                destroyed = self._destroy_random(current_primary, destroy_size)
            
            # Select repair method
            if random.random() < 0.7:  # Favor regret (faster)
                new_primary, new_mirror = self._repair_with_regret(
                    current_primary, current_mirror, destroyed)
            else:
                new_primary, new_mirror = self._repair_optimal(
                    current_primary, current_mirror, destroyed)
            
            # Local search (every 5 iterations for grouped)
            if self.strategy == 'grouped' and iteration % 5 == 0:
                new_primary, new_mirror = self._local_mirror_swap(
                    new_primary, new_mirror)
            
            new_cost = self.solver._calculate_cost(new_primary, new_mirror)
            
            # Validate
            if not self._is_valid(new_primary, new_mirror):
                stagnation_count += 1
                continue
            
            # SA acceptance
            accept_prob = self._acceptance_probability(
                current_cost, new_cost, temperature)
            
            if random.random() < accept_prob:
                # Accept move
                current_primary = new_primary
                current_mirror = new_mirror
                current_cost = new_cost
                stagnation_count = 0
                
                if new_cost < best_cost:
                    best_primary = new_primary[:]
                    best_mirror = new_mirror[:]
                    best_cost = new_cost
                    last_improvement_iter = iteration
                    
                    if self.printing:
                        print(f"Iter {iteration} ({destroy_method}): NEW BEST = {best_cost}")
            else:
                stagnation_count += 1
            
            # Restart from best if stuck
            if stagnation_count > 30:
                if self.printing:
                    print(f"Iter {iteration}: Restarting from best (cost={best_cost})")
                current_primary = best_primary[:]
                current_mirror = best_mirror[:]
                current_cost = best_cost
                stagnation_count = 0
        
        if self.printing:
            print(f"Final cost: {best_cost}")
        
        return best_primary, best_mirror
    
    def _select_destroy_method(self, iteration: int, stagnation: int) -> str:
        """
        Select destroy method based on search state.
        """
        if stagnation > 15:
            # Stuck: use aggressive methods
            return random.choice(['group_destroy', 'random_segments'])
        
        if self.strategy == 'grouped':
            # For grouped, favor group-based destroy
            return random.choices(
                ['group_destroy', 'bad_segments', 'shaw_removal', 'random_segments'],
                weights=[0.4, 0.3, 0.2, 0.1]
            )[0]
        else:
            # For spread, favor segment-based methods
            return random.choices(
                ['bad_segments', 'shaw_removal', 'random_segments'],
                weights=[0.4, 0.4, 0.2]
            )[0]
    
    def _destroy_random(self, primary: List[HostId], destroy_size: float) -> Set[ContentId]:
        """Destroy random segments."""
        n_destroy = max(1, int(self.n_segments * destroy_size))
        return set(random.sample(range(self.n_segments), n_destroy))
    
    def _destroy_primary_groups(self, primary: List[HostId], mirror: List[HostId],
                               destroy_size: float) -> Set[ContentId]:
        """
        Destroy complete primary groups (for grouped strategy).
        Prioritize groups with many moved mirrors.
        """
        # Build groups
        groups = defaultdict(list)
        for seg in range(self.n_segments):
            groups[primary[seg]].append(seg)
        
        # Score each primary by "badness" (how many mirrors deviate from original)
        primary_badness = {}
        for p_host, segments in groups.items():
            moved_mirrors = sum(1 for seg in segments 
                              if mirror[seg] != self.initial_mirror[seg])
            moved_primaries = sum(1 for seg in segments 
                                if primary[seg] != self.initial_primary[seg])
            
            # Normalized badness score
            total_moved = moved_mirrors + moved_primaries
            badness = total_moved / len(segments) if segments else 0
            primary_badness[p_host] = badness
        
        # Calculate how many segments to destroy
        n_destroy = max(1, int(self.n_segments * destroy_size))
        
        # Select primary hosts probabilistically weighted by badness
        primaries = list(groups.keys())
        
        # Add small constant to ensure exploration
        weights = [primary_badness.get(p, 0.0) + 0.1 for p in primaries]
        
        destroyed = set()
        attempts = 0
        max_attempts = 20
        
        available_primaries = primaries[:]
        available_weights = weights[:]
        
        while len(destroyed) < n_destroy and attempts < max_attempts:
            if not available_primaries:
                break
            
            # Sample a primary weighted by badness
            p_host = random.choices(available_primaries, weights=available_weights)[0]
            
            # Add all its segments
            destroyed.update(groups[p_host])
            
            # Remove from further selection
            idx = available_primaries.index(p_host)
            available_primaries.pop(idx)
            available_weights.pop(idx)
            
            attempts += 1
        
        # If we got too many, trim to size (take random subset)
        if len(destroyed) > n_destroy:
            destroyed = set(random.sample(list(destroyed), n_destroy))
        
        return destroyed
    
    def _destroy_bad_segments(self, primary: List[HostId], mirror: List[HostId],
                             destroy_size: float) -> Set[ContentId]:
        """
        Destroy segments that differ from initial placement.
        Combines cost-awareness with relatedness.
        """
        n_destroy = max(1, int(self.n_segments * destroy_size))
        
        # Score segments by badness
        bad_segments = []
        for seg in range(self.n_segments):
            badness = 0
            if primary[seg] != self.initial_primary[seg]:
                badness += 1
            if mirror[seg] != self.initial_mirror[seg]:
                badness += 1
            
            if badness > 0:
                bad_segments.append((badness, seg))
        
        if not bad_segments:
            return self._destroy_random(primary, destroy_size)
        
        # Sort by badness (worst first)
        bad_segments.sort(reverse=True)
        
        # Take top candidates
        candidates = [seg for _, seg in bad_segments[:max(1, len(bad_segments)//2)]]
        
        destroyed = set()
        
        # Start with worst segments
        for _, seg in bad_segments[:min(n_destroy, len(bad_segments))]:
            destroyed.add(seg)
        
        # Add related segments to reach quota (Shaw-style)
        if len(destroyed) < n_destroy:
            seed_segments = list(destroyed)
            
            for seed_seg in seed_segments:
                if len(destroyed) >= n_destroy:
                    break
                
                # Find related segment
                for seg in range(self.n_segments):
                    if seg in destroyed:
                        continue
                    
                    if (primary[seg] == primary[seed_seg] or 
                        mirror[seg] == mirror[seed_seg]):
                        destroyed.add(seg)
                        
                        if len(destroyed) >= n_destroy:
                            break
        
        return destroyed
    
    def _shaw_removal(self, primary: List[HostId], mirror: List[HostId],
                     destroy_size: float) -> Set[ContentId]:
        """
        Remove related segments (same primary host or mirror host).
        """
        n_destroy = max(1, int(self.n_segments * destroy_size))
        
        # Start with random seed
        seed_seg = random.randint(0, self.n_segments - 1)
        destroyed = {seed_seg}
        
        # Calculate relatedness scores
        relatedness = []
        for seg in range(self.n_segments):
            if seg == seed_seg:
                continue
            
            score = 0
            if primary[seg] == primary[seed_seg]:
                score += 2
            if mirror[seg] == mirror[seed_seg]:
                score += 1
            
            relatedness.append((score, seg))
        
        # Sort by relatedness
        relatedness.sort(reverse=True)
        
        # Take most related
        for _, seg in relatedness[:n_destroy-1]:
            destroyed.add(seg)
        
        return destroyed
    
    # REPAIR

    def _build_mirror_groups_cache(self, primary: List[HostId], mirror: List[HostId]) -> Dict[HostId, Set[HostId]]:
        """
        Build cache: primary_host -> set of mirror hosts used.
        """
        cache = defaultdict(set)
        
        for seg in range(self.n_segments):
            # Only assigned segments
            if mirror[seg] != -1:
                cache[primary[seg]].add(mirror[seg])
        
        return cache
    
    def _get_valid_mirrors(self, primary_host: HostId,
                                  mirror_cache: Dict[HostId, Set[HostId]]) -> Set[HostId]:
        """
        Get valid mirror hosts using pre-computed cache.
        """
        
        if self.strategy == 'grouped':
            # Check cache for existing mirrors on this primary_host
            existing = mirror_cache.get(primary_host)
            
            if existing:
                # Must use same mirror (return as set)
                return existing.copy()
            else:
                # Any host except primary_host
                return self._host_set - {primary_host}
        
        elif self.strategy == 'spread':
            # Exclude primary_host and already used mirrors
            used = mirror_cache.get(primary_host, set())
            return self._host_set - used - {primary_host}
        
        else:  # 'any'
            return self._host_set - {primary_host}

    def _repair_with_regret(self, primary: List[HostId], mirror: List[HostId],
                        destroyed: Set[ContentId]) -> Tuple[List[HostId], List[HostId]]:
        """
        Regret-based repair.
        """
        import heapq

        new_primary = primary[:]
        new_mirror = mirror[:]

        if not destroyed:
            return new_primary, new_mirror

        # Clear destroyed segments
        for seg in destroyed:
            new_primary[seg] = -1
            new_mirror[seg] = -1

        # Build mirror cache ONCE
        mirror_cache = self._build_mirror_groups_cache(new_primary, new_mirror)

        # Capacity tracking (arrays, not dict lookups)
        primary_capacity = [self.target_primary_load] * self.n_hosts
        mirror_capacity = [self.target_primary_load] * self.n_hosts

        for seg in range(self.n_segments):
            if seg not in destroyed and new_primary[seg] != -1:
                primary_capacity[new_primary[seg]] -= 1
                mirror_capacity[new_mirror[seg]] -= 1

        # Pre-compute segment options ONCE
        # For each segment, store (regret, best_options_list)
        segment_data = {}

        # Get top-K primaries by capacity
        K_PRIMARY = min(10, self.n_hosts)

        primaries_sorted = sorted(
            range(self.n_hosts),
            key=lambda h: primary_capacity[h],
            reverse=True
        )[:K_PRIMARY]

        # Filter by actual capacity
        primaries_with_capacity = [p for p in primaries_sorted 
                                   if primary_capacity[p] > 0]

        if not primaries_with_capacity:
            return new_primary, new_mirror  # Nothing to repair

        # Pre-compute valid mirrors for each primary (with capacity filter)
        primary_to_valid_mirrors = {}

        for p_host in primaries_with_capacity:
            all_valid = self._get_valid_mirrors(p_host, mirror_cache)

            # Filter by capacity and limit to top-K
            K_MIRROR = min(10, self.n_hosts)

            mirrors_sorted = sorted(
                all_valid - {p_host},
                key=lambda h: mirror_capacity[h],
                reverse=True
            )[:K_MIRROR]

            valid_with_cap = [m for m in mirrors_sorted if mirror_capacity[m] > 0]
            primary_to_valid_mirrors[p_host] = valid_with_cap

        # Batch compute regret for all segments

        for seg in destroyed:
            orig_p = self.initial_primary[seg]
            orig_m = self.initial_mirror[seg]

            # Try to find best 2 options for this segment
            options = []  # List of (cost, p_host, m_host)

            # Prioritize original primary if available
            candidates = []
            if orig_p in primaries_with_capacity:
                candidates.append(orig_p)

            # Add other primaries
            for p in primaries_with_capacity:
                if p != orig_p:
                    candidates.append(p)
                if len(candidates) >= K_PRIMARY:
                    break
                
            for p_host in candidates:
                valid_mirrors = primary_to_valid_mirrors.get(p_host, [])

                if not valid_mirrors:
                    continue
                
                # Calculate cost for this primary
                p_cost = 0 if p_host == orig_p else 1

                # Find best mirror
                if orig_m in valid_mirrors:
                    # Best case: original mirror available
                    best_m = orig_m
                    m_cost = 0
                    options.append((p_cost + m_cost, p_host, best_m))

                    # If cost is 0, no need to check more
                    if p_cost + m_cost == 0:
                        break
                else:
                    # Pick first valid mirror (they're sorted by capacity)
                    best_m = valid_mirrors[0]
                    m_cost = 1
                    options.append((p_cost + m_cost, p_host, best_m))

            if not options:
                continue  # Segment cannot be placed (should not happen)
            
            # Sort by cost
            options.sort()

            # Calculate regret
            if len(options) >= 2:
                regret = options[1][0] - options[0][0]
            elif len(options) == 1:
                regret = 999  # Only one option - MUST take it
            else:
                regret = 0

            best_cost, best_p, best_m = options[0]
            segment_data[seg] = (regret, best_cost, best_p, best_m)  # Store best option

        #  Use heap for fast regret sorting 
        # Build max-heap by regret (negative for max-heap behavior)
        heap = [(-regret, -best_cost, seg, best_p, best_m) 
                for seg, (regret, best_cost, best_p, best_m) 
                in segment_data.items()]

        heapq.heapify(heap)

        # Greedy insertion

        while heap:
            neg_regret, neg_cost, seg, best_p, best_m = heapq.heappop(heap)

            # Check if still valid
            if primary_capacity[best_p] > 0 and mirror_capacity[best_m] > 0:
                # Assign
                new_primary[seg] = best_p
                new_mirror[seg] = best_m

                # Update capacities
                primary_capacity[best_p] -= 1
                mirror_capacity[best_m] -= 1

                # Update mirror cache
                mirror_cache[best_p].add(best_m)

                # Update valid mirrors for affected primaries
                if self.strategy == 'spread':
                    # Only this primary's mirrors change
                    if best_p in primary_to_valid_mirrors:
                        primary_to_valid_mirrors[best_p] = [
                            m for m in primary_to_valid_mirrors[best_p]
                            if m != best_m and mirror_capacity[m] > 0
                        ]

                elif self.strategy == 'grouped':
                    # full update is expensive
                    pass
                
            else:
                # Capacity exhausted - need to recompute options
                # Fallback: find any valid placement
                orig_p = self.initial_primary[seg]
                orig_m = self.initial_mirror[seg]

                placed = False

                # Try original first
                if orig_p < self.n_hosts and primary_capacity[orig_p] > 0:
                    valid_mirrors = [m for m in primary_to_valid_mirrors.get(orig_p, [])
                                   if mirror_capacity[m] > 0]

                    if orig_m in valid_mirrors:
                        new_primary[seg] = orig_p
                        new_mirror[seg] = orig_m
                        primary_capacity[orig_p] -= 1
                        mirror_capacity[orig_m] -= 1
                        placed = True

                    elif valid_mirrors:
                        new_primary[seg] = orig_p
                        new_mirror[seg] = valid_mirrors[0]
                        primary_capacity[orig_p] -= 1
                        mirror_capacity[valid_mirrors[0]] -= 1
                        placed = True

                # Try any primary
                if not placed:
                    for p in range(self.n_hosts):
                        if primary_capacity[p] <= 0:
                            continue
                        
                        valid_mirrors = [m for m in primary_to_valid_mirrors.get(p, [])
                                       if mirror_capacity[m] > 0]

                        if valid_mirrors:
                            new_primary[seg] = p
                            new_mirror[seg] = valid_mirrors[0]
                            primary_capacity[p] -= 1
                            mirror_capacity[valid_mirrors[0]] -= 1
                            break
                        
        return new_primary, new_mirror
    
    def _repair_optimal(self, primary: List[HostId], mirror: List[HostId],
                   destroyed: Set[ContentId]) -> Tuple[List[HostId], List[HostId]]:
        """
        Try to optimally repair destroyed segments.
        """
        new_primary = primary[:]
        new_mirror = mirror[:]

        # Clear destroyed segments
        for seg in destroyed:
            new_primary[seg] = -1
            new_mirror[seg] = -1

        # Build mirror cache once
        mirror_cache = self._build_mirror_groups_cache(new_primary, new_mirror)

        # Initialize load tracker
        primary_capacity = [self.target_primary_load] * self.n_hosts
        mirror_capacity = [self.target_primary_load] * self.n_hosts

        for seg in range(self.n_segments):
            if seg not in destroyed and new_primary[seg] != -1:
                primary_capacity[new_primary[seg]] -= 1
                mirror_capacity[new_mirror[seg]] -= 1

        # Pre-build capacity lists
        primaries_with_capacity = set(h for h in range(self.n_hosts) 
                                      if primary_capacity[h] > 0)
        mirrors_with_capacity = set(h for h in range(self.n_hosts) 
                                    if mirror_capacity[h] > 0)

        # Cache valid mirrors per primary host
        valid_mirror_cache = {}

        def get_cached_valid_mirrors(p_host: int) -> Set[int]:
            """Get valid mirrors with capacity for a primary host."""
            if p_host not in valid_mirror_cache:
                all_valid = self._get_valid_mirrors(p_host, mirror_cache)
                # Filter by capacity and exclude primary
                valid_with_cap = all_valid & mirrors_with_capacity - {p_host}
                valid_mirror_cache[p_host] = valid_with_cap
            return valid_mirror_cache[p_host]

        # Sort destroyed segments (prefer original placements)
        destroyed_list = sorted(destroyed, key=lambda s: (
            0 if self.initial_primary[s] < self.n_hosts else 1, s
        ))

        # Greedy assignment
        for seg in destroyed_list:
            orig_p = self.initial_primary[seg]
            orig_m = self.initial_mirror[seg]

            best_cost = 3  # Max cost is 2
            best_p, best_m = -1, -1

            # Try original placement first
            if (orig_p in primaries_with_capacity and 
                primary_capacity[orig_p] > 0):

                valid_mirrors = get_cached_valid_mirrors(orig_p)

                if orig_m in valid_mirrors and mirror_capacity[orig_m] > 0:
                    # Perfect match: cost = 0
                    best_p, best_m = orig_p, orig_m
                    best_cost = 0
                elif valid_mirrors:
                    # Original primary, different mirror: cost = 1
                    # Pick any valid mirror
                    best_m = next(iter(valid_mirrors))
                    best_p = orig_p
                    best_cost = 1

            # If not cost 0, try other primaries
            if best_cost > 0:
                for p_host in primaries_with_capacity:
                    if primary_capacity[p_host] <= 0:
                        continue  # Exhausted since last update
                    
                    # Skip if we already have cost 0 solution
                    if best_cost == 0:
                        break
                    
                    valid_mirrors = get_cached_valid_mirrors(p_host)

                    if not valid_mirrors:
                        continue
                    
                    # Calculate cost for this primary
                    p_cost = 0 if p_host == orig_p else 1

                    # Find best mirror for this primary
                    if orig_m in valid_mirrors and mirror_capacity[orig_m] > 0:
                        # Original mirror available - best case
                        m_cost = 0
                        best_m_for_p = orig_m
                    else:
                        # Pick any valid mirror
                        m_cost = 1
                        best_m_for_p = next(iter(valid_mirrors))

                    total_cost = p_cost + m_cost

                    if total_cost < best_cost:
                        best_cost = total_cost
                        best_p = p_host
                        best_m = best_m_for_p

                        # Early exit if cost is 0
                        if best_cost == 0:
                            break
                        
            # FALLBACK. Should rarely happen
            if best_p == -1:
                # Just pick any valid assignment
                if primaries_with_capacity:
                    best_p = next(iter(primaries_with_capacity))
                    valid_mirrors = get_cached_valid_mirrors(best_p)
                    if valid_mirrors:
                        best_m = next(iter(valid_mirrors))

            # Apply assignment
            if best_p != -1 and best_m != -1:
                new_primary[seg] = best_p
                new_mirror[seg] = best_m

                # Update capacities
                primary_capacity[best_p] -= 1
                mirror_capacity[best_m] -= 1

                # Update mirror_cache
                mirror_cache[best_p].add(best_m)

                # Invalidate affected entries in valid_mirror_cache
                if self.strategy == 'spread':
                    # Only the assigned primary's cache changes
                    valid_mirror_cache.pop(best_p, None)
                elif self.strategy == 'grouped':
                    # All primaries using best_m might be affected
                    # Clear cache for primaries in same group
                    for p in list(valid_mirror_cache.keys()):
                        if best_m in mirror_cache.get(p, set()):
                            valid_mirror_cache.pop(p, None)

                # Update capacity sets
                if primary_capacity[best_p] == 0:
                    primaries_with_capacity.discard(best_p)
                if mirror_capacity[best_m] == 0:
                    mirrors_with_capacity.discard(best_m)

        return new_primary, new_mirror
    
    # LOCAL SEARCH

    def _local_mirror_swap(self, primary: List[HostId], mirror: List[HostId]) -> Tuple[List[HostId], List[HostId]]:
        """
        Local search: swap mirror assignments between two primary groups.
        Only for grouped strategy.
        """
        if self.strategy != 'grouped':
            return primary, mirror
        
        # Build primary -> mirror mapping
        groups = defaultdict(list)
        primary_to_mirror = {}
        
        for seg in range(self.n_segments):
            p = primary[seg]
            m = mirror[seg]
            groups[p].append(seg)
            primary_to_mirror[p] = m
        
        current_cost = self.solver._calculate_cost(primary, mirror)
        best_primary = primary[:]
        best_mirror = mirror[:]
        best_cost = current_cost
        
        # Try swapping mirrors between random pairs
        primaries = list(groups.keys())
        
        if len(primaries) < 2:
            return primary, mirror
        
        attempts = min(20, len(primaries) * (len(primaries) - 1) // 2)
        
        for _ in range(attempts):
            p1, p2 = random.sample(primaries, 2)
            m1 = primary_to_mirror[p1]
            m2 = primary_to_mirror[p2]
            
            # Check if swap is valid (doesn't violate primary != mirror)
            if m1 == p2 or m2 == p1:
                continue
            
            # Create candidate solution with swapped mirrors
            candidate_mirror = mirror[:]
            for seg in groups[p1]:
                candidate_mirror[seg] = m2
            for seg in groups[p2]:
                candidate_mirror[seg] = m1
            
            candidate_cost = self.solver._calculate_cost(primary, candidate_mirror)
            
            if candidate_cost < best_cost:
                best_mirror = candidate_mirror
                best_cost = candidate_cost
        
        return primary, best_mirror
    
    # UTILITIES
    
    def _acceptance_probability(self, current_cost: Cost, new_cost: Cost,
                               temperature: float) -> float:
        """
        Calculate acceptance probability using Simulated Annealing.
        """
        if new_cost < current_cost:
            return 1.0
        elif new_cost == current_cost:
            return 0.6  # Slightly favor accepting equal-cost moves
        else:
            # Classic SA formula
            delta = (new_cost - current_cost) / self.n_segments  # Normalize
            delta_capped = min(delta, 5.0)  # Prevent overflow
            return math.exp(-delta_capped / temperature)
    
    def _is_valid(self, primary: List[HostId], mirror: List[HostId]) -> bool:
        """
        Validation check.
        """
        solution = {i: (primary[i], mirror[i]) for i in range(self.n_segments)}
        return self.solver._validate_solution(solution)
    
    def _timeout_reached(self) -> bool:
        """Check if time limit exceeded."""
        if self.start_time is None or self.timeout is None:
            return False
        return time.time() - self.start_time > self.timeout
