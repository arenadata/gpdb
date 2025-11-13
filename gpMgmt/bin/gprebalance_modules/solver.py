from dataclasses import dataclass, field
import random
from typing import Set, Dict, List, Tuple, Optional
from collections import defaultdict
import time

class TimeoutException(Exception):
    """Raised when search time limit is exceeded"""
    pass

class GreedySolver:
    """
    Enhanced greedy solver with better initial solution quality.
    """
    
    def __init__(self, 
                 n_segments: int,
                 n_hosts_target: int,
                 n_hosts_initial: int,
                 initial_primary: List[int],
                 initial_mirror: List[int],
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

        self._host_set = set(range(n_hosts_target))
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
    
    def solve(self) -> Tuple[Dict[int, Tuple[int, int]], int]:
        """
        Multi-phase greedy construction with cost awareness.
        """
        # Phase 1: Balance primaries (minimize primary movements)
        self.best_primary = self._balance_primaries()
    
        # Phase 2: Assign mirrors (minimize mirror movements)
        self.best_mirror = self._assign_mirrors(self.best_primary)

        if self.run_improve:
            try:
                iters = 1000
                time_limit = 60
                self.best_primary, self.best_mirror =\
                    self.improve(self.best_primary, self.best_mirror, iters, time_limit)
            except TimeoutException:
                if self.printing:
                    print(f"\n Time limit reached ({60}s)")
                   
        solution = {i: (self.best_primary[i], self.best_mirror[i]) for i in range(self.n_segments)}
        
        cost = self._calculate_cost(self.best_primary, self.best_mirror)

        assert(self._validate_solution(solution))

        return solution, cost
    
    def _balance_primaries(self) -> List[int]:
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
    
    def _assign_mirrors(self, primary: List[int]) -> List[int]:
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

                # Priority 3: Try swapping - move another segment to underloaded host
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
                                blocked_p_host: int,
                                blocked_size: int,
                                mirror_load: List[int],
                                primary_to_mirror: Dict[int, int],
                                groups: Dict[int, List[int]],
                                mirror: List[int]):
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

    def _calculate_cost(self, primary: List[int], mirror: List[int]) -> int:
        """Calculate movement cost"""
        return sum(
            (1 if primary[i] != self.initial_primary[i] else 0) +
            (1 if mirror[i] != self.initial_mirror[i] else 0)
            for i in range(self.n_segments)
        )
    
    def _validate_solution(self, solution: Dict[int, Tuple[int, int]]) -> bool:
        """Validate that solution satisfies all constraints"""
        valid = True
        
        # Check 1: All segments assigned
        if len(solution) != self.n_segments:
            valid = False
        
        # Check 2: Primary host != Mirror host 
        for i, (p, m) in solution.items():
            if p == m:
                valid = False
        
        # Check 3: Load balance
        load = [0] * (self.n_hosts_target)
        for i, (p, m) in solution.items():
            load[p] += 1
            load[m] += 1
        
        for h in range(self.n_hosts_target):
            if load[h] != self.target_primary_load * 2 :
                valid = False
        
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
                    valid = False
            elif self.strategy == 'spread':
                if len(set(mirror_hsts)) != len(mirror_hsts):
                    valid = False
        
        return valid
    
    def _build_mirror_groups_cache(self, primary: List[int], mirror: List[int]) -> Dict[int, Set[int]]:
        """
        Build cache: primary_host -> set of mirror hosts used.
        """
        cache = defaultdict(set)
        
        for seg in range(self.n_segments):
            # Only assigned segments
            if mirror[seg] != -1:
                cache[primary[seg]].add(mirror[seg])
        
        return cache
    
    def _get_valid_mirrors(self, primary_host: int,
                                  mirror_cache: Dict[int, Set[int]]) -> Set[int]:
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
    
    def improve(self, primary: List[int], mirror: List[int], 
                iterations: int = 1000, time_limit: int = 60) -> Tuple[List[int], List[int]]:
        """
        Adaptive large neighborhood search with simulated annealing.
        """
        
        best_primary = primary[:]
        best_mirror = mirror[:]
        best_cost = self._calculate_cost(primary, mirror)
        
        current_primary = primary[:]
        current_mirror = mirror[:]
        current_cost = best_cost
        
        # Track stagnation
        no_improve_count = 0
        last_improve_iter = 0
        
        # Adaptive parameters
        destroy_size = 0.15  # Start conservative

        start_time = time.time()
        
        if self.printing:
            print(f"Initial cost: {best_cost}")
        
        for iteration in range(iterations):

            if time.time() - start_time > time_limit:
                return best_primary, best_mirror

            # Adaptive strategy selection based on progress
            if iteration - last_improve_iter > 15:
                # Stuck: use aggressive diversification
                strategy = 'large_lns'
                destroy_size = min(0.5, destroy_size * 1.2)
            else:
                strategy = 'adaptive_lns'
            
            # Apply selected strategy
            if strategy == 'large_lns':
                new_primary, new_mirror, new_cost = self._large_neighborhood_search(
                    current_primary, current_mirror, destroy_size)
            else:  # adaptive_lns
                new_primary, new_mirror, new_cost = self._adaptive_lns(
                    current_primary, current_mirror)
            
            # Validate solution
            if not self._validate_solution({i: (new_primary[i], new_mirror[i]) for i in range(self.n_segments)}):
                continue
            
            # Acceptance criterion: simulated annealing
            temperature = max(0.05, 1.0 - iteration / iterations)
            accept_prob = (1.0 if new_cost < current_cost else 
                          (0.5 if new_cost == current_cost else 
                           min(0.3, temperature * 0.5)))
            
            if new_cost < current_cost or random.random() < accept_prob:
                # Accept move
                current_primary = new_primary
                current_mirror = new_mirror
                current_cost = new_cost
                
                if new_cost < best_cost:
                    # New best solution
                    best_primary = new_primary[:]
                    best_mirror = new_mirror[:]
                    best_cost = new_cost
                    last_improve_iter = iteration
                    destroy_size = max(0.1, destroy_size * 0.9)
                    
                    if self.printing:
                        print(f"Iter {iteration} ({strategy}): NEW BEST = {best_cost}")
                    
                    no_improve_count = 0
                else:
                    no_improve_count += 1
            else:
                no_improve_count += 1
            
            # Restart from best if stuck too long
            if no_improve_count > 25:
                if self.printing:
                    print(f"Iter {iteration}: Restarting from best (cost={best_cost})")
                current_primary = best_primary[:]
                current_mirror = best_mirror[:]
                current_cost = best_cost
                no_improve_count = 0
                destroy_size = 0.15
        
        if self.printing:
            print(f"Final cost: {best_cost}")
        
        return best_primary, best_mirror

    def _adaptive_lns(self, primary: List[int], mirror: List[int]) -> Tuple[List[int], List[int], int]:
        """
        Adaptive LNS with multiple destroy/repair strategies.
        """
        # Select destroy size adaptively
        destroy_size = random.choice([0.15, 0.25, 0.35])
        
        # Select destroy method
        destroy_method = random.choice(['random_segments', 'shaw_removal'])
        
        if destroy_method == 'random_segments':
            destroyed = self._destroy_random(primary, destroy_size)
        else:  # shaw_removal
            destroyed = self._shaw_removal(primary, mirror, destroy_size)
        
        # Repair with greedy regret
        new_primary, new_mirror = self._repair_with_regret(
            primary, mirror, destroyed)
        
        new_cost = self._calculate_cost(new_primary, new_mirror)
        return new_primary, new_mirror, new_cost

    def _large_neighborhood_search(self, primary: List[int], mirror: List[int],
                                   destroy_size: float) -> Tuple[List[int], List[int], int]:
        """
        Large neighborhood search with aggressive destruction.
        """
        destroyed = self._destroy_random(primary, destroy_size)
        new_primary, new_mirror = self._repair_optimal(primary, mirror, destroyed)
        new_cost = self._calculate_cost(new_primary, new_mirror)
        return new_primary, new_mirror, new_cost
    
    def _destroy_random(self, primary: List[int], destroy_size: float) -> Set[int]:
        """Destroy random segments"""
        n_destroy = max(1, int(self.n_segments * destroy_size))
        return set(random.sample(range(self.n_segments), n_destroy))

    def _shaw_removal(self, primary: List[int], mirror: List[int],
                 destroy_size: float) -> Set[int]:
        """
        Remove segments that are similar: same primary host or same mirror host.
        """
        n_destroy = max(1, int(self.n_segments * destroy_size))

        # Start with random segment
        seed_seg = random.randint(0, self.n_segments - 1)
        destroyed = {seed_seg}

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

        # Remove most related segments
        relatedness.sort(reverse=True)
        for _, seg in relatedness[:n_destroy-1]:
            destroyed.add(seg)

        return destroyed
    
    def _repair_with_regret(self, primary: List[int], mirror: List[int],
                       destroyed: Set[int]) -> Tuple[List[int], List[int]]:
        """
        Repair using regret-based insertion.
        """
        new_primary = primary[:]
        new_mirror = mirror[:]

        # Clear destroyed segments
        for seg in destroyed:
            new_primary[seg] = -1
            new_mirror[seg] = -1

        # Build mirror cache ONCE
        mirror_cache = self._build_mirror_groups_cache(new_primary, new_mirror)

        # Calculate primary capacities
        primary_capacity = [self.target_primary_load] * self.n_hosts_target
        mirror_capacity = [self.target_primary_load] * self.n_hosts_target

        for seg in range(self.n_segments):
            if seg not in destroyed and new_primary[seg] != -1:
                primary_capacity[new_primary[seg]] -= 1
                mirror_capacity[new_mirror[seg]] -= 1

        primaries_with_capacity = [h for h in range(self.n_hosts_target) 
                                   if primary_capacity[h] > 0]
        mirrors_with_capacity = set(h for h in range(self.n_hosts_target) 
                                    if mirror_capacity[h] > 0)

        # Since mirror constraints depend on (primary_host, used_mirrors),
        # we can cache the valid set for each primary host
        valid_mirror_cache = {}

        def get_cached_valid_mirrors(p_host: int) -> Set[int]:
            """Get valid mirrors with capacity for a primary host."""
            if p_host not in valid_mirror_cache:
                # Compute once
                all_valid = self._get_valid_mirrors(p_host, mirror_cache)
                # Filter by capacity and exclude primary
                valid_with_cap = all_valid & mirrors_with_capacity - {p_host}
                valid_mirror_cache[p_host] = valid_with_cap
            return valid_mirror_cache[p_host]

        remaining = list(destroyed)

        while remaining:
            # Calculate regret for each segment
            regrets = []

            for seg in remaining:
                costs = []

                # Use cached capacity list
                for p_host in primaries_with_capacity:
                    if primary_capacity[p_host] <= 0:
                        continue  # Capacity exhausted since last update
                    
                    # Use cached capacity list
                    valid_mirrors = get_cached_valid_mirrors(p_host)

                    if not valid_mirrors:
                        continue
                    
                    # Find best mirror host for this primary
                    orig_p = self.initial_primary[seg]
                    orig_m = self.initial_mirror[seg]

                    p_cost = 0 if p_host == orig_p else 1

                    if orig_m in valid_mirrors:
                        # Best case: original mirror available
                        best_m = orig_m
                        m_cost = 0
                    else:
                        # Pick any valid mirror (doesn't matter which for regret)
                        best_m = next(iter(valid_mirrors))
                        m_cost = 1

                    total_cost = p_cost + m_cost
                    costs.append((total_cost, p_host, best_m))

                if len(costs) == 0:
                    continue
                
                costs.sort()
                best_cost = costs[0][0]
                second_cost = costs[1][0] if len(costs) > 1 else best_cost + 10

                regret = second_cost - best_cost
                regrets.append((regret, -best_cost, seg, costs[0][1], costs[0][2]))

            if not regrets:
                break
            
            # Insert segment with highest regret
            regrets.sort(reverse=True)
            _, _, seg, best_p, best_m = regrets[0]

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
                # All primaries using best_m as mirror must be invalidated
                # ( grouped usually has one mirror host per primary group)
                for p in list(valid_mirror_cache.keys()):
                    if best_m in mirror_cache.get(p, set()):
                        valid_mirror_cache.pop(p, None)

            # Update capacity lists (only if needed)
            if primary_capacity[best_p] == 0:
                primaries_with_capacity.remove(best_p)
            if mirror_capacity[best_m] == 0:
                mirrors_with_capacity.discard(best_m)

            remaining.remove(seg)

        return new_primary, new_mirror
        
    def _repair_optimal(self, primary: List[int], mirror: List[int],
                   destroyed: Set[int]) -> Tuple[List[int], List[int]]:
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
        primary_capacity = [self.target_primary_load] * self.n_hosts_target
        mirror_capacity = [self.target_primary_load] * self.n_hosts_target

        for seg in range(self.n_segments):
            if seg not in destroyed and new_primary[seg] != -1:
                primary_capacity[new_primary[seg]] -= 1
                mirror_capacity[new_mirror[seg]] -= 1

        # Pre-build capacity lists
        primaries_with_capacity = set(h for h in range(self.n_hosts_target) 
                                      if primary_capacity[h] > 0)
        mirrors_with_capacity = set(h for h in range(self.n_hosts_target) 
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
            0 if self.initial_primary[s] < self.n_hosts_target else 1, s
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
