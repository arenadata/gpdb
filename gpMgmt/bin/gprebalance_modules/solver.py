from dataclasses import dataclass, field
from typing import Set, Dict, List, Tuple, Optional
from collections import defaultdict
import time

import cProfile
import pstats

class TimeoutException(Exception):
    """Raised when search time limit is exceeded"""
    pass

class GreedySolver:
    """
    Greedy algorithm to find first suitable solution in two phases:
    1. Balance primaries
    2. Adjust mirrors to satisfy strategy and load balance
    Args:
    """
    def __init__(self, 
                 n_segments: int,
                 n_hosts_target: int,
                 n_hosts_initial: int, 
                 initial_primary: List[int],
                 initial_mirror: List[int],
                 strategy: str = 'grouped'):
        self.n_segments = n_segments
        self.n_hosts_target = n_hosts_target
        self.n_hosts_initial = n_hosts_initial

        assert(self.n_hosts_initial >= self.n_hosts_target),\
            "Initial set should already include decommissioned and new hosts"

        self.initial_p_plcmnt = initial_primary
        self.initial_m_plcmnt = initial_mirror
        self.strategy = strategy
        
        # Target load
        self.L_target = n_segments // self.n_hosts_target
    
    def solve(self) -> Tuple[Dict[int, Tuple[int, int]], int]:
        """
        Main greedy solve procedure.
        
        Returns:
            (solution, cost) where solution maps segment_id -> (primary_host, mirror_host)
        """
        
        # Phase 1: Balance primaries
        primaries = self._balance_primaries()
        
        # Phase 2: Assign mirrors
        solution = self._assign_mirrors(primaries)
        
        # Calculate cost
        cost = self._calculate_cost(solution)
        
        assert(self._validate_solution(solution)), "Invalid solution"
        
        return solution, cost
    
    def _is_decommissioned(self, host : int) -> bool:
        return host >= self.n_hosts_target
    
    def _balance_primaries(self) -> List[int]:
        """
        Phase 1: Balance primary assignments.
        
        Strategy:
        - Calculate current load per host (primaries only)
        - Move segments from overloaded to underloaded hosts
        - Prioritize minimal movements
        """
        # Start with original primaries
        primaries = self.initial_p_plcmnt.copy()
        
        # Calculate initial load
        primary_load = self._calculate_primary_load(primaries)
                
        # Identify overloaded and underloaded hosts
        overloaded = [(h, primary_load[h]) for h in range(0, self.n_hosts_initial) 
                      if primary_load[h] > self.L_target or self._is_decommissioned(h)]
        underloaded = [(h, primary_load[h]) for h in range(0, self.n_hosts_target) 
                       if primary_load[h] < self.L_target]
        
        # Already balanced
        if not overloaded and not underloaded:
            return primaries
        
        overloaded.sort(key=lambda x: x[1])
        underloaded.sort(key=lambda x: x[1])
        
        movements = 0
        
        # Greedy balancing: move from overloaded to underloaded
        for over_host, _ in overloaded:
            # Find segments on this host
            segments_on_host = [i for i in range(self.n_segments) 
                               if primaries[i] == over_host]
            
            # Move segments to underloaded hosts
            for segment_id in segments_on_host:
                if not self._is_decommissioned(over_host) and primary_load[over_host] == self.L_target:
                    break
                
                # Find destination
                dest = None

                for under_host, _ in underloaded:
                    if primary_load[under_host] >= self.L_target:
                        continue  # This destination is now full
                    
                    dest = under_host
                
                if dest is not None:
                    # Move segment
                    primaries[segment_id] = dest
                    primary_load[over_host] -= 1
                    primary_load[dest] += 1
                    movements += 1
        
        assert(len(set(primaries)) == self.n_hosts_target)
        
        return primaries
    
    def _assign_mirrors(self, primaries: List[int]) -> Dict[int, Tuple[int, int]]:
        """
        Phase 2: Assign mirrors based on primaries and strategy.
        
        For each segment:
        - Cannot place mirror on same host as primary
        - Must satisfy strategy constraints (grouped/spread/any)
        - Should balance load
        """
        solution = {}
        mirror_load = [0] * (self.n_hosts_initial)
        
        # Group segments by host
        primaries_by_host = defaultdict(list)
        for i in range(self.n_segments):
            primaries_by_host[primaries[i]].append(i)
        
        for h in self.initial_m_plcmnt:
            mirror_load[h] += 1
        
        # Process each primary group
        for phost in sorted(primaries_by_host.keys()):
            segments = primaries_by_host[phost]
            
            if self.strategy == 'grouped':
                # All segment in group should have same host
                # Cyclic assignment to guarantee perfect balance
                mirror_host = (phost + 1) % self.n_hosts_target
                for seg_id in segments:
                    solution[seg_id] = (phost, mirror_host)
            
            elif self.strategy == 'spread':
                # Each segment should have different mirror hosts
                mirror_hsts = self._find_spread_hosts(
                    segments, phost, mirror_load
                )
                                
                for seg_id, mirror_h in zip(segments, mirror_hsts):
                    solution[seg_id] = (primaries[seg_id], mirror_h)
            else: # 'any'
                mirror_hsts = self._find_any_hosts(
                    segments, phost, mirror_load
                    )
                for seg_id, mirror_h in zip(segments, mirror_hsts):
                     solution[seg_id] = (primaries[seg_id], mirror_h)
        
        return solution
    
    def _find_spread_hosts(self,
                                   segments: List[int],
                                   primary_host: int,
                                   mirror_load: List[int]) -> List[int]:
        """
        Spread: Each segment → different mirror host.
        Distribute across least-loaded hosts.
        """
        n = len(segments)

        # Get valid hosts (not primary)
        valid_hosts = [h for h in range(self.n_hosts_target) if h != primary_host]

        # Sort by load
        valid_hosts.sort(key=lambda h: mirror_load[h])

        # Assign round-robin style
        result = []
        for i, seg_id in enumerate(segments):
            mirror_host = valid_hosts[i % len(valid_hosts)]
            result.append(mirror_host)
            mirror_load[mirror_host] += 1
            mirror_load[self.initial_m_plcmnt[seg_id]] -= 1

        return result
            
    def _find_any_hosts(self,
                                segments: List[int],
                                primary_host: int,
                                mirror_load: List[int]) -> List[int]:
        """
        Any: Each mirror -> least loaded host.
        No constraints on uniqueness.
        """
        result = []
    
        for seg_id in segments:
            # Find least loaded valid host (not primary)
            candidates = [(mirror_load[h], h) for h in range(self.n_hosts_target)
                         if h != primary_host]

            best_host = min(candidates)[1]
            result.append(best_host)
            mirror_load[best_host] += 1
            mirror_load[self.initial_m_plcmnt[seg_id]] -= 1

        return result

    def _calculate_primary_load(self, primaries: List[int]) -> List[int]:
        """Calculate load on each host (primaries only)"""
        load = [0] * (self.n_hosts_initial)
        for primary in primaries:
            load[primary] += 1
        return load
    
    def _calculate_cost(self, solution: Dict[int, Tuple[int, int]]) -> int:
        """Calculate total movement cost"""
        cost = 0
        for i in range(self.n_segments):
            p, m = solution[i]
            if p != self.initial_p_plcmnt[i]:
                cost += 1
            if m != self.initial_m_plcmnt[i]:
                cost += 1
        return cost
    
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
            if load[h] != self.L_target * 2 :
                valid = False
        
        # Check 4: Strategy constraints
        segments_by_host = defaultdict(list)
        for i, (p, m) in solution.items():
            segments_by_host[p].append((i, m))
        
        for primary_h, segs in segments_by_host.items():
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

# ============================================================================
# DOMAIN REPRESENTATION
# ============================================================================
@dataclass
class SegmentDomain:
    """
    Domain of valid (primary_host, mirror_host) pairs for each segment.
    
    Attributes:
        pairs: List where pairs[seg_id] = {(primary_host, mirror_host), ...}
    """
    pairs: List[Set[Tuple[int, int]]]  # pairs[seg_id] → {(p_host, m_host)}
    
    def __init__(self, n_segments: int, n_hosts: int):
        """Initialize with all valid pairs (primary_host ≠ mirror_host)"""
        self.pairs = []
        for seg_id in range(n_segments):
            valid_pairs = {
                (primary_host, mirror_host) 
                for primary_host in range(n_hosts) 
                for mirror_host in range(n_hosts) 
                if primary_host != mirror_host
            }
            self.pairs.append(valid_pairs)
    
    def copy(self):
        new_domain = SegmentDomain.__new__(SegmentDomain)
        new_domain.pairs = [pair_set.copy() for pair_set in self.pairs]
        return new_domain
    
    def is_consistent(self) -> bool:
        """Check if all segments have at least one valid placement"""
        return all(len(pair_set) > 0 for pair_set in self.pairs)

# ============================================================================
# NOGOOD STORE - Prune already-explored failing branches
# ============================================================================
class NogoodStore:
    """
    Store partial assignments that led to failure.
    Avoids re-exploring equivalent subtrees.
    """
    
    def __init__(self, n_segments, max_size: int = 10000):
        self.nogoods: Set[str] = set()
        self.max_size = max_size
        self.hits = 0
        self.checks = 0
        self.edge_depth = int(0.5 * n_segments)
    
    def add_nogood(self, assignments: Dict[int, Tuple[int, int]], depth: int):
        """Add a failing partial assignment (only shallow nogoods)"""
        if depth > self.edge_depth:  # Don't store very specific nogoods
            return
        
        if len(self.nogoods) >= self.max_size:
            self.nogoods.pop()
        
        nogood_hash = self._hash_assignment(assignments)
        self.nogoods.add(nogood_hash)
    
    def is_nogood(self, assignments: Dict[int, Tuple[int, int]]) -> bool:
        """Check if current assignment contains a known nogood"""
        self.checks += 1
        
        # Check subsets up to size 5
        import itertools
        for subset_size in range(1, min(len(assignments) + 1, 6)):
            for subset_keys in itertools.combinations(assignments.keys(), subset_size):
                subset = {seg_id: assignments[seg_id] for seg_id in subset_keys}
                subset_hash = self._hash_assignment(subset)
                
                if subset_hash in self.nogoods:
                    self.hits += 1
                    return True
        
        return False
    
    def _hash_assignment(self, assignments: Dict[int, Tuple[int, int]]) -> str:
        """Hash a partial assignment for storage"""
        return str(sorted(assignments.items()))

# ============================================================================
# SEARCH NODE - State in the Branch-and-Bound tree
# ============================================================================

@dataclass
class SearchNode:
    """
    Node in the search tree.
    
    Attributes:
        domains: Valid (primary_host, mirror_host) pairs for each segment
        assignments: Current assignment branch {seg_id: (primary_host, mirror_host)}
        depth: Number of segments assigned
        lower_bound: Minimum cost achievable from this node
        host_load: host_load[host_id] = number of segments on that host
    """
    domains: SegmentDomain
    assignments: Dict[int, Tuple[int, int]]  # seg_id → (primary_host, mirror_host)
    depth: int
    min_costs: Dict[int, int]  # Cached min cost per segment
    host_load: List[int] = field(default_factory=list)  # host_load[host_id]
    future_sum: int = 0  # Sum of all min costs
    lower_bound: Optional[int] = None
    
    def is_complete(self, n_segments: int) -> bool:
        """Check if all segments are assigned"""
        return len(self.assignments) == n_segments
    
    def assign_segment(self, seg_id: int, primary_host: int, mirror_host: int):
        """Assign a segment and update domain"""
        self.assignments[seg_id] = (primary_host, mirror_host)
        self.domains.pairs[seg_id] = {(primary_host, mirror_host)}

# ============================================================================
# DYNAMIC MIRROR GROUPS - Groups computed from current assignments
# ============================================================================

class DynamicMirrorGroups:
    """
    Compute mirror groups dynamically based on CURRENT primary assignments.
    Not based on initial mirroring, but on actual assigned primaries during search.
    """
    
    @staticmethod
    def get_groups(node: SearchNode, strategy: str) -> List[Dict]:
        """
        Build mirror groups from CURRENT assignments.
        
        Returns:
            List of groups: [{'primary_host': h, 'seg_ids': [...], 'strategy': ...}, ...]
        """
        groups_by_primary = defaultdict(list)
        
        # Group ASSIGNED segments by their NEW primary_host
        for seg_id, (primary_host, mirror_host) in node.assignments.items():
            groups_by_primary[primary_host].append(seg_id)
        
        # Convert to group structures (only if 2+ segments share primary)
        mirror_groups = []
        for primary_host in sorted(groups_by_primary.keys()):
            seg_ids = groups_by_primary[primary_host]
            
            if len(seg_ids) > 1:  # Only groups with 2+ segments
                group = {
                    'primary_host': primary_host,
                    'seg_ids': seg_ids,
                    'strategy': strategy
                }
                mirror_groups.append(group)
        
        return mirror_groups
    
    @staticmethod
    def get_potential_groups(node: SearchNode, seg_id: int, 
                            primary_host: int, strategy: str) -> List[Dict]:
        """
        Get groups that WOULD exist if we assign seg_id to primary_host.
        Used for lookahead in constraint checking.
        """
        groups_by_primary = defaultdict(list)
        
        # Add existing assignments
        for sid, (p_host, m_host) in node.assignments.items():
            groups_by_primary[p_host].append(sid)
        
        # Add hypothetical assignment
        groups_by_primary[primary_host].append(seg_id)
        
        # Convert to groups
        mirror_groups = []
        for p_host in sorted(groups_by_primary.keys()):
            seg_list = groups_by_primary[p_host]
            if len(seg_list) > 1:
                mirror_groups.append({
                    'primary_host': p_host,
                    'seg_ids': seg_list,
                    'strategy': strategy
                })
        
        return mirror_groups

# ============================================================================
# CONSTRAINT PROPAGATOR - Domain reduction engine
# ============================================================================

class ConstraintPropagator:
    """
    Advanced constraint propagation with dynamic mirror groups.
    
    Reduces domain space by:
    1. Singleton propagation (auto-assign segments with 1 option)
    2. Load balance constraints (remove pairs violating capacity)
    3. Strategy constraints (grouped/spread enforcement)
    4. Arc consistency (AC-3 algorithm)
    """
    
    def __init__(self, solver):
        self.solver = solver
        self.n_segments = solver.n_segments
        self.n_hosts = solver.n_hosts
        self.strategy = solver.strategy
        self.L_target = solver.L_target
        self.delta = 0
        self.L_min = solver.L_target
        self.L_max = solver.L_target
    
    def propagate(self, node: SearchNode) -> bool:
        """
        Full constraint propagation.
        Returns False if node is inconsistent.
        """
        if not node.domains.is_consistent():
            return False
        
        changed = True
        iterations = 0
        max_iterations = self.n_segments * 3
        
        while changed and iterations < max_iterations:
            changed = False
            iterations += 1
            
            # Rule 1: Singleton domains → auto-assign
            result = self._propagate_singletons(node)
            changed |= result
            
            # Rule 2: Load balance constraints
            result = self._propagate_load_balance(node)
            if result is None:
                return False
            changed |= result
            
            # Rule 3: Dynamic mirror group strategy constraints
            result = self._propagate_strategy_constraints(node)
            if result is None:
                return False
            changed |= result
        
        return node.domains.is_consistent()
    
    def _propagate_singletons(self, node: SearchNode) -> Optional[bool]:
        """Assign segments with only one valid pair"""
        changed = False
        
        for seg_id in range(self.n_segments):
            if seg_id not in node.assignments and len(node.domains.pairs[seg_id]) == 1:
                primary_host, mirror_host = next(iter(node.domains.pairs[seg_id]))
                node.assign_segment(seg_id, primary_host, mirror_host)
                
                # Update load
                if len(node.host_load) > max(primary_host, mirror_host):
                    node.host_load[primary_host] += 1
                    node.host_load[mirror_host] += 1
                
                changed = True
        
        return changed
    
    def _propagate_load_balance(self, node: SearchNode) -> Optional[bool]:
        """Remove pairs that would violate load constraints"""
        changed = False
        
        # Initialize auxilarry load
        primary_load = [0] * (self.n_hosts)
        mirror_load = [0] * (self.n_hosts)
        for seg_id, (primary_host, mirror_host) in node.assignments.items():
            primary_load[primary_host] += 1
            mirror_load[mirror_host] += 1
        
        # Check each host
        for host_id in range(self.n_hosts):            
            # Host is full - remove all pairs using it
            if primary_load[host_id] >= self.L_max // 2:
                for seg_id in range(self.n_segments):
                    if seg_id not in node.assignments:
                        before_size = len(node.domains.pairs[seg_id])
                        node.domains.pairs[seg_id] = {
                            (p_host, m_host) 
                            for (p_host, m_host) in node.domains.pairs[seg_id]
                            if p_host != host_id
                        }
                        if len(node.domains.pairs[seg_id]) == 0:
                            return None  # Inconsistent
                        if len(node.domains.pairs[seg_id]) < before_size:
                            changed = True
            elif mirror_load[host_id] >= self.L_max // 2:
                for seg_id in range(self.n_segments):
                    if seg_id not in node.assignments:
                        before_size = len(node.domains.pairs[seg_id])
                        node.domains.pairs[seg_id] = {
                            (p_host, m_host) 
                            for (p_host, m_host) in node.domains.pairs[seg_id]
                            if m_host != host_id
                        }
                        if len(node.domains.pairs[seg_id]) == 0:
                            return None  # Inconsistent
                        if len(node.domains.pairs[seg_id]) < before_size:
                            changed = True

            # Check if host can still reach minimum load
            n_unassigned = sum(1 for seg in range(self.n_segments) 
                             if seg not in node.assignments)
            max_possible_load = node.host_load[host_id] + n_unassigned * 2
            
            if max_possible_load < self.L_min:
                return None  # Cannot satisfy minimum load
        
        return changed
    
    def _propagate_strategy_constraints(self, node: SearchNode) -> Optional[bool]:
        """
        Propagate strategy constraints using DYNAMIC mirror groups.
        Groups are based on CURRENT primary assignments, not initial P0.
        """
        changed = False
        
        # Get current mirror groups (dynamic!)
        mirror_groups = DynamicMirrorGroups.get_groups(node, self.strategy)
        
        if self.strategy == 'grouped':
            result = self._propagate_grouped(node, mirror_groups)
        elif self.strategy == 'spread':
            result = self._propagate_spread(node, mirror_groups)
        else: # 'any'
            result = False

        if result is None:
            return None
        changed |= result
        
        return changed
    
    def _propagate_grouped(self, node: SearchNode, 
                          mirror_groups: List[Dict]) -> Optional[bool]:
        """
        Grouped strategy: All segments on same primary_host must share same mirror_host.
        
        Example:
          Seg0 = (host1, host3), Seg2 = (host1, ?)
          → Seg2 must have (host1, host3)
        """
        changed = False
        
        for group in mirror_groups:
            primary_host = group['primary_host']
            seg_ids = group['seg_ids']
            
            # Find assigned mirror host for this primary group
            assigned_mirror_host = None
            for seg_id in seg_ids:
                if seg_id in node.assignments:
                    p_host, m_host = node.assignments[seg_id]
                    if p_host == primary_host:
                        if assigned_mirror_host is None:
                            assigned_mirror_host = m_host
                        elif assigned_mirror_host != m_host:
                            return None  # Conflict!
            
            # If mirror is determined, propagate to unassigned segments
            if assigned_mirror_host is not None:
                for seg_id in range(self.n_segments):
                    if seg_id not in node.assignments:
                        before_size = len(node.domains.pairs[seg_id])
                        # Keep only (primary_host, assigned_mirror_host)
                        node.domains.pairs[seg_id] = {
                            (p_host, m_host) 
                            for (p_host, m_host) in node.domains.pairs[seg_id]
                            if p_host != primary_host or m_host == assigned_mirror_host
                        }
                        if len(node.domains.pairs[seg_id]) == 0:
                            return None
                        if len(node.domains.pairs[seg_id]) < before_size:
                            changed = True
        
        return changed
    
    def _propagate_spread(self, node: SearchNode, 
                         mirror_groups: List[Dict]) -> Optional[bool]:
        """
        Spread strategy: All segments on same primary_host must have DIFFERENT mirror_hosts.
        
        Example:
          Seg0 = (host1, host3), Seg2 = (host1, ?)
          → Seg2 cannot have (host1, host3)
        """
        changed = False
        
        for group in mirror_groups:
            primary_host = group['primary_host']
            seg_ids = group['seg_ids']
            
            # Collect used mirrors for this primary
            used_mirror_hosts = set()
            for seg_id in seg_ids:
                if seg_id in node.assignments:
                    p_host, m_host = node.assignments[seg_id]
                    if p_host == primary_host:
                        used_mirror_hosts.add(m_host)
            
            # Remove used mirrors from unassigned domains
            for seg_id in range(self.n_segments):
                if seg_id not in node.assignments:
                    before_size = len(node.domains.pairs[seg_id])
                    node.domains.pairs[seg_id] = {
                        (p_host, m_host) 
                        for (p_host, m_host) in node.domains.pairs[seg_id]
                        if p_host != primary_host or m_host not in used_mirror_hosts
                    }
                    if len(node.domains.pairs[seg_id]) == 0:
                        return None
                    if len(node.domains.pairs[seg_id]) < before_size:
                        changed = True
            
            # Feasibility check: enough distinct mirrors available?
            unassigned_on_primary = [
                seg for seg in range(self.n_segments)
                if seg not in node.assignments
                and any(p == primary_host for (p, m) in node.domains.pairs[seg])
            ]
            
            available_mirrors = set()
            for seg_id in unassigned_on_primary:
                for (p_host, m_host) in node.domains.pairs[seg_id]:
                    if p_host == primary_host:
                        available_mirrors.add(m_host)
            
            needed_count = len(used_mirror_hosts) + len(unassigned_on_primary)
            total_count = len(used_mirror_hosts) + len(available_mirrors)
            
            if total_count < needed_count:
                return None  # Not enough distinct mirrors
        
        return changed

# ============================================================================
# BRANCH-AND-BOUND SOLVER
# ============================================================================

class BABSolver:
    """
    Branch-and-Bound solver with:
    - Dynamic mirror groups (based on current assignments)
    - Constraint propagation (load, strategy)
    - Smart variable ordering
    - Nogood learning
    - Lower bound pruning
    """
    
    def __init__(self, n_segments: int,
                 n_hosts_target: int,
                 n_hosts_initial: int,
                 initial_primary: List[int],
                 initial_mirror: List[int],
                 strategy: str = 'grouped',
                 printing: bool = False):
        
        self.n_segments = n_segments
        self.n_hosts = n_hosts_target
        self.n_hosts_initial = n_hosts_initial
        self.initial_primary = initial_primary
        self.initial_mirror = initial_mirror
        self.strategy = strategy
        self.printing = printing
        
        # Target load per host including mirrors
        total_capacity = 2 * n_segments
        self.L_target = total_capacity // n_hosts_target
        
        # Pruning structures
        self.nogood_store = NogoodStore(n_segments)
        
        # Statistics
        self.nodes_explored = 0
        self.nodes_pruned = 0
        self.nogood_prunes = 0
        self.bound_prunes = 0
        self.load_prunes = 0
        self.check_counter = 0
        self.check_interval = 50

        # Best solution tracking
        self.initial_solution = GreedySolver(n_segments, n_hosts_target, n_hosts_initial,
                                      initial_primary, initial_mirror, strategy).solve()
        self.best_cost = self.initial_solution[1]
        self.best_solution =  self.initial_solution[0]
        self.start_time = None
    
    def solve(self, time_limit: float = 120) -> Tuple[Optional[Dict], int]:
        """Solve the rebalancing problem"""
        self.start_time = time.time()
        
        # Create root node
        root = SearchNode(
            domains=SegmentDomain(self.n_segments, self.n_hosts),
            assignments={},
            depth=0,
            min_costs={},
            host_load=[0] * (self.n_hosts)
        )

        self._initialize_bounds(root)

        # Initial propagation
        propagator = ConstraintPropagator(self)
        if not propagator.propagate(root):
            return None, float('inf')
        
        
        # Start search
        try:
            self._branch_and_bound(root, time_limit)
        except TimeoutException:
            if self.printing:
                print(f"\n Time limit reached ({time_limit}s)")

        if self.printing:
            self._print_statistics()
                
        return self.best_solution, self.best_cost

    def _branch_and_bound(self, node: SearchNode, time_limit: float):
        """Main B&B recursive procedure"""
        
        self.nodes_explored += 1
        
        # Time limit check
        self._check_timeout(time_limit)
        
        # Quick lower bound
        quick_lb = self._quick_lower_bound(node)
        if quick_lb >= self.best_cost:
            self.nodes_pruned += 1
            self.bound_prunes += 1
            return
        
        # Full constraint propagation
        propagator = ConstraintPropagator(self)
        if not propagator.propagate(node):
            self.nodes_pruned += 1
            self.nogood_store.add_nogood(node.assignments, node.depth)
            return
        
        # Compute tight lower bound
        node.lower_bound = self._compute_lower_bound(node)
        if node.lower_bound >= self.best_cost:
            self.nodes_pruned += 1
            self.bound_prunes += 1
            return
        
        # Check if complete
        if node.is_complete(self.n_segments):
            cost = self._compute_cost(node.assignments)
            if cost < self.best_cost:
                self.best_cost = cost
                self.best_solution = node.assignments.copy()
                if self.printing:
                    print(f"\n✓ New best: {cost} movements at node {self.nodes_explored:,}")
                    self._print_solution_summary(node.assignments)
            return
        
        # Select segment to assign (variable ordering)
        seg_id = self._select_segment_smart(node)
        if seg_id is None:
            return
        
        # Order placements to try (value ordering)
        placements = self._order_placements_smart(seg_id, node)
        
        # Branch on each placement
        for (primary_host, mirror_host) in placements:
            # Quick feasibility check
            if not self._can_place(node, primary_host, mirror_host):
                self.nodes_pruned += 1
                self.load_prunes += 1
                continue
            
            # Create child node
            child = self._create_child(node, seg_id, primary_host, mirror_host)
            
            if child:
                self._branch_and_bound(child, time_limit)
    
    def _check_timeout(self, time_limit: float):
        """
        Check timeout and raise exception if expired
        
        Raises TimeoutException to immediately exit recursion
        """
        self.check_counter += 1
        
        # Only check every N nodes
        if self.check_counter % self.check_interval == 0:
            if time.time() - self.start_time > time_limit:
                raise TimeoutException()

    def _select_segment_smart(self, node: SearchNode) -> Optional[int]:
        """
        Smart variable selection
        
        Two-phase approach:
        1. Finding Minimum Remaining Values (smallest domain first)
        2. Prioritize must-move segments
        """
        min_domain = float('inf')
        mrv_candidates = []

        # Phase 1: Find all segments with minimum domain size
        for seg_id in range(self.n_segments):
            if seg_id not in node.assignments:
                domain_size = len(node.domains.pairs[seg_id])

                if domain_size < min_domain:
                    min_domain = domain_size
                    mrv_candidates = [seg_id]
                elif domain_size == min_domain:
                    mrv_candidates.append(seg_id)

        if not mrv_candidates:
            return None

        if len(mrv_candidates) == 1:
            return mrv_candidates[0]

        # Phase 2: Break ties - prioritize must-move segments
        must_move_candidates = []
        for seg_id in mrv_candidates:
            original = (self.initial_primary[seg_id], self.initial_mirror[seg_id])
            if original not in node.domains.pairs[seg_id]:
                must_move_candidates.append(seg_id)

        if must_move_candidates:
            return must_move_candidates[0]  # Return first must-move

        return min(mrv_candidates)
    
    def _order_placements_smart(self, seg_id: int, 
                               node: SearchNode) -> List[Tuple[int, int]]:
        """
        Smart value ordering: Try best placements first
        
        Priority:
        1. Original placement (cost 0)
        2. Single movement (cost 1)
        3. Better load balance
        4. Less constraining for others
        """
        placements = list(node.domains.pairs[seg_id])
        original_primary = self.initial_primary[seg_id]
        original_mirror = self.initial_mirror[seg_id]
        
        scored_placements = []
        for (primary_host, mirror_host) in placements:
            # Movement cost
            cost = (1 if primary_host != original_primary else 0) + \
                   (1 if mirror_host != original_mirror else 0)
            
            # Load balance score
            load_score = abs(node.host_load[primary_host] - self.L_target) + \
                        abs(node.host_load[mirror_host] - self.L_target)
            
            # Constraining score (fewer future constraints is better)
            potential_groups = DynamicMirrorGroups.get_potential_groups(
                node, seg_id, primary_host, self.strategy
            )
            constraining_score = len(potential_groups)
            
            score = (cost, load_score, constraining_score)
            scored_placements.append((score, primary_host, mirror_host))
        
        scored_placements.sort()
        return [(p, m) for (_, p, m) in scored_placements]
    
    def _can_place(self, node: SearchNode, primary_host: int, mirror_host: int) -> bool:
        """Quick feasibility check before branching"""
        L_max = self.L_target
        
        if len(node.host_load) > max(primary_host, mirror_host):
            if node.host_load[primary_host] >= L_max or node.host_load[mirror_host] >= L_max:
                return False
        
        return True
    
    def _create_child(self, parent: SearchNode, seg_id: int,
                     primary_host: int, mirror_host: int) -> Optional[SearchNode]:
        """Create child node with new assignment"""
        child = SearchNode(
            domains=parent.domains.copy(),
            assignments=parent.assignments.copy(),
            depth=parent.depth + 1,
            host_load=parent.host_load.copy(),
            min_costs=parent.min_costs.copy()
        )
        
        child.assign_segment(seg_id, primary_host, mirror_host)
        
        # Update load
        if len(child.host_load) > max(primary_host, mirror_host):
            child.host_load[primary_host] += 1
            child.host_load[mirror_host] += 1
        
        # Update child bounds
        if seg_id in child.min_costs:
            removed_cost = child.min_costs[seg_id]
            del child.min_costs[seg_id]
            child.future_sum = parent.future_sum - removed_cost
        else:
            child.future_sum = parent.future_sum
        
        child.lower_bound = None 
        
        return child
    
    def _quick_lower_bound(self, node: SearchNode) -> int:
        """Fast lower bound: count definite movements"""
        cost = 0
        
        for seg_id in range(self.n_segments):
            if seg_id in node.assignments:
                primary_host, mirror_host = node.assignments[seg_id]
                if primary_host != self.initial_primary[seg_id]:
                    cost += 1
                if mirror_host != self.initial_mirror[seg_id]:
                    cost += 1
            else:
                # If original not in domain, must move at least once
                original = (self.initial_primary[seg_id], self.initial_mirror[seg_id])
                if original not in node.domains.pairs[seg_id]:
                    cost += 1
        
        return cost
    
    def _initialize_bounds(self, node: SearchNode):
        """Initialize all minimum costs"""
        node.future_sum = 0
        for seg_id in range(self.n_segments):
            if seg_id not in node.assignments:
                min_cost = self._min_cost_for_segment(seg_id, node)
                node.min_costs[seg_id] = min_cost
                node.future_sum += min_cost
    
    def _min_cost_for_segment(self, seg_id: int, node: SearchNode) -> int:
        """Minimum movement cost for one segment"""
        domain = node.domains.pairs[seg_id]
        if not domain:
            return 999  # Not a solution
        
        orig_p, orig_m = self.initial_primary[seg_id], self.initial_mirror[seg_id]
        
        return min(
            (0 if p == orig_p else 1) + (0 if m == orig_m else 1)
            for p, m in domain
        )
    
    def _compute_lower_bound(self, node: SearchNode) -> int:
        """Tight lower bound: current + minimum future cost"""
        if node.lower_bound is not None:
            return node.lower_bound
        
        # Current cost
        current = sum(
            (0 if p == self.initial_primary[sid] else 1) +
            (0 if m == self.initial_mirror[sid] else 1)
            for sid, (p, m) in node.assignments.items()
        )
        
        node.lower_bound = current + node.future_sum
        return node.lower_bound
    
    def _compute_cost(self, assignments: Dict[int, Tuple[int, int]]) -> int:
        """Total movement cost"""
        return sum(
            (1 if assignments[seg_id][0] != self.initial_primary[seg_id] else 0) +
            (1 if assignments[seg_id][1] != self.initial_mirror[seg_id] else 0)
            for seg_id in range(self.n_segments)
        )
    
    def _print_progress(self, node: SearchNode):
        """Print search progress"""
        elapsed = time.time() - self.start_time
        prune_rate = self.nodes_pruned / max(1, self.nodes_explored) * 100
        
        print(f"Nodes: {self.nodes_explored:,} | "
              f"Pruned: {self.nodes_pruned:,} ({prune_rate:.1f}%) | "
              f"Best: {self.best_cost} | "
              f"Depth: {node.depth}/{self.n_segments} | "
              f"Time: {elapsed:.1f}s")
    
    def _print_statistics(self):
        """Print final search statistics"""
        elapsed = time.time() - self.start_time
        
        print("\n" + "=" * 70)
        print("SEARCH COMPLETE")
        print("=" * 70)
        print(f"Nodes explored:    {self.nodes_explored:,}")
        print(f"Nodes pruned:      {self.nodes_pruned:,}")
        print(f"  Nogood prunes:   {self.nogood_prunes:,}")
        print(f"  Bound prunes:    {self.bound_prunes:,}")
        print(f"  Load prunes:     {self.load_prunes:,}")
        print(f"Pruning rate:      {self.nodes_pruned / max(1, self.nodes_explored) * 100:.1f}%")
        print(f"Time elapsed:      {elapsed:.2f}s")
        print(f"Nodes/second:      {self.nodes_explored / max(0.001, elapsed):.0f}")
        print(f"\nBest cost:         {self.best_cost}")
        print("=" * 70)
    
    def _print_solution_summary(self, assignments: Dict[int, Tuple[int, int]]):
        """Print solution summary"""
        primary_moves = sum(1 for seg_id in range(self.n_segments) 
                           if assignments[seg_id][0] != self.initial_primary[seg_id])
        mirror_moves = sum(1 for seg_id in range(self.n_segments) 
                          if assignments[seg_id][1] != self.initial_mirror[seg_id])
        
        host_loads = [0] * (self.n_hosts)
        for seg_id in range(self.n_segments):
            primary_host, mirror_host = assignments[seg_id]
            host_loads[primary_host] += 1
            host_loads[mirror_host] += 1
        
        print(f"  Primary moves: {primary_moves}, Mirror moves: {mirror_moves}")
        print(f"  Load: {host_loads}")

import random

class ConfigGenerator:
    """Generate test configurations"""
    
    @staticmethod
    def generate_balanced_grouped(n_segments: int, n_hosts: int) -> Tuple[List[int], List[int]]:
        """Generate balanced GROUPED configuration"""
        assert n_segments % n_hosts == 0, "Must be evenly divisible"
        
        segs_per_host = n_segments // n_hosts
        
        # Assign primaries round-robin
        primaries = []
        for host_id in range(n_hosts):
            primaries.extend([host_id] * segs_per_host)
        
        # Assign mirrors (grouped: all primaries on host_i → mirrors on host_(i+1))
        mirrors = []
        for seg_id in range(n_segments):
            p_host = primaries[seg_id]
            m_host = (p_host + 1) % n_hosts
            mirrors.append(m_host)
        
        return primaries, mirrors
    
    @staticmethod
    def generate_balanced_spread(n_segments: int, n_hosts: int) -> Tuple[List[int], List[int]]:
        """Generate balanced SPREAD configuration"""
        assert n_segments % n_hosts == 0, "Must be evenly divisible"
        
        segs_per_host = n_segments // n_hosts
        
        # Assign primaries
        primaries = []
        for host_id in range(n_hosts):
            primaries.extend([host_id] * segs_per_host)
        
        # Assign mirrors (spread: distribute across different hosts)
        mirrors = []
        for host_id in range(n_hosts):
            # Get segments on this host
            start_idx = host_id * segs_per_host
            
            # Available mirror hosts (not self)
            available = [h for h in range(n_hosts) if h != host_id]
            
            for i in range(segs_per_host):
                m_host = available[i % len(available)]
                mirrors.append(m_host)
        
        return primaries, mirrors
    
    @staticmethod
    def generate_unbalanced_grouped(n_segments: int, n_hosts: int, 
                                   skew: float = 0.3) -> Tuple[List[int], List[int]]:
        """Generate UNBALANCED GROUPED configuration"""
        base = n_segments // n_hosts
        
        # Create skewed distribution
        loads = [base] * n_hosts
        
        # Transfer segments from last hosts to first hosts
        transfer = int(base * skew)
        if transfer > 0 and n_hosts >= 2:
            loads[0] += transfer
            loads[-1] -= transfer
        
        # Assign primaries
        primaries = []
        for host_id in range(n_hosts):
            primaries.extend([host_id] * loads[host_id])
        
        # Shuffle to randomize order
        random.shuffle(primaries)
        
        # Assign mirrors (grouped)
        mirrors = []
        for seg_id in range(n_segments):
            p_host = primaries[seg_id]
            m_host = (p_host + 1) % n_hosts
            mirrors.append(m_host)
        
        return primaries, mirrors


if __name__=='__main__':
    conf = ConfigGenerator.generate_unbalanced_grouped(1000, 50)
    solver = BABSolver(1000, 40, 50, conf[0], conf[1],
                       strategy='grouped', printing=True)
    #conf = ConfigGenerator.generate_unbalanced_grouped(20, 5)
    #solver = BABSolver(20, 5, 5, conf[0], conf[1],
    #                   strategy='grouped', printing=True)
    profiler = cProfile.Profile()
    profiler.enable()
    solution, cost = solver.solve()
    profiler.disable()
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(20)  # Top 20 functions
    a = 5