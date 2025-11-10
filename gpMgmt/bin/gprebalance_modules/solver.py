import array
from dataclasses import dataclass, field
from typing import Set, Dict, List, Tuple, Optional
from collections import defaultdict
import time

from gprebalance_modules.config_generator import ConfigGenerator

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

class ChunkedBitset:
    """
    Bitset using array of 64-bit integers.
    Used for encoding pairs of hosts for segment assignments.
    """

    __slots__ = ['chunks', 'n_chunks', 'universe_size']

    def __init__(self, universe_size: int):
        self.universe_size = universe_size
        self.n_chunks = (universe_size + 63) // 64
        # 'Q' = unsigned long long (64-bit)
        self.chunks = array.array('Q', [0] * self.n_chunks)
    
    @classmethod
    def from_chunks(cls, chunks: array.array, universe_size: int) -> 'ChunkedBitset':
        """
        Create bitset from existing chunk array (for copy operations)
        """
        bitset = cls.__new__(cls)
        bitset.chunks = chunks
        bitset.n_chunks = len(chunks)
        bitset.universe_size = universe_size
        return bitset
    
    def copy(self) -> 'ChunkedBitset':
        """
        Deep copy of bitset
        """
        new_chunks = array.array('Q', self.chunks)
        return ChunkedBitset.from_chunks(new_chunks, self.universe_size)
    
    def shallow_copy(self) -> 'ChunkedBitset':
        """
        Shallow copy - shares chunk array (for COW)
        """
        return ChunkedBitset.from_chunks(self.chunks, self.universe_size)
    
    # -------------------------------------------------------------------------
    # Bit manipulation
    # -------------------------------------------------------------------------
    
    def set_bit(self, bit: int):
        """
        Set bit at position (0-indexed)
        """
        chunk_idx = bit >> 6  # bit // 64
        bit_offset = bit & 63  # bit % 64
        self.chunks[chunk_idx] |= 1 << bit_offset
    
    def clear_bit(self, bit: int):
        """Clear bit at position"""
        chunk_idx = bit >> 6
        bit_offset = bit & 63
        self.chunks[chunk_idx] &= ~(1 << bit_offset)
    
    def test_bit(self, bit: int) -> bool:
        """Test if bit is set"""
        if bit < 0 or bit >= self.universe_size:
            return False
        chunk_idx = bit >> 6
        bit_offset = bit & 63
        return bool(self.chunks[chunk_idx] & (1 << bit_offset))
    
    def set_all(self):
        """Set all bits to 1"""
        max_val = 0xFFFFFFFFFFFFFFFF
        for i in range(self.n_chunks):
            self.chunks[i] = max_val
        
        # Clear excess bits in last chunk
        last_bits = self.universe_size & 63
        if last_bits != 0:
            self.chunks[-1] = (1 << last_bits) - 1
    
    def clear_all(self):
        """Clear all bits to 0"""
        for i in range(self.n_chunks):
            self.chunks[i] = 0
    
    # -------------------------------------------------------------------------
    # Bitwise operations (in-place)
    # -------------------------------------------------------------------------
    
    def and_inplace(self, other: 'ChunkedBitset'):
        """Bitwise AND: self &= other"""
        for i in range(self.n_chunks):
            self.chunks[i] &= other.chunks[i]
    
    def or_inplace(self, other: 'ChunkedBitset'):
        """Bitwise OR: self |= other"""
        for i in range(self.n_chunks):
            self.chunks[i] |= other.chunks[i]
    
    def andnot_inplace(self, other: 'ChunkedBitset'):
        """Bitwise AND-NOT: self &= ~other (remove bits in other)"""
        for i in range(self.n_chunks):
            self.chunks[i] &= ~other.chunks[i] & 0xFFFFFFFFFFFFFFFF
    
    def not_inplace(self):
        """Bitwise NOT: self = ~self"""
        for i in range(self.n_chunks):
            self.chunks[i] = ~self.chunks[i] & 0xFFFFFFFFFFFFFFFF
        
        # Clear excess bits in last chunk
        last_bits = self.universe_size & 63
        if last_bits != 0:
            self.chunks[-1] &= (1 << last_bits) - 1
    
    # -------------------------------------------------------------------------
    # Bitwise operations (new instance)
    # -------------------------------------------------------------------------
    
    def and_copy(self, other: 'ChunkedBitset') -> 'ChunkedBitset':
        """Return new bitset: self & other"""
        result = self.copy()
        result.and_inplace(other)
        return result
    
    def or_copy(self, other: 'ChunkedBitset') -> 'ChunkedBitset':
        """Return new bitset: self | other"""
        result = self.copy()
        result.or_inplace(other)
        return result
    
    def andnot_copy(self, other: 'ChunkedBitset') -> 'ChunkedBitset':
        """Return new bitset: self & ~other"""
        result = self.copy()
        result.andnot_inplace(other)
        return result
    
    # -------------------------------------------------------------------------
    # Query operations
    # -------------------------------------------------------------------------
    
    def count_ones(self) -> int:
        """Count number of set bits (population count)"""
        total = 0
        for chunk in self.chunks:
            # Use Brian Kernighan's algorithm for counting
            count = 0
            n = chunk
            while n:
                n &= n - 1
                count += 1
            total += count
        return total
    
    def is_empty(self) -> bool:
        """Check if bitset is empty (all zeros)"""
        for chunk in self.chunks:
            if chunk != 0:
                return False
        return True
    
    def is_nonempty(self) -> bool:
        """Check if bitset has at least one bit set"""
        return not self.is_empty()
    
    def find_first_set(self) -> int:
        """Find position of first set bit, or -1 if empty"""
        for chunk_idx, chunk in enumerate(self.chunks):
            if chunk != 0:
                # Find first set bit in this chunk using bit manipulation
                # chunk & -chunk isolates rightmost set bit
                rightmost = chunk & -chunk
                bit_offset = (rightmost.bit_length() - 1) if rightmost else 0
                return (chunk_idx << 6) + bit_offset
        return -1
    
    def iter_set_bits(self):
        """Iterator over positions of all set bits"""
        for chunk_idx, chunk in enumerate(self.chunks):
            if chunk == 0:
                continue
            base = chunk_idx << 6
            bit_offset = 0
            while chunk:
                if chunk & 1:
                    yield base + bit_offset
                chunk >>= 1
                bit_offset += 1
    
    def __eq__(self, other: 'ChunkedBitset') -> bool:
        """Equality check"""
        if self.n_chunks != other.n_chunks:
            return False
        for i in range(self.n_chunks):
            if self.chunks[i] != other.chunks[i]:
                return False
        return True

class BitsetUtils:
    """Precompute bitset masks for fast domain operations using chunked bitsets"""
    
    def __init__(self, n_hosts: int):
        self.n_hosts = n_hosts
        self.universe_size = n_hosts * n_hosts
        
        # Precompute masks for each host (using ChunkedBitset)
        self.primary_masks: List[ChunkedBitset] = []  # primary_masks[h] = all pairs with primary=h
        self.mirror_masks: List[ChunkedBitset] = []   # mirror_masks[h] = all pairs with mirror=h
        
        for h in range(n_hosts):
            primary_mask = ChunkedBitset(self.universe_size)
            mirror_mask = ChunkedBitset(self.universe_size)
            
            for p in range(n_hosts):
                for m in range(n_hosts):
                    if p != m:  # valid pairs only
                        bit = p * n_hosts + m
                        if p == h:
                            primary_mask.set_bit(bit)
                        if m == h:
                            mirror_mask.set_bit(bit)
            
            self.primary_masks.append(primary_mask)
            self.mirror_masks.append(mirror_mask)
        
        # Full domain (all valid pairs)
        self.full_domain = ChunkedBitset(self.universe_size)
        for p in range(n_hosts):
            for m in range(n_hosts):
                if p != m:
                    bit = p * n_hosts + m
                    self.full_domain.set_bit(bit)
    
    def create_full_domain(self) -> ChunkedBitset:
        """Create a new full domain bitset"""
        return self.full_domain.copy()
    
    def create_empty_domain(self) -> ChunkedBitset:
        """Create an empty domain bitset"""
        return ChunkedBitset(self.universe_size)
    
    def pair_to_bit(self, primary: int, mirror: int) -> int:
        """Convert (primary, mirror) to bit position"""
        return primary * self.n_hosts + mirror
    
    def bit_to_pair(self, bit: int) -> Tuple[int, int]:
        """Convert bit position to (primary, mirror)"""
        primary = bit // self.n_hosts
        mirror = bit % self.n_hosts
        return primary, mirror
    
    def has_pair(self, bitset: ChunkedBitset, primary: int, mirror: int) -> bool:
        """Check if bitset contains specific pair"""
        bit = self.pair_to_bit(primary, mirror)
        return bitset.test_bit(bit)
    
    def add_pair(self, bitset: ChunkedBitset, primary: int, mirror: int):
        """Add pair to bitset"""
        bit = self.pair_to_bit(primary, mirror)
        bitset.set_bit(bit)
    
    def remove_pair(self, bitset: ChunkedBitset, primary: int, mirror: int):
        """Remove pair from bitset"""
        bit = self.pair_to_bit(primary, mirror)
        bitset.clear_bit(bit)
    
    def bitset_to_pairs(self, bitset: ChunkedBitset) -> List[Tuple[int, int]]:
        """Convert bitset to list of (primary, mirror) pairs"""
        pairs = []
        for bit in bitset.iter_set_bits():
            primary, mirror = self.bit_to_pair(bit)
            pairs.append((primary, mirror))
        return pairs
    
    def pairs_to_bitset(self, pairs: List[Tuple[int, int]]) -> ChunkedBitset:
        """Convert list of pairs to bitset"""
        bitset = self.create_empty_domain()
        for primary, mirror in pairs:
            self.add_pair(bitset, primary, mirror)
        return bitset
    
    def get_primary_mask(self, host: int) -> ChunkedBitset:
        """Get precomputed mask for all pairs with primary=host"""
        return self.primary_masks[host]
    
    def get_mirror_mask(self, host: int) -> ChunkedBitset:
        """Get precomputed mask for all pairs with mirror=host"""
        return self.mirror_masks[host]

# ============================================================================
# DOMAIN REPRESENTATION
# ============================================================================
@dataclass
class SegmentDomain:
    """
    Copy-on-write domain representation using chunked bitsets.
    Each segment has a ChunkedBitset representing valid 
    primary host, mirror host) pairs.
    """
    
    def __init__(self, n_segments: int, bitset_utils: BitsetUtils):
        self.n_segments = n_segments
        self.bitset_utils = bitset_utils
        
        # Each segment starts with full domain
        full = bitset_utils.full_domain
        self.domains: List[ChunkedBitset] = [full.copy() for _ in range(n_segments)]
        self._shared = [False] * n_segments  # Track which domains are shared
        initial_size = full.count_ones()
        self._sizes: List[int] = [initial_size] * n_segments
    
    def shallow_copy(self) -> "SegmentDomain":
        """Shallow copy - share domain bitsets"""
        new_domain = SegmentDomain.__new__(SegmentDomain)
        new_domain.n_segments = self.n_segments
        new_domain.bitset_utils = self.bitset_utils
        new_domain.domains = self.domains[:]  # Share bitset references
        new_domain._shared = [True] * self.n_segments  # All shared in child
        new_domain._sizes = self._sizes[:]
        
        # Mark as shared in parent too
        for i in range(self.n_segments):
            self._shared[i] = True
        
        return new_domain
    
    def _detach(self, seg_id: int):
        """
        Copy-on-write: make segment domain private before mutation
        """
        if self._shared[seg_id]:
            self.domains[seg_id] = self.domains[seg_id].copy()
            self._shared[seg_id] = False
    
    def update_domain(self, seg_id: int, new_bitset: ChunkedBitset):
        """Update domain for segment (COW)"""
        self._detach(seg_id)
        self.domains[seg_id] = new_bitset
        self._sizes[seg_id] = new_bitset.count_ones()
    
    def update_domain_inplace(self, seg_id: int, operation, *args):
        """
        Update domain in-place with operation (COW first).
        operation: callable like lambda d: d.and_inplace(mask)
        """
        self._detach(seg_id)
        operation(self.domains[seg_id], *args)
    
    def get_domain(self, seg_id: int) -> ChunkedBitset:
        """Get domain bitset for segment (may be shared)"""
        return self.domains[seg_id]
    
    def domain_size(self, seg_id: int) -> int:
        """Number of valid pairs for segment"""
        return self._sizes[seg_id]
    
    def is_consistent(self) -> bool:
        """Check if all segments have at least one valid pair"""
        for d in self.domains:
            if d.is_empty():
                return False
        return True
    
    def get_domain_copy(self, seg_id: int) -> ChunkedBitset:
        """Get a private copy of domain (for safe mutation)"""
        return self.domains[seg_id].copy()

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
    assignments: Dict[int, Tuple[int, int]]  # seg_id -> (primary_host, mirror_host)
    unassigned: Set[int]
    unassigned_by_ph: Dict[int, List[int]] # primary_host -> [seg_ids]
    depth: int
    min_costs: Dict[int, int]  # Cached min cost per segment
    host_load: List[int] = field(default_factory=list)  # host_load[host_id]
    primary_load: List[int] = field(default_factory=list)
    mirror_load: List[int] = field(default_factory=list)
    future_sum: int = 0  # Sum of all min costs
    lower_bound: Optional[int] = None

    
    def is_complete(self, n_segments: int) -> bool:
        """Check if all segments are assigned"""
        return len(self.assignments) == n_segments
    
    def assign_segment(self, seg_id: int, primary_host: int, mirror_host: int):
        """Assign a segment and update domain"""
        self.assignments[seg_id] = (primary_host, mirror_host)
        self.unassigned.discard(seg_id)
        if primary_host in self.unassigned_by_ph:
            if seg_id in self.unassigned_by_ph[primary_host]:
                self.unassigned_by_ph[primary_host].remove(seg_id)
        self.host_load[primary_host] += 1
        self.host_load[mirror_host] += 1
        self.primary_load[primary_host] += 1
        self.mirror_load[mirror_host] += 1

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
        self.bitset_utils = solver.bitset_utils
    
    def propagate(self, node: SearchNode) -> bool:
        """
        Full constraint propagation.
        Returns False if node is inconsistent.
        """
        if not node.domains.is_consistent():
            return False
        
        # Rule 1: Singleton domains -> auto-assign
        self._propagate_singletons(node)
        
        # Rule 2: Load balance constraints
        result = self._propagate_load_balance(node)
        if result is None:
            return False
        
        # Rule 3: Dynamic mirror group strategy constraints
        result = self._propagate_strategy_constraints(node)
        if result is None:
            return False
        
        return node.domains.is_consistent()
    
    def _propagate_singletons(self, node: SearchNode) -> Optional[bool]:
        """Assign segments with only one valid pair"""
        changed = False
        
        for seg_id in node.unassigned:
            domain = node.domains.get_domain(seg_id)
            if node.domains.domain_size(seg_id) == 1:
                # Extract the single pair
                bit = domain.find_first_set()
                primary_host, mirror_host = self.bitset_utils.bit_to_pair(bit)
                
                node.assign_segment(seg_id, primary_host, mirror_host)
                changed = True
        
        return changed
    
    def _propagate_load_balance(self, node: SearchNode) -> Optional[bool]:
        """Remove pairs that would violate load constraints"""
        changed = False
        
        # Calculate remaining capacity for each host
        remaining_unassigned = len(node.unassigned)
        if remaining_unassigned == 0:
            return False
        
        full_primary_hosts = []
        full_mirror_hosts = []
        # Initialize auxilarry load
        for h in range(self.n_hosts):
            if node.primary_load[h] >= self.L_target // 2:
                # This host cannot take any more segments
                full_primary_hosts.append(h)
            elif node.mirror_load[h] >= self.L_target // 2:
                full_mirror_hosts.append(h)
        
        if not full_primary_hosts and not full_mirror_hosts:
            return False
        
        for seg_id in node.unassigned:
            domain = node.domains.get_domain(seg_id)
            original_count = node.domains.domain_size(seg_id)
            new_domain = domain.copy()
            for h in full_primary_hosts:
                new_domain.andnot_inplace(self.bitset_utils.get_primary_mask(h))
            for h in full_mirror_hosts:
                new_domain.andnot_inplace(self.bitset_utils.get_mirror_mask(h))
            
            if new_domain.is_empty():
                return None
            
            if new_domain.count_ones() < original_count:
                node.domains.update_domain(seg_id, new_domain)
                changed = True
        
        return changed
    
    def _propagate_strategy_constraints(self, node: SearchNode) -> Optional[bool]:
        """
        Propagate strategy constraints using DYNAMIC mirror groups.
        Groups are based on CURRENT primary assignments, not initial P0.
        """
        if self.strategy == 'any':
            return False
        
        groups_by_primary = defaultdict(list)
        for seg_id, (p, m) in node.assignments.items():
            groups_by_primary[p].append((seg_id, m))

        changed = False
        
        for primary_host, assigned_segs in groups_by_primary.items():
            if len(assigned_segs) < 1:
                continue
            
            if self.strategy == 'grouped':
                result = self._propagate_grouped(
                    node, primary_host, assigned_segs
                )
            else:  # 'spread'
                result = self._propagate_spread(
                    node, primary_host, assigned_segs
                )
            
            if result is None:
                return None
            changed |= result
        
        return changed
            
    def _propagate_grouped(self, node: SearchNode, 
                          primary_host: int,
                          assigned_segs: List[Tuple[int, int]]) -> Optional[bool]:
        """
        Grouped strategy: All segments on same primary_host must share same mirror_host.
        
        Example:
          Seg0 = (host1, host3), Seg2 = (host1, ?)
          → Seg2 must have (host1, host3)
        """
        changed = False
        
        mirrors = set(m for _, m in assigned_segs)
        if len(mirrors) > 1:
            return None  # Conflict detected
        if len(mirrors) == 0:
            return False
        
        assigned_mirror = mirrors.pop()
        
        # Create mask for allowed pair: (primary_host, assigned_mirror)
        allowed_bitset = self.bitset_utils.create_empty_domain()
        self.bitset_utils.add_pair(allowed_bitset, primary_host, assigned_mirror)
        
        # Get mask for this primary
        primary_mask = self.bitset_utils.get_primary_mask(primary_host)
        
        for seg_id in node.unassigned_by_ph[primary_host]:
            domain = node.domains.get_domain(seg_id)
            original_count = node.domains.domain_size(seg_id)
            
            # Remove all (primary_host, *) except (primary_host, assigned_mirror)
            new_domain = domain.copy()
            new_domain.andnot_inplace(primary_mask)  # Remove all with this primary
            new_domain.or_inplace(domain.and_copy(allowed_bitset))  # Add back the allowed one
            
            if new_domain.is_empty():
                return None
            
            if new_domain.count_ones() < original_count:
                node.domains.update_domain(seg_id, new_domain)
                changed = True
        
        return changed
    
    def _propagate_spread(self, node: SearchNode, 
                          primary_host: int,
                          assigned_segs: List[Tuple[int, int]]) -> Optional[bool]:
        """
        Spread strategy: All segments on same primary_host must have DIFFERENT mirror_hosts.
        
        Example:
          Seg0 = (host1, host3), Seg2 = (host1, ?)
          → Seg2 cannot have (host1, host3)
        """
        changed = False
        
        used_mirrors = set(m for _, m in assigned_segs)
        if len(used_mirrors) != len(assigned_segs):
            return None  # Conflict detected
        if len(used_mirrors) == 0:
            return False

        # Create mask of forbidden pairs
        forbidden_bitset = self.bitset_utils.create_empty_domain()
        for mirror_host in used_mirrors:
            self.bitset_utils.add_pair(forbidden_bitset, primary_host, mirror_host)
        
        for seg_id in node.unassigned_by_ph[primary_host]:
            domain = node.domains.get_domain(seg_id)
            original_count = node.domains.domain_size(seg_id)
            
            # Remove forbidden pairs
            new_domain = domain.copy()
            new_domain.andnot_inplace(forbidden_bitset)
            
            if new_domain.is_empty():
                return None
            
            if new_domain.count_ones() < original_count:
                node.domains.update_domain(seg_id, new_domain)
                changed = True
        
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
        self.bitset_utils = BitsetUtils(n_hosts_target)
        self.propagator = ConstraintPropagator(self)
        
        # Statistics
        self.nodes_explored = 0
        self.nodes_pruned = 0
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
            domains=SegmentDomain(self.n_segments, self.bitset_utils),
            assignments={},
            depth=0,
            unassigned=set(range(self.n_segments)),
            unassigned_by_ph={h: list(range(self.n_segments)) for h in range(self.n_hosts)},
            min_costs={},
            host_load=[0] * (self.n_hosts),
            primary_load=[0] * (self.n_hosts),
            mirror_load=[0] * (self.n_hosts)
        )

        self._initialize_bounds(root)

        # Initial propagation
        
        if not self.propagator.propagate(root):
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
        if not self.propagator.propagate(node):
            self.nodes_pruned += 1
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
        
        domain = node.domains.get_domain(seg_id)        
        for bit in domain.iter_set_bits():
            primary_host, mirror_host = self.bitset_utils.bit_to_pair(bit)
        
            # Quick feasibility check
            if not self._can_place(node, primary_host, mirror_host):
                self.nodes_explored += 1
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
        for seg_id in node.unassigned:
            domain_size = node.domains.domain_size(seg_id)
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
            domain = node.domains.get_domain(seg_id)
            if not self.bitset_utils.has_pair(domain, original[0], original[1]):
                must_move_candidates.append(seg_id)

        if must_move_candidates:
            return max(must_move_candidates, key=lambda s: node.min_costs.get(s, 0))

        return max(mrv_candidates, key=lambda s: node.min_costs.get(s, 0))
    
    
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
            domains=parent.domains.shallow_copy(),
            assignments=parent.assignments.copy(),
            unassigned=parent.unassigned.copy(),
            unassigned_by_ph=parent.unassigned_by_ph.copy(),
            depth=parent.depth + 1,
            host_load=parent.host_load.copy(),
            primary_load=parent.primary_load.copy(),
            mirror_load=parent.mirror_load.copy(),
            min_costs=parent.min_costs.copy()
        )
        
        child.assign_segment(seg_id, primary_host, mirror_host)
        
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
                if node.assignments[seg_id][0] != self.initial_primary[seg_id]:
                    cost += 1
                if node.assignments[seg_id][1] != self.initial_mirror[seg_id]:
                    cost += 1
            else:
                # If original not in domain, must move at least once
                original_primary = self.initial_primary[seg_id]
                original_mirror = self.initial_mirror[seg_id]
                
                domain = node.domains.get_domain(seg_id)
                
                # If original placement not in domain, must move at least once
                if not self.bitset_utils.has_pair(domain, original_primary, original_mirror):
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
        domain = node.domains.get_domain(seg_id)
        placements = self.bitset_utils.bitset_to_pairs(domain)
        if not placements:
            return 999  # Not a solution
        
        orig_p, orig_m = self.initial_primary[seg_id], self.initial_mirror[seg_id]
        
        return min(
            (0 if p == orig_p else 1) + (0 if m == orig_m else 1)
            for p, m in placements
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


from ortools.sat.python import cp_model

class ILPSolver:
    """
    Solve using Integer Linear Programming with decommissioned host support
    """
    
    def __init__(self, n_segments, n_hosts_target, n_hosts_initial,
                 initial_primary, initial_mirror, 
                 strategy='grouped'):
        """
        Args:
            n_segments: Number of segments
            n_hosts_target: Number of hosts after decommissioning
            n_hosts_initial: Number of hosts before decommissioning
            initial_primary: List/dict of initial primary host assignments
            initial_mirror: List/dict of initial mirror host assignments
            decommissioned_hosts: Set of host indices that are being decommissioned
            strategy: 'grouped', 'spread', or 'any'
        """
        self.n_segments = n_segments
        self.n_hosts_initial = n_hosts_initial
        self.n_hosts_target = n_hosts_target
        self.initial_primary = initial_primary
        self.initial_mirror = initial_mirror
        self.strategy = strategy
        

        self.decommissioned_hosts = set(range(n_hosts_initial)) - set(range(n_hosts_target))
        
        self.active_hosts = set(range(n_hosts_target))
        
        # Target load per active host
        self.L_target = (2 * n_segments) // n_hosts_target
        
        # Count forced movements from decommissioned hosts
        self.forced_movements = 0
        for i in range(n_segments):
            if initial_primary[i] in self.decommissioned_hosts:
                self.forced_movements += 1
            if initial_mirror[i] in self.decommissioned_hosts:
                self.forced_movements += 1
        
    
    def solve_with_ortools(self, time_limit=120):
        """
        Solve using Google OR-Tools CP-SAT solver
        """
        model = cp_model.CpModel()
        
        
        # Variables (span ALL initial hosts)
        
        # x[i][p] = 1 if segment i has primary on host p
        x = {}
        for i in range(self.n_segments):
            for p in range(self.n_hosts_initial):
                x[i, p] = model.NewBoolVar(f'x_{i}_{p}')
        
        # y[i][m] = 1 if segment i has mirror on host m
        y = {}
        for i in range(self.n_segments):
            for m in range(self.n_hosts_initial):
                y[i, m] = model.NewBoolVar(f'y_{i}_{m}')
        
        # Movement indicators (voluntary movements only)
        mu_p_voluntary = {}  # primary moved voluntarily
        mu_m_voluntary = {}  # mirror moved voluntarily
        for i in range(self.n_segments):
            mu_p_voluntary[i] = model.NewBoolVar(f'mu_p_vol_{i}')
            mu_m_voluntary[i] = model.NewBoolVar(f'mu_m_vol_{i}')
        
        # Constraints
        
        # Each segment assigned to exactly one primary
        for i in range(self.n_segments):
            model.Add(sum(x[i, p] for p in range(self.n_hosts_initial)) == 1)
        
        # Each segment assigned to exactly one mirror
        for i in range(self.n_segments):
            model.Add(sum(y[i, m] for m in range(self.n_hosts_initial)) == 1)
        
        # Co-location prevention: primary != mirror
        for i in range(self.n_segments):
            for h in range(self.n_hosts_initial):
                model.Add(x[i, h] + y[i, h] <= 1)
        
        # NEW: DECOMMISSIONING CONSTRAINTS
        # No segments can be assigned to decommissioned hosts
        for i in range(self.n_segments):
            for h in self.decommissioned_hosts:
                model.Add(x[i, h] == 0)
                model.Add(y[i, h] == 0)
        
        # LOAD BALANCE (only on active hosts)
        for h in self.active_hosts:
            model.Add(
                sum(x[i, h] for i in range(self.n_segments))  == self.L_target // 2)
            model.Add(
                sum(y[i, h] for i in range(self.n_segments)) == self.L_target // 2)
        
        # VOLUNTARY MOVEMENT TRACKING
        for i in range(self.n_segments):
            orig_p = self.initial_primary[i]
            orig_m = self.initial_mirror[i]
            
            # Primary movement tracking
            if orig_p in self.decommissioned_hosts:
                # Forced movement - don't count it
                model.Add(mu_p_voluntary[i] == 0)
            else:
                # Voluntary movement: mu_p[i] = 1 iff segment i moved primary
                model.Add(mu_p_voluntary[i] == 1 - x[i, orig_p])
            
            # Mirror movement tracking
            if orig_m in self.decommissioned_hosts:
                # Forced movement - don't count it
                model.Add(mu_m_voluntary[i] == 0)
            else:
                # Voluntary movement: mu_m[i] = 1 iff segment i moved mirror
                model.Add(mu_m_voluntary[i] == 1 - y[i, orig_m])
        
        # Strategy constraints (only on active hosts)
        if self.strategy == 'grouped':
            self._add_grouped_constraints(model, x, y)
        elif self.strategy == 'spread':
            self._add_spread_constraints(model, x, y)
        
        # Objective: Minimize VOLUNTARY movements only
        model.Minimize(
            sum(mu_p_voluntary[i] for i in range(self.n_segments)) +
            sum(mu_m_voluntary[i] for i in range(self.n_segments))
        )
        
        # Solve
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = time_limit
        solver.parameters.num_search_workers = 8  # Parallel solving
        solver.parameters.log_search_progress = True
        
        print(f"\nSolving ILP with OR-Tools (N={self.n_segments}, H_initial={self.n_hosts_initial}, H_target={self.n_hosts_target})...")
        status = solver.Solve(model)
        
        if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
            # Extract solution
            solution = {}
            for i in range(self.n_segments):
                primary = next(p for p in range(self.n_hosts_initial) if solver.Value(x[i, p]) == 1)
                mirror = next(m for m in range(self.n_hosts_initial) if solver.Value(y[i, m]) == 1)
                solution[i] = (primary, mirror)
            
            voluntary_movements = int(solver.ObjectiveValue())
            total_movements = voluntary_movements + self.forced_movements
            
            print(f"\n{'='*70}")
            print(f"ILP SOLUTION FOUND")
            print(f"{'='*70}")
            print(f"Status:               {solver.StatusName(status)}")
            print(f"Voluntary movements:  {voluntary_movements}")
            print(f"Forced movements:     {self.forced_movements}")
            print(f"Total movements:      {total_movements}")
            print(f"Movement ratio:       {total_movements / (2 * self.n_segments) * 100:.1f}%")
            print(f"Time:                 {solver.WallTime():.2f}s")
            print(f"Branches:             {solver.NumBranches():,}")
            print(f"Conflicts:            {solver.NumConflicts():,}")
            print(f"{'='*70}")
            
            return solution, total_movements
        else:
            print(f"No solution found. Status: {solver.StatusName(status)}")
            return None, float('inf'), float('inf')
        
    def _add_grouped_constraints(self, model, x, y):
        """
        Grouped: all segments on same primary must use same mirror (only on active hosts)

        Efficient approach using auxiliary variables:
        - For each host h, introduce uses_mirror[h][m] = 1 if primary host h uses mirror host m
        """
        # Auxiliary variables: uses_mirror[h][m] = 1 if primary host h uses mirror host m
        uses_mirror = {}
        for h in self.active_hosts:
            for m in self.active_hosts:
                if m != h:  # Can't mirror on same host
                    uses_mirror[h, m] = model.NewBoolVar(f'uses_mirror_{h}_{m}')

        # Constraint 1: Each primary host uses AT MOST 1 mirror host
        for h in self.active_hosts:
            model.Add(sum(uses_mirror[h, m] for m in self.active_hosts if m != h) <= 1)

        # Constraint 2: Link segment assignments to mirror usage
        # If segment i has primary on h, then its mirror must be on h's designated mirror host
        for i in range(self.n_segments):
            for h in self.active_hosts:
                for m in self.active_hosts:
                    if m != h:
                        model.Add(x[i, h] + y[i, m] <= uses_mirror[h, m] + 1)

    def _add_spread_constraints(self, model, x, y):
        """

        """
        # For each (primary, mirror) pair, at most 1 segment can use it
        for h in self.active_hosts:
            for m in self.active_hosts:
                if m != h:  # Can't mirror on same host
                    # At most 1 segment can use (primary=h, mirror=m)
                    segments_using_pair = []
                    for i in range(self.n_segments):
                        # aux[i,h,m] = 1 iff segment i uses (primary=h, mirror=m)
                        aux = model.NewBoolVar(f'pair_{i}_{h}_{m}')

                        # aux = x[i,h] AND y[i,m]
                        model.AddBoolAnd([x[i, h], y[i, m]]).OnlyEnforceIf(aux)
                        model.AddBoolOr([x[i, h].Not(), y[i, m].Not()]).OnlyEnforceIf(aux.Not())

                        segments_using_pair.append(aux)

                    # At most 1 segment uses this (h,m) pair
                    model.Add(sum(segments_using_pair) <= 1)

    def validate_solution(self, solution):
        """
        Validate that the solution satisfies all constraints
        """
        errors = []
        
        # Check no segments on decommissioned hosts
        for seg_id, (prim, mirr) in solution.items():
            if prim in self.decommissioned_hosts:
                errors.append(f"Segment {seg_id} has primary on decommissioned host {prim}")
            if mirr in self.decommissioned_hosts:
                errors.append(f"Segment {seg_id} has mirror on decommissioned host {mirr}")
        
        # Check co-location
        for seg_id, (prim, mirr) in solution.items():
            if prim == mirr:
                errors.append(f"Segment {seg_id} has co-located primary and mirror on host {prim}")
        
        # Check load balance on active hosts only
        from collections import defaultdict
        host_loads = defaultdict(int)
        for seg_id, (prim, mirr) in solution.items():
            host_loads[prim] += 1
            host_loads[mirr] += 1
        
        for h in self.active_hosts:
            if host_loads[h] != self.L_target:
                errors.append(f"Active host {h} has load {host_loads[h]}, expected {self.L_target}")
        
        # Check decommissioned hosts have zero load
        for h in self.decommissioned_hosts:
            if host_loads[h] > 0:
                errors.append(f"Decommissioned host {h} has non-zero load {host_loads[h]}")
        
        if errors:
            print(f"\nValidation FAILED - {len(errors)} errors:")
            for error in errors:
                print(f"  ❌ {error}")
            return False
        else:
            print("✅ Validation passed - all constraints satisfied")
            return True

    def print_host_distribution(self, solution):
        """Print load distribution across all hosts"""
        from collections import defaultdict
        host_primaries = defaultdict(int)
        host_mirrors = defaultdict(int)
        
        for seg_id, (prim, mirr) in solution.items():
            host_primaries[prim] += 1
            host_mirrors[mirr] += 1
        
        print(f"\n{'='*60}")
        print(f"HOST LOAD DISTRIBUTION")
        print(f"{'='*60}")
        print(f"{'Host':>4} | {'Prim':>5} | {'Mirr':>5} | {'Total':>5} | Status")
        print(f"{'-'*4}-+-{'-'*5}-+-{'-'*5}-+-{'-'*5}-+{'-'*15}")
        
        for h in range(self.n_hosts_initial):
            prim_count = host_primaries[h]
            mirr_count = host_mirrors[h]
            total = prim_count + mirr_count
            
            if h in self.decommissioned_hosts:
                status = "DECOMMISSIONED"
            elif h in self.active_hosts:
                status = "✓ ACTIVE" if total == self.L_target else f"⚠ UNBALANCED"
            else:
                status = "?"
            
            print(f"{h:4d} | {prim_count:5d} | {mirr_count:5d} | {total:5d} | {status}")
        
        print(f"{'='*60}")


if __name__=='__main__':
    conf = ConfigGenerator.generate_unbalanced_grouped(1000, 50)
    #solver = BABSolver(1000, 40, 50, conf[0], conf[1],
    #                   strategy='grouped', printing=True)
    #conf = ConfigGenerator.generate_unbalanced_grouped(20, 5)
    #solver = BABSolver(20, 5, 5, conf[0], conf[1],
    #                   strategy='grouped', printing=True)
    #solution, cost = solver.solve()


    ilp_solver = ILPSolver(1000, 40, 50, conf[0], conf[1], strategy='grouped')
    solution, cost = ilp_solver.solve_with_ortools(time_limit=120)
