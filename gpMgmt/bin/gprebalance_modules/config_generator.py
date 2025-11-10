import random
from typing import List, Tuple

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
