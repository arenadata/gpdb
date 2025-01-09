from typing import List
from gprebalance_modules.rebalance import Host, MirrorStrategy, dbconn, GpArray,  Segment, MODE_NOT_SYNC, STATUS_DOWN


class StateValidationError(Exception):
    pass


class ClusterValidator:
    def __init__(self, hosts: List[Host], segarray: List[Segment], has_mirrors: bool, mirror_strategy: MirrorStrategy):
        self.hosts = hosts
        self.mirror_strategy = mirror_strategy
        self.existing_hosts = [
            h for h in hosts if h.primary_segments or h.mirror_segments]
        self.new_hosts = [h for h in hosts if not bool(
            h.primary_segments) and not bool(h.mirror_segments)]
        self.segarray = segarray
        self.has_mirrors = has_mirrors

    def validate_segment_status(self):
        inv = self.get_invalid_segments()
        if len(inv) > 0:
            raise StateValidationError(
                f"The {[s.content for s in inv]} segments are down")

    def get_invalid_segments(self) -> List[Segment]:
        l = []
        for seg in self.segarray:
            if not seg.valid:
                l.append(seg)
        return l

    def validate_existing_configuration(self) -> tuple[bool, MirrorStrategy]:
        arr = GpArray(self.segarray)
        total_primaries = arr.get_primary_count()
        total_hosts = len(self.existing_hosts)
        expected_primaries = total_primaries // total_hosts
        strat = None

        if arr.guessIsSpreadMirror():
            strat = MirrorStrategy.SPREAD
        elif arr.hasMirrors:
            strat = MirrorStrategy.GROUPED
            for host in self.existing_hosts:
                if host.primary_segments & host.mirror_segments:
                    strat = None
                    break

        for host in self.existing_hosts:
            if len(host.primary_segments) != expected_primaries:
                return False, strat

        return True, strat

    def prevalidate_segment_distribution(self):
        """
        Validate whether segments can be uniformly distributed across hosts
        """
        total_primary_segments = sum(len(h.primary_segments)
                                     for h in self.existing_hosts)
        total_hosts = len(self.hosts)

        if total_primary_segments % total_hosts != 0:
            raise StateValidationError(
                f"Cannot evenly distribute {total_primary_segments} segments across {total_hosts} hosts."
            )

    def prevalidate_mirror_strategy(self):
        """
        Validate whether the specified mirroring strategy can be achieved
        """
        if not self.has_mirrors:
            return
        total_hosts = len(self.hosts)
        total_primary_segments = sum(len(h.primary_segments)
                                     for h in self.existing_hosts)
        if total_hosts < 2:
            raise StateValidationError(
                """Cannot support target mirroring strategy on given configuration. All
                primaries will be at single host."""
            )
        if total_primary_segments % total_hosts != 0:
            raise StateValidationError(
                f"Cannot evenly distribute {total_primary_segments} segments across {total_hosts} hosts."
            )

        primaries_per_host = total_primary_segments // total_hosts
        if self.mirror_strategy == MirrorStrategy.SPREAD and primaries_per_host >= total_hosts:
            raise StateValidationError(
                f"Cannot support spread mirroring strategy on given configuration."
            )
