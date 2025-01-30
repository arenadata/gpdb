from dataclasses import dataclass
from typing import Set, Dict, List
from enum import Enum


@dataclass(eq=False)
class Host:
    hostname: str
    address: str
    primary_datadirs: Set[str]
    mirror_datadirs: Set[str]
    # set of content ids
    primary_segments: Set[int]
    mirror_segments: Set[int]

    def __eq__(self, other):
        return self.hostname == other.hostname and \
            self.address == other.address

    def __hash__(self):
        return hash((self.hostname, self.address))


class MirrorStrategy(Enum):
    MIRRORLESS = "none"
    GROUPED = "grouped"
    SPREAD = "spread"
