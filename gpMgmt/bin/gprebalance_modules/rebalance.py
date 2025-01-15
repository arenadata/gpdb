import yaml
import os
from dataclasses import dataclass
from typing import List, Set, Dict
from enum import Enum
from gppylib.db import dbconn
from gppylib.gparray import GpArray, Segment, MODE_NOT_SYNC, STATUS_DOWN


@dataclass
class Host:
    hostname: str
    address: str
    primary_datadirs: Set[str]
    mirror_datadirs: Set[str]
    # set of content ids
    primary_segments: Set[int]
    mirror_segments: Set[int]


class MirrorStrategy(Enum):
    MIRRORLESS = "none"
    GROUPED = "grouped"
    SPREAD = "spread"


class GPRebalance:
    def __init__(self, logger, gparray, dburl, options):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.original_gparray = gparray
        self.conn = dbconn.connect(
            self.dburl, utility=True, encoding='UTF8', allowSystemTableMods=True)
        if options.mirroring == 'spread':
            self.target_strategy = MirrorStrategy.SPREAD
        elif options.mirroring == 'grouped':
            self.target_strategy = MirrorStrategy.GROUPED
        else:
            self.target_strategy = MirrorStrategy.MIRRORLESS

        hosts = self.getHostsFromGpArray()
        if options.filename:
            with open(options.filenam, 'r') as fp:
                config = yaml.safe_load(fp)
                for host_config in config['hosts']:
                    key = (host_config['hostname'], host_config['address'])
                    if key not in hosts:
                        hosts[key] = Host(hostname=host_config['hostname'],
                                          address=host_config['address'],
                                          primary_datadirs=set(
                                          host_config['primary_datadirs']),
                                          mirror_datadirs=set(
                                          host_config['mirror_datadirs']),
                                          primary_segments=set(), mirror_segments=set())
                    else:
                        hosts[key].primary_datadirs.union(
                            set(host_config['primary_datadirs']))
                        hosts[key].mirror_datadirs.union(
                            set(host_config['mirror_datadirs']))
        self.target_hosts = list(hosts.values())

    def setMirroringStrategy(self, strategy: MirrorStrategy):
        self.target_strategy = strategy

    def getHostsFromGpArray(self) -> Dict[tuple[str, str], Host]:
        hosts = {}
        for seg in self.original_gparray.getSegmentsAsLoadedFromDb():
            if seg.content >= 0:
                hosts[(seg.hostname, seg.address)] = Host(
                    hostname=seg.hostname, address=seg.address, primary_datadirs=set(), mirror_datadirs=set(),
                    primary_segments=set(), mirror_segments=set())
        for pair in self.original_gparray.segmentPairs:
            primary = pair.primaryDB
            mirror = pair.mirrorDB
            key_pr = (primary.hostname, primary.address)
            hosts[key_pr].primary_datadirs.add(
                os.path.dirname(primary.datadir))
            hosts[key_pr].primary_segments.add(primary.content)
            if mirror:
                key_mr = (mirror.hostname, mirror.address)
                hosts[key_mr].mirror_datadirs.add(
                    os.path.dirname(mirror.datadir))
                hosts[key_mr].mirror_segments.add(mirror.content)
        return hosts
