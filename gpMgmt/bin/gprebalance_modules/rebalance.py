import yaml
from dataclasses import dataclass
from typing import List, Set
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


def get_base_path(path):
    """
    Extract base path from full path
    """
    return '/'.join(path.split('/')[:-1])


def form_target_hosts(gparray: GpArray, filename: str) -> List[Host]:
    hosts = {}
    for seg in gparray.getSegmentsAsLoadedFromDb():
        if seg.content >= 0:
            hosts[(seg.hostname, seg.address)] = Host(
                hostname=seg.hostname, address=seg.address, primary_datadirs=set(), mirror_datadirs=set(),
                primary_segments=set(), mirror_segments=set())
    for pair in gparray.segmentPairs:
        primary = pair.primaryDB
        mirror = pair.mirrorDB
        key_pr = (primary.hostname, primary.address)
        hosts[key_pr].primary_datadirs.add(get_base_path(primary.datadir))
        hosts[key_pr].primary_segments.add(primary.content)
        if mirror:
            key_mr = (mirror.hostname, mirror.address)
            hosts[key_mr].mirror_datadirs.add(get_base_path(mirror.datadir))
            hosts[key_mr].mirror_segments.add(mirror.content)
    if filename:
        with open(filename, 'r') as fp:
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

    return list(hosts.values())


class GPRebalance:
    def __init__(self, logger, gparray, dburl, options):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.original_gparray = gparray
        self.conn = dbconn.connect(
            self.dburl, utility=True, encoding='UTF8', allowSystemTableMods=True)
        self.target_hosts = form_target_hosts(gparray, options.filename)
        if options.mirroring == 'spread':
            self.target_strategy = MirrorStrategy.SPREAD
        elif options.mirroring == 'grouped':
            self.target_strategy = MirrorStrategy.GROUPED
        else:
            self.target_strategy = MirrorStrategy.MIRRORLESS

    def setMirroringStrategy(self, type: MirrorStrategy):
        self.target_strategy = type
