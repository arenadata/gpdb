import yaml
import os
import copy
from typing import List, Dict, Optional
from gppylib.userinput import ask_yesno, ask_string
from gppylib.db import dbconn
from gppylib.gparray import GpArray, Segment, MODE_SYNCHRONIZED, MODE_NOT_SYNC, STATUS_DOWN
from gprebalance_modules.models import MirrorStrategy, Host
from gprebalance_modules.rebalance_validator import ClusterValidator, StateValidationError


class ValidationError(Exception):
    pass


class GPRebalance:
    def __init__(self,
                 logger,
                 dburl: dbconn.DbURL,
                 target_filename: Optional[str] = None,
                 mirroring: Optional[str] = None):
        self.logger = logger

        self.dburl = dburl

        self.original_gparray = GpArray.initFromCatalog(dburl, utility=True)

        self.conn = dbconn.connect(
            self.dburl, utility=True, encoding='UTF8', allowSystemTableMods=True)

        if mirroring == 'spread':
            self.target_strategy = MirrorStrategy.SPREAD
        elif mirroring == 'grouped':
            self.target_strategy = MirrorStrategy.GROUPED
        elif mirroring is None:
            self.target_strategy = MirrorStrategy.MIRRORLESS

        self.current_conf = self.getHostsFromGpArray(self.original_gparray)
        self.current_hosts = list(self.current_conf .values())
        self.target_hosts = self.load_target_hosts(target_filename)
        if self.target_hosts is None:
            self.target_hosts = self.current_hosts

        self.unpreferred_segments = self.getSegmentsUnpreferredRole()

    def getSegmentsUnpreferredRole(self) -> List[tuple[Segment, Segment]]:
        segs = []
        for pair in self.original_gparray.segmentPairs:
            prim = pair.primaryDB
            mir = pair.mirrorDB
            if prim.role != prim.preferred_role and mir.role != mir.preferred_role:
                segs.append((prim, mir))
        return segs

    def setMirroringStrategy(self, strategy: MirrorStrategy):
        self.target_strategy = strategy

    @staticmethod
    def getHostsFromGpArray(original_gparray: GpArray) -> Dict[tuple[str, str], Host]:
        hosts = {}
        for seg in original_gparray.getSegmentsAsLoadedFromDb():
            if seg.content >= 0:
                hosts[(seg.hostname, seg.address)] = Host(
                    hostname=seg.hostname, address=seg.address, primary_datadirs=set(), mirror_datadirs=set(),
                    primary_segments=set(), mirror_segments=set())
        for pair in original_gparray.segmentPairs:
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

    def dump_hosts_info(self) -> str:
        """
          Converts info about hosts to YAML format
        """
        yaml_entries = []
        for (hostname, address), val in self.current_conf.items():
            host_entry = {
                'hostname': hostname,
                'address': address,
                'primary_datadirs': list(val.primary_datadirs),
                'mirror_datadirs': list(val.mirror_datadirs),
            }
            yaml_entries.append(host_entry)

        yaml_entries.sort(key=lambda x: x['hostname'])
        config = {'hosts': yaml_entries}
        config_yaml = yaml.dump(
            config, default_flow_style=False, sort_keys=False)

    def load_target_hosts(self, filename: str) -> List[Host]:
        if filename:
            self.logger.info(f'Loading target hosts from {filename}')
            with open(filename, 'r') as fp:
                hosts = {}
                config = yaml.safe_load(fp)
                for host_config in config['hosts']:
                    key = (host_config['hostname'], host_config['address'])
                    hosts[key] = Host(hostname=host_config['hostname'],
                                      address=host_config['address'],
                                      primary_datadirs=set(
                        host_config['primary_datadirs']),
                        mirror_datadirs=set(
                        host_config['mirror_datadirs']),
                        primary_segments=set(), mirror_segments=set())
                return list(hosts.values())
        return None

    def needs_balance(self,
                      option_silent: bool,
                      option_allow_mirrorless: bool) -> bool:
        self.logger.info('Validation of rebalance possibility...')

        hosts_list = self.current_hosts
        if self.unpreferred_segments:
            if not option_silent and ask_yesno('', "Current role does not match preferred role for several segments.\n"
                                               "Are you sure you want "
                                               "to continue with this gprebalance session?", "N"):
                hstmap = copy.deepcopy(self.current_conf)
                for prim, mir in self.unpreferred_segments:
                    hstmap[(prim.hostname, prim.address)
                           ].primary_segments.discard(prim.content)
                    hstmap[(prim.hostname, prim.address)
                           ].mirror_segments.add(prim.content)
                    hstmap[(mir.hostname, mir.address)
                           ].primary_segments.add(mir.content)
                    hstmap[(mir.hostname, mir.address)
                           ].mirror_segments.discard(mir.content)
                hosts_list = list(hstmap.values())

        startup_validator = ClusterValidator(
            hosts_list,
            self.target_hosts,
            self.original_gparray.getSegmentsAsLoadedFromDb(),
            self.original_gparray.get_mirroring_enabled(),
            self.target_strategy)
        try:
            startup_validator.validate_segment_status()
        except StateValidationError as e:
            if not option_silent and not ask_yesno('', " %s Are you sure you want "
                                                   "to continue with this gprebalance session?" % str(e), "N"):
                raise ValidationError(f'User Aborted: {str(e)}')
            elif option_silent:
                raise ValidationError(f'{str(e)} Exiting...')
        if not self.original_gparray.get_mirroring_enabled() and not option_allow_mirrorless:
            if not option_silent and not ask_yesno('',
                                                   "Mirroring is disabled. \n"
                                                   "During rebalance the whole cluster may "
                                                   "be not available. \n"
                                                   "Are you sure you want "
                                                   "to continue with this gprebalance session?", "N"):
                raise ValidationError('User Aborted')
            elif option_silent:
                raise ValidationError('Mirroring is disabled.')

        if self.original_gparray.get_mirroring_enabled() and self.target_strategy == MirrorStrategy.MIRRORLESS:
            if not option_silent:
                mirror_type = ask_string(
                    "\nYou haven't specified desirable mirroring strategy.  Spread mirroring places\n"
                    "a given hosts mirrored segments each on a separate host.  You must be \n"
                    "using more hosts than the number of segments per host for spread mirroring. \n"
                    "Grouped mirroring places all of a given hosts segments on a single \n"
                    "mirrored host.  You must be using at least 2 hosts for grouped strategy.\n\n",
                    "What type of mirroring strategy would you like?",
                    'grouped', ['spread', 'grouped'])
            else:
                mirror_type = 'grouped'
            strat = MirrorStrategy.SPREAD if mirror_type == 'spread' else MirrorStrategy.GROUPED
            self.setMirroringStrategy(strat)
            startup_validator.mirror_strategy = strat

        already_bal, _ = startup_validator.validate_existing_configuration()
        if already_bal:
            # case of hosts shrinkage, expansion
            if len(set(startup_validator.existing_hosts) &
                   set(startup_validator.target_hosts)) < len(startup_validator.existing_hosts) or \
                    len(startup_validator.target_hosts) > len(startup_validator.existing_hosts):
                try:
                    startup_validator.prevalidate_segment_distribution()
                    startup_validator.prevalidate_mirror_strategy()
                except StateValidationError as e:
                    raise ValidationError(
                        "Cluster is already balanced. Cannot additionally rebalance to newly added hosts\n"
                        f"due to validation error {str(e)}")
            else:
                return False
        else:
            try:
                startup_validator.prevalidate_segment_distribution()
                startup_validator.prevalidate_mirror_strategy()
            except StateValidationError as e:
                raise ValidationError(str(e))
        return True

    def shutdown(self):
        self.conn.close()
        self.logger.info('Shutting down gprebalance...')
