#!/usr/bin/env python3

import base64
from collections import defaultdict
from dataclasses import dataclass
import ipaddress
import pickle
import re
import socket
from typing import Any, Dict, List, Set, Optional, Tuple
from enum import IntEnum
from gppylib.gparray import Segment, GpArray
from gppylib.commands.base import REMOTE, WorkerPool
from gppylib.commands.unix import Hostname, DiskFree, DiskUsage
from gppylib.operations.validate_disk_space import FileSystem

class ValidationError(Exception):
    pass

class ResourceError(Exception):
    pass

DEFAULT_PRIMARY_TEMPLATE = '/data1/primary/gpseg{content}'
DEFAULT_MIRROR_TEMPLATE = '/data1/mirror/gpseg{content}'

class HostStatus(IntEnum):
    ACTIVE = 1
    NEW = 2
    DECOMMISSIONED = 3

@dataclass
class DatadirInfo:
    """
    Stores both template and actual datadirs for a host
    """
    primary_template: str
    mirror_template: str
    # Actual paths from existing segment
    existing_primary_datadirs: Set[str]
    existing_mirror_datadirs: Set[str]
    
    def __init__(self, primary_template: str, mirror_template: str):
        self.primary_template = primary_template
        self.mirror_template = mirror_template
        self.existing_primary_datadirs = set()
        self.existing_mirror_datadirs = set()

@dataclass
class Host:
    """
    Segment host representaion
    
    Attributes:
        hostname: hostname from gp_segment_configuration
        address: address from gp_segment_configuration
        primary_datadirs: set of datadirs which primary catalogs belong to
        mirror_datadirs: set of datadirs which mirror catalogs belong to
        status: intendend host usage
    """
    hostname: str
    address: str
    datadir_info: DatadirInfo = None
    status: HostStatus = None

    def __hash__(self):
        return hash((self.hostname, self.address))
    
    def __eq__(self, other):
        if not isinstance(other, Host):
            return NotImplemented
        return self.hostname == other.hostname and self.address == other.address

    def __str__(self):
        pass

DISK_SPACE_SAFETY_MARGIN = 0.10

@dataclass
class SegmentSize:
    """
    Storage size of segment instance with all tablespace info
    
    Attributes:
        datadir_size_kb: Size of main datadir in KB
        tablespace_usage: Dict mapping tablespace paths to their sizes in KB
        total_size_kb: Total size including tablespaces
    """
    datadir_size_kb: int
    tablespace_usage: Optional[Dict[str, int]] = None

    @property
    def total_size_kb(self) -> int:
        """Calculate total size including tablespaces"""
        total = self.datadir_size_kb
        if self.tablespace_usage:
            total += sum(self.tablespace_usage.values())
        return total
    
    def __str__(self):
        """Human-readable size string"""
        size_mb = self.total_size_kb / 1024
        if size_mb < 1024:
            return f"{size_mb:.2f} MB"
        else:
            size_gb = size_mb / 1024
            return f"{size_gb:.2f} GB"
    
    def __repr__(self):
        return f"SegmentSize(datadir={self.datadir_size_kb}KB, tablespaces={self.tablespace_usage})"

class TemplateParser:
    """
    Handles parsing and validation of directory templates
    """
    
    VALID_PLACEHOLDERS = {'hostname', 'content'}
    PLACEHOLDER_PATTERN = r'\{(\w+)\}'
    
    @classmethod
    def parse_datadirs_input(cls, input_str: str) -> Tuple[str, str]:
        """
        Parse --target-datadirs input and return (primary_template, mirror_template)
        
        Handles:
        - "/data/primary/gpseg{content}, /data/mirror/gpseg{content}" -> as is
        - "/data/primary, /data/mirror" -> adds gpseg{content}
        - "/data/primary/{hostname}, /data/mirror/{hostname}" -> adds gpseg{content}
        """
        parts = [p.strip() for p in input_str.split(',')]
        
        if len(parts) != 2:
            raise ValidationError(
                '--target-datadirs should have format: '
                '"/data/primary/gpseg{content}, /data/mirror/gpseg{content}". '
                'Available templated parameters: {hostname}, {content}'
            )
        
        primary_template = cls._normalize_template(parts[0])
        mirror_template = cls._normalize_template(parts[1])
        
        return primary_template, mirror_template
    
    @classmethod
    def _normalize_template(cls, path: str) -> str:
        """
        Normalize a directory template path
        - If it contains placeholders, validate and return as-is
        - If it doesn't contain {content} placeholders, append gpseg{content}
        """
        placeholders = re.findall(cls.PLACEHOLDER_PATTERN, path)
        
        # Validate placeholders
        for placeholder in placeholders:
            if placeholder not in cls.VALID_PLACEHOLDERS:
                raise ValidationError(
                    f'Invalid placeholder {{{placeholder}}}. '
                    f'Valid placeholders are: {", ".join("{" + p + "}" for p in cls.VALID_PLACEHOLDERS)}'
                )
        
        # If no placeholders, add default gpseg{content}
        if not placeholders or ('content' not in placeholders):
            # Remove trailing slash if present
            path = path.rstrip('/')
            return f'{path}/gpseg{{content}}'
        
        return path
    
    @classmethod
    def parse_datadirs_file(cls, filepath: str) -> Tuple[str, str]:
        """
        Parse --target-datadirs-file
        Expected format (2 lines):
        /data/primary/gpseg{content}
        /data/mirror/gpseg{content}
        Available templated parameters: {hostname}, {content}
        """
        
        with open(filepath, 'r') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        
        if len(lines) != 2:
            raise ValidationError(
                f'File {filepath} should contain exactly 2 lines: '
                'primary template and mirror template'
            )
        
        primary_template = cls._normalize_template(lines[0])
        mirror_template = cls._normalize_template(lines[1])
        
        return primary_template, mirror_template
    
    @staticmethod
    def extract_base_path(datadir: str) -> str:
        """
        Extract base path from an actual segment datadir
        Examples:
        - /data/primary/gpseg0 -> /data/primary/gpseg{content}
        - /data/primary/host1/gpseg0 -> /data/primary/host1/gpseg{content}
        """
        # Remove trailing digits (content id)
        match = re.match(r'^(.+?)(\d+)$', datadir)
        if match:
            return match.group(1) + '{content}'
        return datadir + '{content}'
    
    @staticmethod
    def remove_content_suffix(datadir: str) -> str:
        """
        Remove trailing content ID from datadir
        /data/primary/gpseg0 -> /data/primary/gpseg
        """
        return re.sub(r'\d+$', '', datadir)
    
    @staticmethod
    def instantiate_template(template: str, hostname: str = None, content: int = None) -> str:
        """
        Instantiate a template with actual values
        """
        result = template
        if hostname is not None:
            result = result.replace('{hostname}', hostname)
        if content is not None:
            result = result.replace('{content}', str(content))
        return result

class HostResolver:
    """
    Utility class to resolve and match hostnames with IP addresses
    """
    def __init__(self):
        self._hostname_to_ips = {}  # Cache for hostname -> IP mapping
        self._ip_to_hostnames = {}  # Cache for IP -> hostname mapping
    
    def get_address(self, hostname: str) -> str:
        return self._hostname_to_ips.get(hostname, hostname)
    
    def get_hostname(self, ip: str) -> str:
        return self._ip_to_hostnames.get(ip, ip)
    
    def resolve_hostname(self, hostname: str) -> str:
        """
        Resolve hostname to IP addresses
        Returns IP address
        """
        if hostname in self._hostname_to_ips:
            return self._hostname_to_ips[hostname]
        
        ip = None
        try:
            addr_info = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
            for info in addr_info:
                ip = info[4][0]
                self._hostname_to_ips[hostname] = ip
                break
        except (socket.gaierror, socket.error):
            pass
        
        return ip
    
    def resolve_ip(self, ip_str: str) -> str:
        """
        Reverse resolve IP to hostnames
        Returns first corresponding hostname
        """
        if ip_str in self._ip_to_hostnames:
            return self._ip_to_hostnames[ip_str]
        
        hostname = None
        try:
            # Validate it's a valid IP first
            ipaddress.ip_address(ip_str)
            # Reverse lookup
            cmd = Hostname('hostname', ctxt=REMOTE, remoteHost=ip_str)
            cmd.run()
            hostname = cmd.get_hostname()
            self._ip_to_hostnames[ip_str] = cmd.get_hostname()
        except:
            pass
        
        return hostname
    
    def is_ip_address(self, host: str) -> bool:
        """
        Check if string is a valid IP address
        """
        try:
            ipaddress.ip_address(host)
            return True
        except ValueError:
            return False
    
    def hosts_match(self, host1: str, host2: str) -> bool:
        """
        Check if two hosts match (considering hostname/IP resolution)
        """
        # Direct match
        if host1 == host2:
            return True
        
        # Check if both are IPs
        is_ip1 = self.is_ip_address(host1)
        is_ip2 = self.is_ip_address(host2)
        
        if is_ip1 and is_ip2:
            return False
        
        # One is hostname, one is IP
        if is_ip1 and not is_ip2:
            # host1 is IP, host2 is hostname
            ips_of_host2 = self.resolve_hostname(host2)
            if ips_of_host2:
                return host1 in ips_of_host2
        
        if not is_ip1 and is_ip2:
            # host1 is hostname, host2 is IP
            ips_of_host1 = self.resolve_hostname(host1)
            if ips_of_host1:
                return host2 in ips_of_host1
        
        return False
    
    def find_matching_hostname(self, target_host: str, existing_hosts: List[str]) -> str:
        """
        Find if target_host matches any existing host
        Returns the matching existing host name, or None if no match
        """
        for existing_host in existing_hosts:
            if self.hosts_match(target_host, existing_host):
                return existing_host
        return None

def validate_ip_address(ip_str: str):
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False

def validate_hostname(hostname:str):
    if len(hostname) > 255:
        raise ValidationError(f"Hostname '{hostname}' exceeds maximum length of 255 characters")
    
    if not re.match(r'^[a-zA-Z0-9._-]+$', hostname):
        raise ValidationError(f"Hostname '{hostname}' contains invalid characters. "
        "Only ASCII letters, digits, hyphen, underscore, and dot are allowed")

def validate_hosts_basic(hosts: str, option_name: str):

    if not hosts:
        return

    target_hosts = list(map(str.strip, hosts.split(',')))

    # Remove empty strings
    target_hosts = [h for h in target_hosts if h]
    if not target_hosts:
        raise ValidationError(f" --{option_name}: No valid hosts provided")

    seen_hosts = set()
    has_ip = False
    has_hostname = False
    for host in target_hosts:
        # Check for duplicates
        if host in seen_hosts:
            raise ValidationError(f" --{option_name}: Duplicate host '{host}' found")
        seen_hosts.add(host)
        
        if validate_ip_address(host):
            has_ip = True
            continue
        has_hostname = True
        validate_hostname(host)
    if has_ip and has_hostname:
        raise ValidationError(f" --{option_name} must not contain IP adress and hostname simultaniously")

def get_hosts_from_file(file, option_name) -> str:
    result = ""
    with open(file, 'r') as fp:
        i = 0
        for line in fp:
            i += 1
            if i >= 1000:
                raise ValidationError(f" --{option_name} contains more than 1000 hosts")
            result = ", ".join(line.strip())
    return result

@dataclass
class SegmentId:
    """Identifier for a segment"""
    dbid: int
    content: int
    
    def __hash__(self):
        return hash((self.dbid, self.content))
    
    def __eq__(self, other):
        if not isinstance(other, SegmentId):
            return NotImplemented
        return self.dbid == other.dbid and self.content == other.content

@dataclass
class DiskSpaceInfo:
    """
    Disk space information for a filesystem
    
    Attributes:
        filesystem: Filesystem name/device
        available_kb: Available disk space in KB
        directory: Directory that was checked
    """
    filesystem: str
    available_kb: int
    directory: str
    
    @property
    def available_mb(self) -> float:
        return self.available_kb / 1024
    
    @property
    def available_gb(self) -> float:
        return self.available_mb / 1024
    
    def __str__(self):
        return f"Available: {self.available_gb:.2f} GB on {self.filesystem}"

class DiskSpaceChecker:
    """
    Utility for checking disk space on local and remote hosts
    """
    
    def __init__(self, logger: Any, batch_size: int = 16):
        """
        Initialize disk space checker
        
        Args:
            logger: Logger instance
            batch_size: Number of parallel operations
        """
        self.logger = logger
        self.batch_size = batch_size
    
    def get_disk_usage(self, hostaddr: str, directories: List[str]) -> Dict[str, int]:
        """
        Get the disk usage for the given set of directories on the targeted host
        
        Args:
            hostaddr: Host address (sometimes can be hostname) to check
            directories: List of directories to check
        
        Returns:
            Dictionary mapping directories to disk usage in KB
        """
        dirs_disk_usage = {}
        
        if not directories:
            return dirs_disk_usage
        
        pool = WorkerPool(numWorkers=min(len(directories), self.batch_size))
        try:
            for directory in directories:
                cmd = DiskUsage('check segment disk space used',
                               directory, ctxt=REMOTE, remoteHostAddr=hostaddr)
                pool.addCommand(cmd)
            pool.join()
        finally:
            pool.haltWork()
            pool.joinWorkers()
        
        for cmd in pool.getCompletedItems():
            if not cmd.was_successful():
                raise Exception(f"Unable to check disk usage on segment: {cmd.get_results().stderr}")
            
            dirs_disk_usage[cmd.directory] = cmd.kbytes_used()
        
        return dirs_disk_usage
    
    def get_available_space(self, hostaddr: str, directories: List[str]) -> Dict[str, DiskSpaceInfo]:
        """
        Get available disk space information for directories on remote host
        
        Uses DiskFree command which runs calculate_disk_free.py script.
        This handles the case where directories don't exist yet by walking
        up the path until it finds an existing directory.
        
        Args:
            hostaddr: Host address to check
            directories: List of directories/paths to check
        
        Returns:
            Dictionary mapping directory to DiskSpaceInfo
        """
        if not directories:
            return {}
        
        filesystems = self._get_filesystems(hostaddr, directories)
        
        # Build result mapping
        result = {}
        for fs in filesystems:
            # Each FileSystem has a list of directories it applies to
            for directory in fs.directories:
                result[directory] = DiskSpaceInfo(
                    filesystem=fs.name,
                    available_kb=fs.disk_free,
                    directory=directory
                )
        
        return result
    
    def _get_filesystems(self, hostaddr: str, directories: List[str]) -> List[FileSystem]:
        """
        Get filesystem information for directories on target host
        
        Args:
            hostaddr: Host address
            directories: List of directories
            
        Returns:
            List of FileSystem objects

        """
        filesystems = []
        # DiskFree handles multiple dirs in one command
        pool = WorkerPool(numWorkers=1)
        
        try:
            cmd = DiskFree(hostaddr, directories)
            pool.addCommand(cmd)
            pool.join()
        finally:
            pool.haltWork()
            pool.joinWorkers()
        
        for cmd in pool.getCompletedItems():
            if not cmd.was_successful():
                raise Exception(f"Failed to check disk free on target segment: {cmd.get_results().stderr}")
            
            # Decode the pickled result
            filesystems = pickle.loads(
                base64.urlsafe_b64decode(cmd.get_results().stdout))
        
        return filesystems
    
    def check_batch_usage(self, directories_by_host: Dict[str, List[str]]) -> Dict[str, Dict[str, int]]:
        """
        Check disk usage for multiple directories across multiple hosts
        
        Args:
            directories_by_host: Dict mapping host address to list of directories
        
        Returns:
            Dict mapping host address to dict of (directory -> usage_kb)
        """
        results = {}
        
        for hostaddr, directories in directories_by_host.items():
            try:
                usage = self.get_disk_usage(hostaddr, directories)
                results[hostaddr] = usage
            except Exception as e:
                self.logger.error(f"Failed to get disk usage for host {hostaddr}: {e}")
                raise
        
        return results
    
    def check_batch_available_space(self, 
                                    directories_by_host: Dict[str, List[str]]) -> Dict[str, Dict[str, DiskSpaceInfo]]:
        """
        Check available space for multiple directories across multiple hosts
        
        Args:
            directories_by_host: Dict mapping host address to list of directories
        
        Returns:
            Dict mapping host address to dict of (directory -> DiskSpaceInfo)
        """
        results = {}
        
        for hostaddr, directories in directories_by_host.items():
            try:
                space_info = self.get_available_space(hostaddr, directories)
                results[hostaddr] = space_info
            except Exception as e:
                self.logger.error(f"Failed to check available space on {hostaddr}: {e}")
                raise
        
        return results

def get_filesystem_base_path(datadir: str) -> str:
    """
    Extract base filesystem path from datadir
    
    This is a heuristic that attempts to identify the mount point
    or base directory for a datadir path.
    
    Examples:
        /data1/primary/gpseg0 -> /data1
        /data/primary/gpseg0 -> /data
        /gpdata/seg0 -> /gpdata
    
    Args:
        datadir: Full datadir path
    """
    parts = datadir.rstrip('/').split('/')
    
    # try to take first 2 path components for mount point
    if len(parts) >= 3:
        return '/' + parts[1] 
    return '/'