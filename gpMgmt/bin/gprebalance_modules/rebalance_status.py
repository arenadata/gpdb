from datetime import datetime
import os
from gppylib.db import dbconn
from enum import Enum
from typing import Dict, List

from gprebalance_modules.rebalance_plan import Move
from gprebalance_modules.rebalance import SegmentId, Segment, SegmentSize

create_schema_sql = "CREATE SCHEMA gprebalance"
drop_schema_sql = "DROP SCHEMA IF EXISTS gprebalance CASCADE"

status_table_sql = """CREATE TABLE gprebalance.status
                        ( status varchar(255),
                          updated timestamp ) """

status_detail_table_sql = """CREATE TABLE gprebalance.status_detail
                        ( step_id serial,
                          dbid smallint,
                          content smallint,
                          role char,
                          preferred_role char,
                          source_hostname varchar(255),
                          source_port integer,
                          source_datadir varchar(255),
                          dest_hostname varchar(255),
                          target_datadir varchar(255),
                          target_port integer,
                          status varchar(255),
                          rebalance_started timestamp,
                          rebalance_finished timestamp,
                          source_kbytes numeric )"""


class MoveStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    AWAITS_SWITCH = "awaits_switch"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"
    REVERTED = "reverted"


class RebalanceStatus(Enum):
    INITIALIZED = "initialized"
    PREPARED = "prepared"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"
    AWAITS_SWITCH = "awaits_switch"


class InvalidStatusError(Exception):
    pass


class StatusManager:
    def __init__(self, conn: dbconn.Connection, options, logger):

        self.main_conn = conn
        self.options = options
        self.logger = logger

        self._status_filename = self.options.coordinator_data_directory + '/gprebalance.status'
        self._fp = None
        self._status = []
        self._status_info = []
        self._status_values = {'UNINITIALIZED': 1,
                               'VALIDATED': 2,
                               'PLANNED': 3,
                               'REBALANCE_PREPARE_SCHEMA_STARTED': 4,
                               'REBALANCE_PREPARE_SCHEMA_DONE': 5,
                               'EXECUTION_PREPARED': 6,
                               'EXECTUTION_STARTED': 7,
                               'EXECUTION_DONE': 8,
                               }
        self.conf_dir = None
        self.target_hosts_filename = None
        if os.path.exists(self._status_filename):
            self._read_status_file()

    def _read_status_file(self):
        self.logger.debug(
            "Trying to read in a pre-existing gprebalance status file")
        try:
            self._fp = open(self._status_filename, 'a+')
            self._fp.seek(0)

            for line in self._fp:
                (status, status_info) = line.rstrip().split(':')
                if status == 'PLANNED':
                    self.conf_dir = status_info
                elif status == 'VALIDATED':
                    self.target_hosts_filename = status_info
                self._status.append(status)
                self._status_info.append(status_info)
        except IOError:
            raise

        if self._status[-1] not in self._status_values:
            raise InvalidStatusError(
                'Invalid status file.  Unknown status %s' % self._status)

    def create_status_file(self):
        """Creates a new gpexpand status file"""
        try:
            self._fp = open(self._status_filename, 'w')
            self._fp.write('UNINITIALIZED:None\n')
            self._fp.flush()
            os.fsync(self._fp)
            self._status.append('UNINITIALIZED')
            self._status_info.append('None')
        except IOError:
            raise

    def get_current_status(self):
        """Gets the current status that has been written to the gpexpand
           status file"""
        if (len(self._status) > 0 and len(self._status_info) > 0):
            return (self._status[-1], self._status_info[-1])
        else:
            return (None, None)

    def set_status(self, status, status_info=None):
        if not self._fp:
            raise InvalidStatusError(
                'The status file is invalid and cannot be written to')
        if status not in self._status_values:
            raise InvalidStatusError(
                '%s is an invalid gpexpand status' % status)
        self._fp.write('%s:%s\n' % (status, status_info))
        self._fp.flush()
        os.fsync(self._fp)
        self._status.append(status)
        self._status_info.append(status_info)

    def setup_schema(self):
        dbconn.execSQL(self.main_conn, create_schema_sql)
        dbconn.execSQL(self.main_conn, status_table_sql)
        dbconn.execSQL(self.main_conn, status_detail_table_sql)

    def cleanup_schema(self):
        dbconn.execSQL(self.main_conn, drop_schema_sql)

    def set_db_status(self, status: RebalanceStatus, stamp: datetime = None):
        if not stamp:
            stamp = datetime.now()
        sql = "INSERT INTO gprebalance.status VALUES ('%s', '%s')" % (
            status, stamp)
        dbconn.execSQL(self.main_conn, sql)

    @staticmethod
    def record_move(conn: dbconn.Connection, move: Move, segment: Segment, source_kbytes: int, start_time: datetime):
        """Record initial move details"""

        sql = """
                    INSERT INTO gprebalance.status_detail (
                        dbid, content, role, preferred_role,
                        source_hostname, source_port, source_datadir,
                        dest_hostname, target_datadir, target_port, status, rebalance_started, source_kbytes
                    ) VALUES (
                        %d, %d, '%s', '%s', '%s', %d, '%s', '%s', '%s', %d, '%s', '%s', %d
                    )
                """ % (
            segment.dbid,
            segment.content,
            segment.role,
            segment.preferred_role,
            move.srcHost.hostname,
            segment.port,
            segment.datadir,
            move.dstHost.hostname,
            move.target_datadir,
            move.target_port,
            MoveStatus.PENDING,
            str(start_time),
            source_kbytes
        )
        dbconn.execSQL(conn, sql)

    @staticmethod
    def update_record_status(conn: dbconn.Connection, dbids: List[int], status: MoveStatus):
        """Record initial move details"""
        dbid_str = ','.join(str(dbid) for dbid in dbids)
        sql = """
                    UPDATE gprebalance.status_detail SET status = '%s' WHERE
                    dbid IN (%s)
                """ % (
            status,
            dbid_str
        )
        dbconn.execSQL(conn, sql)

    def remove_all(self):
        if self._fp:
            self._fp.close()
            self._fp = None
        if os.path.exists(self._status_filename):
            os.unlink(self._status_filename)
