#!/usr/bin/env python3

from transitions import Machine
from psycopg2 import DatabaseError
try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gparray import GpArray
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from gppylib.userinput import *
    from gppylib.commands import base
    from gppylib.commands.gp import SEGMENT_STOP_TIMEOUT_DEFAULT, SegmentStop
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))


DBNAME = 'postgres'

def print_progress(pool, interval=10):
    """
    Waits for a WorkerPool to complete, printing a progress percentage marker
    once at the beginning of the call, and thereafter at the provided interval
    (default ten seconds). A final 100% marker is printed upon completion.
    """
    def print_completed_percentage():
        # pool.completed can change asynchronously; save its value.
        completed = pool.completed

        pct = 0
        if pool.assigned:
            pct = float(completed) / pool.assigned

        pool.logger.info('%0.2f%% of jobs completed' % (pct * 100))
        return completed >= pool.assigned

    # print_completed_percentage() returns True if we're done.
    while not print_completed_percentage():
        if pool.join(interval):
            return

class GGShrink:
    timeout = SEGMENT_STOP_TIMEOUT_DEFAULT
    stop_mode = 'fast'

    rebalance_schema_name = 'ggrebalance'
    rebalance_status = 'rebalance_status'
    table_rebalance_status_detail = 'table_rebalance_status_detail'

    states = [
        'STATE_START',
        'STATE_OPTIONS_VALIDATION',
        'STATE_CHECK_PREVIOUS_RUN',
        'STATE_END',
        'STATE_CLEANUP',
        'STATE_ERROR',
        'STATE_ROLLBACK'
    ]

    # Note: order of states in the list above is important,
    # as we rely on it when recover from an interrupted state.
    states_main_shrink_flow = [
        'STATE_SETUP_SHRINK_SCHEMA_STARTED',
        'STATE_SETUP_SHRINK_SCHEMA_DONE',
        'STATE_BACKUP_CATALOG_STARTED',
        'STATE_BACKUP_CATALOG_DONE',
        'STATE_UPDATE_TARGET_SEGMENT_COUNT_STARTED',
        'STATE_UPDATE_TARGET_SEGMENT_COUNT_DONE',
        'STATE_PREPARE_SHRINK_SCHEMA_STARTED',
        'STATE_PREPARE_SHRINK_SCHEMA_DONE',
        'STATE_SHRINK_TABLES_STARTED',
        'STATE_SHRINK_TABLES_DONE',
        'STATE_SHRINK_CATALOG_STARTED',
        'STATE_SHRINK_CATALOG_DONE',
        'STATE_SHRINK_SEGMENTS_STOP_STARTED',
        'STATE_SHRINK_SEGMENTS_STOP_DONE',
        'STATE_SHRINK_DONE'
    ]

    transitions = [
        {
            'trigger': 'start',
            'source': 'STATE_START',
            'dest': 'STATE_OPTIONS_VALIDATION'
        },
        {
            'trigger': 'move_to_STATE_CLEANUP',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_CLEANUP'
        },
        {
            'trigger': 'move_to_STATE_CHECK_PREVIOUS_RUN',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_CHECK_PREVIOUS_RUN'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SHRINK_SCHEMA_STARTED',
            'source': 'STATE_CHECK_PREVIOUS_RUN',
            'dest': 'STATE_SETUP_SHRINK_SCHEMA_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SHRINK_SCHEMA_DONE',
            'source': 'STATE_SETUP_SHRINK_SCHEMA_STARTED',
            'dest': 'STATE_SETUP_SHRINK_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_BACKUP_CATALOG_STARTED',
            'source': 'STATE_SETUP_SHRINK_SCHEMA_DONE',
            'dest':  'STATE_BACKUP_CATALOG_STARTED'
        },
        {
            'trigger': 'move_to_STATE_BACKUP_CATALOG_DONE',
            'source': 'STATE_BACKUP_CATALOG_STARTED',
            'dest': 'STATE_BACKUP_CATALOG_DONE'
        },
        {
            'trigger': 'move_to_STATE_UPDATE_TARGET_SEGMENT_COUNT_STARTED',
            'source': 'STATE_BACKUP_CATALOG_DONE',
            'dest': 'STATE_UPDATE_TARGET_SEGMENT_COUNT_STARTED'
        },
        {
            'trigger': 'move_to_STATE_UPDATE_TARGET_SEGMENT_COUNT_DONE',
            'source': 'STATE_UPDATE_TARGET_SEGMENT_COUNT_STARTED',
            'dest': 'STATE_UPDATE_TARGET_SEGMENT_COUNT_DONE'
        },
        {
            'trigger': 'move_to_STATE_PREPARE_SHRINK_SCHEMA_STARTED',
            'source': 'STATE_UPDATE_TARGET_SEGMENT_COUNT_DONE',
            'dest': 'STATE_PREPARE_SHRINK_SCHEMA_STARTED'
        },
        {
            'trigger': 'move_to_STATE_PREPARE_SHRINK_SCHEMA_DONE',
            'source': 'STATE_PREPARE_SHRINK_SCHEMA_STARTED',
            'dest': 'STATE_PREPARE_SHRINK_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_TABLES_STARTED',
            'source': 'STATE_PREPARE_SHRINK_SCHEMA_DONE',
            'dest': 'STATE_SHRINK_TABLES_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_TABLES_DONE',
            'source': 'STATE_SHRINK_TABLES_STARTED',
            'dest': 'STATE_SHRINK_TABLES_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_CATALOG_STARTED',
            'source': 'STATE_SHRINK_TABLES_DONE',
            'dest': 'STATE_SHRINK_CATALOG_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_CATALOG_DONE',
            'source': 'STATE_SHRINK_CATALOG_STARTED',
            'dest': 'STATE_SHRINK_CATALOG_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_SEGMENTS_STOP_STARTED',
            'source': 'STATE_SHRINK_CATALOG_DONE',
            'dest': 'STATE_SHRINK_SEGMENTS_STOP_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_SEGMENTS_STOP_DONE',
            'source': 'STATE_SHRINK_SEGMENTS_STOP_STARTED',
            'dest': 'STATE_SHRINK_SEGMENTS_STOP_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_DONE',
            'source': 'STATE_SHRINK_SEGMENTS_STOP_DONE',
            'dest': 'STATE_SHRINK_DONE'
        },
        {
            'trigger': 'move_to_STATE_END',
            'source': ['STATE_SHRINK_DONE', 'STATE_CHECK_PREVIOUS_RUN'],
            'dest': 'STATE_END'
        },
        {
            'trigger': 'move_to_STATE_ERROR',
            'source': '*',
            'dest': 'STATE_ERROR'
        }
    ]

    def __init__(self, logger, dburl, options, gpEnv):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.gpEnv = gpEnv
        self.conn = dbconn.connect(
            self.dburl, encoding='UTF8', allowSystemTableMods=True)
        self.shutdown_requested = False
        self.workers_for_tables_rebalance = None
        self.workers_for_segment_stop = None
        self.gparray_dump_file = options.coordinator_data_directory + '/gparraydump'

        if os.path.exists(self.gparray_dump_file):
            self.logger.info('Init gparray from file %s' % self.gparray_dump_file)
            self.gparray = GpArray.initFromFile(self.gparray_dump_file)
        else:
            self.logger.info('Init gparray from catalog')
            try:
                self.gparray = GpArray.initFromCatalog(dburl, utility=True)
            except DatabaseError as ex:
                logger.error("Failed to connect to database.  Make sure the"
                             " Greengage instance you wish to expand is running"
                             " and that your environment is correct, then rerun"
                             " gprebalance" + ' '.join(sys.argv[1:]))
                sys.exit(1)
            except ConnectionError as ex:
                logger.error(f"{str(ex)}")
                sys.exit(1)

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states + self.states_main_shrink_flow,
                               transitions = self.transitions,
                               initial = 'STATE_START',
                               before_state_change = 'on_every_state')

    def run(self):
        self.trigger('start')

    def ggrebalance_schema_exists(self) -> bool:
        conn = dbconn.connect(self.dburl, encoding='UTF8')
        row = dbconn.queryRow(conn, "SELECT COUNT(1) FROM pg_namespace WHERE nspname = '%s';" % self.rebalance_schema_name)
        result = (int(row[0]) == 1)
        conn.close()
        return result

    def get_state_from_previous_run(self) -> str:
        conn = dbconn.connect(self.dburl, encoding='UTF8')
        cursor = dbconn.query(conn,
                              '''
                              SELECT status FROM %s.%s order by updated DESC limit 1;
                              ''' % (self.rebalance_schema_name, self.rebalance_status))
        result = 'not defined'
        if cursor.rowcount > 0:
            result = str(cursor.fetchone()[0])
        conn.close()
        return result

    def on_every_state(self):
        if self.shutdown_requested:
            self.logger.info("Shrink was interrupted")
            sys.exit(1)

        # self.logger.info('on_every_state %s' % (self.state))
        # insert status if the schema already exists
        if self.state in self.states_main_shrink_flow:
            if self.ggrebalance_schema_exists():
                conn = dbconn.connect(self.dburl, encoding='UTF8')
                dbconn.execSQL(conn,
                               '''
                                INSERT INTO %s.%s
                                VALUES ('%s', NOW());
                               ''' % (self.rebalance_schema_name, self.rebalance_status, self.state))
                conn.close()

    # state callbacks start here
    def on_enter_STATE_OPTIONS_VALIDATION(self):
        if self.options.clean_required:
            self.trigger('move_to_STATE_CLEANUP')
        else:
            if self.gparray.get_segment_count() <= self.options.target_segment_count:
                self.logger.error('Target segment count (%s) >= current segment count (%s).\n'
                                 'Currently only shrink is supported (target segment count < current segment count).'
                                  % (self.options.target_segment_count, self.gparray.get_segment_count()))
                self.trigger('move_to_STATE_ERROR')
                # self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')
            else:
                self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')

    def on_enter_STATE_CHECK_PREVIOUS_RUN(self):
        if self.ggrebalance_schema_exists():
            self.logger.info("Rebalance schema already exists")
            state_from_prev_run = self.get_state_from_previous_run()
            if state_from_prev_run == self.states_main_shrink_flow[len(self.states_main_shrink_flow) - 1]:
                self.logger.info("Previous run was completed successfully. Please execute cleanup before a new run.")
                self.trigger('move_to_STATE_END')
            else:
                self.logger.info("Previous run stopped after state '%s', trying to continue from the next state..." % state_from_prev_run)
                try:
                    next_state = self.states_main_shrink_flow[ self.states_main_shrink_flow.index(state_from_prev_run) + 1 ]
                except:
                    self.logger.error("Can't determine next state")
                    self.trigger('move_to_STATE_ERROR')
                    return
                # use auto to_«state» method to recover
                trigger_name = 'to_' + next_state
                self.trigger(trigger_name)
        else:
            self.trigger('move_to_STATE_SETUP_SHRINK_SCHEMA_STARTED')

    def on_enter_STATE_SETUP_SHRINK_SCHEMA_STARTED(self):
        # Create schema
        dbconn.execSQL(self.conn, 'BEGIN;')
        dbconn.execSQL(self.conn, 'DROP SCHEMA IF EXISTS %s CASCADE;' % self.rebalance_schema_name)
        dbconn.execSQL(self.conn, 'CREATE SCHEMA %s;' % self.rebalance_schema_name)
        dbconn.execSQL(self.conn,
                       '''
                       CREATE TABLE %s.%s
                       (status TEXT, updated TIMESTAMP WITH TIME ZONE)
                       DISTRIBUTED REPLICATED;
                       ''' % (self.rebalance_schema_name, self.rebalance_status))
        dbconn.execSQL(self.conn,
                       '''
                       CREATE TABLE %s.%s
                       (db_name TEXT, schema_name TEXT, rel_name TEXT, status TEXT,
                       CONSTRAINT unique_fqn UNIQUE (db_name, schema_name, rel_name))
                       DISTRIBUTED REPLICATED;
                       ''' % (self.rebalance_schema_name, self.table_rebalance_status_detail))
        dbconn.execSQL(self.conn, 'COMMIT;')

        self.trigger('move_to_STATE_SETUP_SHRINK_SCHEMA_DONE')

    def on_enter_STATE_SETUP_SHRINK_SCHEMA_DONE(self):
        self.logger.info("Created shrink schema %s" % self.rebalance_schema_name)
        self.trigger('move_to_STATE_BACKUP_CATALOG_STARTED')

    def on_enter_STATE_BACKUP_CATALOG_STARTED(self):
        self.trigger('move_to_STATE_BACKUP_CATALOG_DONE')

    def on_enter_STATE_BACKUP_CATALOG_DONE(self):
        self.trigger('move_to_STATE_UPDATE_TARGET_SEGMENT_COUNT_STARTED')

    def get_table_distr_segment_count(self, conn, schema_name, table_name) -> int:
        row = dbconn.queryRow(conn, '''
                              SELECT p.numsegments
                              FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                              JOIN gp_distribution_policy p ON c.oid = p.localoid
                              WHERE n.nspname='%s' AND c.relname='%s';
                              ''' % (schema_name, table_name))
        return int(row[0])

    def on_enter_STATE_UPDATE_TARGET_SEGMENT_COUNT_STARTED(self):
        dbconn.execSQL(self.conn, 'BEGIN;')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog();')
        dbconn.execSQL(self.conn, 'SELECT gp_toolkit.gp_set_rebalance_numsegments(%s);' % self.options.target_segment_count)

        self.gparray.dumpToFile(self.gparray_dump_file)

        # Rebalance the status tables we've created previously right here before we start to rebalance all other tables.
        # Before that check if the tables are already rebalanced
        # (in case we re-enter after interruption that happened after COMMIT but before new state)
        if self.get_table_distr_segment_count(self.conn,
                                              self.rebalance_schema_name,
                                              self.rebalance_status) > self.options.target_segment_count:
            dbconn.execSQL(self.conn,
                           '''ALTER TABLE "%s"."%s" REBALANCE %s;'''
                           % (self.rebalance_schema_name, self.rebalance_status, self.options.target_segment_count))

        if self.get_table_distr_segment_count(self.conn,
                                              self.rebalance_schema_name,
                                              self.table_rebalance_status_detail) > self.options.target_segment_count:
            dbconn.execSQL(self.conn,
                           '''ALTER TABLE "%s"."%s" REBALANCE %s;'''
                           % (self.rebalance_schema_name, self.table_rebalance_status_detail, self.options.target_segment_count))

        dbconn.execSQL(self.conn, 'COMMIT;')

        self.trigger('move_to_STATE_UPDATE_TARGET_SEGMENT_COUNT_DONE')

    def on_enter_STATE_UPDATE_TARGET_SEGMENT_COUNT_DONE(self):
        self.logger.info("Updated target segment count to %s", self.options.target_segment_count)
        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_STARTED')

    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED(self):
        # collect databases and tables that require 'ALTER TABLE REBALANCE'
        # and store in 'table_rebalance_status_detail' table

        dbconn.execSQL(self.conn, "BEGIN;");

        # cleanup table_rebalance_status_detail for the case we re-enter this state after we were interrupted right after it
        dbconn.execSQL(self.conn,"TRUNCATE %s.%s;" % (self.rebalance_schema_name, self.table_rebalance_status_detail));

        cursor = dbconn.query(self.conn, 'SELECT datname FROM pg_database;')
        databases_to_process = []
        for record in cursor:
            database_name = record[0]
            if database_name != 'template0':
                databases_to_process.append(database_name)

        for db in databases_to_process:
            dburl = dbconn.DbURL(dbname=db, port=self.gpEnv.getCoordinatorPort())
            conn = dbconn.connect(dburl, encoding='UTF8')
            cursor = dbconn.query(conn, '''
                SELECT n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN gp_distribution_policy p ON c.oid = p.localoid
                WHERE c.relkind IN ('r', 'p') AND c.relispartition = FALSE AND
                      p.numsegments > %s AND
                      n.nspname NOT IN ('pg_catalog', 'information_schema', '%s');
                ''' % (self.options.target_segment_count, self.rebalance_schema_name))
            for record in cursor:
                schema_name = record[0]
                rel_name = record[1]
                dbconn.execSQL(self.conn,
                               '''
                               INSERT INTO %s.%s
                               VALUES ('%s', '%s', '%s', 'none');
                               ''' % (self.rebalance_schema_name, self.table_rebalance_status_detail, db, schema_name, rel_name));
            conn.close()

        dbconn.execSQL(self.conn, "COMMIT;");

        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_DONE')

    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE(self):
        self.logger.info("Initiated %s.%s" % (self.rebalance_schema_name, self.table_rebalance_status_detail))
        self.trigger('move_to_STATE_SHRINK_TABLES_STARTED')

    class TableRebalanceTask(SQLCommand):
        def __init__(self,
                     shrink,
                     db_name,
                     schema_name,
                     rel_name):
            self.shrink = shrink
            self.db_name = db_name
            self.schema_name = schema_name
            self.rel_name = rel_name
            SQLCommand.__init__(self, "task rebalance for %s.%s.%s" % (self.db_name, self.schema_name, self.rel_name))

        def run(self, validateAfter=False):
            dburl = dbconn.DbURL(dbname=self.db_name, port=self.shrink.gpEnv.getCoordinatorPort())
            conn = dbconn.connect(dburl, encoding='UTF8')
            dbconn.execSQL(conn, 'BEGIN;')
            dbconn.execSQL(conn,
                           '''
                           ALTER TABLE "%s"."%s" REBALANCE %s;
                           ''' % (self.schema_name, self.rel_name, self.shrink.options.target_segment_count))
            dbconn.execSQL(self.shrink.conn,
                           '''
                           UPDATE %s.%s SET status = 'done'
                           WHERE db_name = '%s' AND schema_name = '%s' AND rel_name = '%s';
                           ''' % (self.shrink.rebalance_schema_name, self.shrink.table_rebalance_status_detail,
                                  self.db_name, self.schema_name, self.rel_name))
            dbconn.execSQL(conn, 'COMMIT;')
            conn.close()
            self.set_results(CommandResult(0, b'', b'', True, False))

    def on_enter_STATE_SHRINK_TABLES_STARTED(self):
        self.logger.info("Start tables rebalance")

        # perform 'ALTER TABLE REBALANCE' for all not yet processed tables
        cursor = dbconn.query(self.conn,
                              '''
                              SELECT * FROM %s.%s WHERE status = 'none';
                              ''' % (self.rebalance_schema_name, self.table_rebalance_status_detail))

        self.logger.info("Tables to process %s" % cursor.rowcount)

        if cursor.rowcount > 0:
            self.workers_for_tables_rebalance = WorkerPool(numWorkers=min(cursor.rowcount, self.options.parallel))

            for record in cursor:
                db_name = record[0]
                schema_name = record[1]
                rel_name = record[2]
                task = self.TableRebalanceTask(self,
                                               db_name,
                                               schema_name,
                                               rel_name)
                self.workers_for_tables_rebalance.addCommand(task)

            print_progress(self.workers_for_tables_rebalance, interval=1)
            #self.workers_for_tables_rebalance.join()

            self.workers_for_tables_rebalance.haltWork()
            self.workers_for_tables_rebalance.joinWorkers()

            for task in self.workers_for_tables_rebalance.getCompletedItems():
                if not task.was_successful():
                    raise Exception("Failed to do ALTER REBALANCE: {}" .format(
                        task.get_results().stderr))

            self.workers_for_tables_rebalance = None

        self.trigger('move_to_STATE_SHRINK_TABLES_DONE')

    def on_enter_STATE_SHRINK_TABLES_DONE(self):
        self.logger.info("Tables rebalance complete")
        self.trigger('move_to_STATE_SHRINK_CATALOG_STARTED')

    def on_enter_STATE_SHRINK_CATALOG_STARTED(self):
        self.logger.info("Start catalog shrink")

        ## Shrink catalog
        dbconn.execSQL(self.conn, 'BEGIN;')
        cursor = dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog();')
        ## TODO: repopulate?
        dbconn.execSQL(self.conn, 'DELETE FROM gp_segment_configuration WHERE content >= %s;' % self.options.target_segment_count)
        dbconn.execSQL(self.conn, 'CHECKPOINT;')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_bump_version();')
        cursor = dbconn.query(self.conn, 'SELECT gp_toolkit.gp_reset_rebalance_numsegments();')
        dbconn.execSQL(self.conn, 'COMMIT;')

        self.conn.close();

        self.trigger('move_to_STATE_SHRINK_CATALOG_DONE')

    def on_enter_STATE_SHRINK_CATALOG_DONE(self):
        self.logger.info("Catalog shrink complete")
        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_STARTED')

    def on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED(self):
        self.logger.info("Stopping shrinked segments...")
        # TODO: elaborate more what to do if some segments (mirrors) are marked as down
        segments_to_stop = self.gparray.get_segment_count() - self.options.target_segment_count
        segments_to_stop = segments_to_stop * 2 # consider mirrors
        self.workers_for_segment_stop = WorkerPool(numWorkers=min(segments_to_stop, self.options.batch_size))

        for seg_pair in self.gparray.getSegmentList():
            if seg_pair.primaryDB.getSegmentContentId() >= self.options.target_segment_count:
                cmd = SegmentStop("stop primary (content %s, dbid %s)" %
                                  (seg_pair.primaryDB.getSegmentContentId(),
                                   seg_pair.primaryDB.getSegmentDbId()),
                                   seg_pair.primaryDB.getSegmentDataDirectory(),
                                   mode=self.stop_mode,
                                   timeout=self.timeout,
                                   ctxt=base.REMOTE,
                                   remoteHost=seg_pair.primaryDB.getSegmentHostName())
                self.workers_for_segment_stop.addCommand(cmd)

                if seg_pair.mirrorDB != None:
                    cmd = SegmentStop("stop mirror (content %s, dbid %s)" %
                                      (seg_pair.mirrorDB.getSegmentContentId(),
                                       seg_pair.mirrorDB.getSegmentDbId()),
                                       seg_pair.mirrorDB.getSegmentDataDirectory(),
                                       mode=self.stop_mode,
                                       timeout=self.timeout,
                                       ctxt=base.REMOTE,
                                       remoteHost=seg_pair.mirrorDB.getSegmentHostName())
                    self.workers_for_segment_stop.addCommand(cmd)

        print_progress(self.workers_for_segment_stop, interval=1)

        self.workers_for_segment_stop.haltWork()
        self.workers_for_segment_stop.joinWorkers()

        for task in self.workers_for_segment_stop.getCompletedItems():
            if not task.was_successful():
                raise Exception("Failed to stop segments")

        self.workers_for_segment_stop = None

        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_DONE')

    def on_enter_STATE_SHRINK_SEGMENTS_STOP_DONE(self):
        self.logger.info("Shrinked segments were stopped")
        self.trigger('move_to_STATE_SHRINK_DONE')

    def on_enter_STATE_SHRINK_DONE(self):
        os.remove(self.gparray_dump_file)
        self.logger.info("Shrink is complete")
        self.trigger('move_to_STATE_END')

    def on_enter_STATE_CLEANUP(self):
        conn = dbconn.connect(self.dburl, encoding='UTF8')
        dbconn.execSQL(conn, 'DROP SCHEMA %s CASCADE;' % self.rebalance_schema_name)
        conn.close()
        self.logger.info("Cleanup is complete")

    def on_enter_STATE_ERROR(self):
        sys.exit(1)

    # state callbacks end here

    def shutdown(self):
        if self.workers_for_tables_rebalance != None:
            self.workers_for_tables_rebalance.haltWork()
            self.workers_for_tables_rebalance.joinWorkers()

        if self.workers_for_segment_stop != None:
            self.workers_for_segment_stop.haltWork()
            self.workers_for_segment_stop.joinWorkers()

        self.shutdown_requested = True
